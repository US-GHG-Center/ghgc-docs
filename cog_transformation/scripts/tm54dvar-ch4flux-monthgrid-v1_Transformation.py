# This script was used to transform the TM5 inverse flux dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.
import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
from datetime import datetime

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = (
    "ghgc-data-store-dev"  # S3 bucket where the COGs are stored after transformation
)
FOLDER_NAME = "tm5-ch4-inverse-flux"

files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # A dataframe to keep track of the files that we have transformed into COGs

# Reading the raw netCDF files from local machine
for name in os.listdir(FOLDER_NAME):
    xds = xarray.open_dataset(f"{FOLDER_NAME}/{name}", engine="netcdf4")
    xds = xds.rename({"latitude": "lat", "longitude": "lon"})
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars if "global" not in var]

    for time_increment in range(0, len(xds.months)):
        filename = name.split("/ ")[-1]
        filename_elements = re.split("[_ .]", filename)
        start_time = datetime(int(filename_elements[-2]), time_increment + 1, 1)
        for var in variable:
            data = getattr(xds.isel(months=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)

            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = start_time.strftime("%Y%m")
            filename_elements.insert(2, var)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"

            with tempfile.NamedTemporaryFile() as temp_file:
                data.rio.to_raster(
                    temp_file.name,
                    driver="COG",
                )
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=bucket_name,
                    Key=f"{FOLDER_NAME}/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")

# Generate the json file with the metadata that is present in the netCDF files.
with tempfile.NamedTemporaryFile(mode="w+") as fp:
    json.dump(xds.attrs, fp)
    json.dump({"data_dimensions": dict(xds.dims)}, fp)
    json.dump({"data_variables": list(xds.data_vars)}, fp)
    fp.flush()

    s3_client.upload_file(
        Filename=fp.name,
        Bucket=bucket_name,
        Key=f"{FOLDER_NAME}/metadata.json",
    )

# creating the csv file with the names of files transformed.
files_processed.to_csv(
    f"s3://{bucket_name}/{FOLDER_NAME}/files_converted.csv",
)
print("Done generating COGs")
