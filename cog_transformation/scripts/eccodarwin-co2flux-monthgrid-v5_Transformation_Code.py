# This script was used to transform the Ecco Darwin dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.

import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
import rasterio
from datetime import datetime
from dateutil.relativedelta import relativedelta

session = boto3.session.Session()
s3_client = session.client("s3")

bucket_name = (
    "ghgc-data-store-dev"  # S3 bucket where the COGs are stored after transformation
)
FOLDER_NAME = "ecco-darwin"
s3_fol_name = "ecco_darwin"

# Reading the raw netCDF files from local machine
files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # A dataframe to keep track of the files that we have transformed into COGs
for name in os.listdir(FOLDER_NAME):
    xds = xarray.open_dataset(
        f"{FOLDER_NAME}/{name}",
        engine="netcdf4",
    )
    xds = xds.rename({"y": "latitude", "x": "longitude"})
    xds = xds.assign_coords(longitude=((xds.longitude / 1440) * 360) - 180).sortby(
        "longitude"
    )
    xds = xds.assign_coords(latitude=((xds.latitude / 721) * 180) - 90).sortby(
        "latitude"
    )

    variable = [var for var in xds.data_vars]

    for time_increment in xds.time.values:
        for var in variable[2:]:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = xds[var]

            data = data.reindex(latitude=list(reversed(data.latitude)))
            data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)

            # generate COG
            COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

            filename_elements.pop()
            filename_elements[-1] = filename_elements[-2] + filename_elements[-1]
            filename_elements.pop(-2)
            # # insert date of generated COG into filename
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"

            with tempfile.NamedTemporaryFile() as temp_file:
                data.rio.to_raster(temp_file.name, **COG_PROFILE)
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=bucket_name,
                    Key=f"{s3_fol_name}/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )
            del data

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
        Key="s3_fol_name/metadata.json",
    )

# A csv file to store the names of all the files converted.
files_processed.to_csv(
    f"s3://{bucket_name}/{s3_fol_name}/files_converted.csv",
)
print("Done generating COGs")
