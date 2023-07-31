# This script was used to transform the Wetland Methane Emissions, LPJ-wsl Model dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.

import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
from datetime import datetime, timedelta

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = "ghgc-data-store-dev"
FOLDER_NAME = "NASA_GSFC_ch4_wetlands_daily"
directory = "ch4_wetlands_daily"

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])
for name in os.listdir(directory):
    xds = xarray.open_dataset(
        f"{directory}/{name}", engine="netcdf4", decode_times=False
    )
    xds = xds.assign_coords(
        longitude=(((xds.longitude + 180) % 360) - 180)
    ).sortby("longitude")
    variable = [var for var in xds.data_vars]
    filename = name.split("/ ")[-1]
    filename_elements = re.split("[_ .]", filename)
    start_time = datetime(int(filename_elements[-2]), 1, 1)

    for time_increment in range(0, len(xds.time)):
        for var in variable:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(latitude=slice(None, None, -1))
            data = data * 1000
            data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            date = start_time + timedelta(hours=data.time.item(0))

            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = date.strftime("%Y%m%d")
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
files_processed.to_csv(
    f"s3://{bucket_name}/{FOLDER_NAME}/files_converted.csv",
)
print("Done generating COGs")
