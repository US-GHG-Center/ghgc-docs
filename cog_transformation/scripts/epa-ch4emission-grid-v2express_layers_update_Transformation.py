# This script was used to add concatenated layers and transform the Gridded EPA U.S. Anthropogenic Methane Greenhouse Gas dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.

import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
from datetime import datetime
import numpy as np

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = (
    "ghgc-data-store-dev"  # S3 bucket where the COGs are stored after transformation
)
FOLDER_NAME = "epa_emissions_express_extension"

files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # A dataframe to keep track of the files that we have transformed into COGs

# Reading the raw netCDF files from local machine
for name in os.listdir(FOLDER_NAME):
    xds = xarray.open_dataset(f"{FOLDER_NAME}/{name}", engine="netcdf4")
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]
    new_variables = {
        "all-variables": variable[:-1],
        "agriculture": variable[17:21],
        "natural-gas-systems": variable[10:15] + [variable[26]],
        "petroleum-systems": variable[5:9],
        "waste": variable[21:26],
        "coal-mines": variable[2:5],
        "other": variable[:2] + [variable[9]] + variable[15:17],
    }
    filename = name.split("/ ")[-1]
    filename_elements = re.split("[_ .]", filename)
    start_time = datetime(int(filename_elements[-2]), 1, 1)

    for time_increment in range(0, len(xds.time)):
        for key, value in new_variables.items():
            data = np.zerosmpty(dtype=np.float32, shape=(len(xds.lat), len(xds.lon)))
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            for var in value:
                data = data + getattr(xds.isel(time=time_increment), var)
            data = data / pow(10, 6)
            data = data.isel(lat=slice(None, None, -1))
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)

            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = start_time.strftime("%Y")
            filename_elements.insert(2, key)
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
print("Done generating COGs")
