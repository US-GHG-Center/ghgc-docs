import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
from glob import glob
from datetime import datetime
import numpy as np

session = boto3.session.Session(profile_name="veda-smce-mfa")
s3_client = session.client("s3")
bucket_name = "ghgc-data-store-dev"
date_fmt = "%Y%m"

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])
data_files = glob("data/epa_emissions_express_extension/*.nc", recursive=True)
for name in data_files:
    xds = xarray.open_dataset(name, engine="netcdf4")
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
    filename = name.split("/")[-1]
    filename_elements = re.split("[_ .]", filename)
    start_time = datetime(int(filename_elements[-2]), 1, 1)

    for time_increment in range(0, len(xds.time)):
        for key, value in new_variables.items():
            data = np.zeros(dtype=np.float32, shape=(len(xds.lat), len(xds.lon)))
            filename = name.split("/")[-1]
            filename_elements = re.split("[_ .]", filename)
            for var in value:
                data = data + getattr(xds.isel(time=time_increment), var)
            # data = np.round(data / pow(10, 9), 2)
            data.values[data.values == 0] = np.nan
            data = data * (
                (1 / (6.022 * pow(10, 23)))
                * (16.04 * pow(10, -6))
                * 366
                * pow(10, 10)
                * 86400
            )
            data = data.fillna(-9999)
            data = data.isel(lat=slice(None, None, -1))
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)

            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = start_time.strftime("%Y")
            filename_elements.insert(2, key)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"

            with tempfile.NamedTemporaryFile() as temp_file:
                data.rio.to_raster(
                    temp_file.name, driver="COG", overview_resampling="bilinear"
                )
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=bucket_name,
                    Key=f"epa-ch4emission-yeargrid-v2express/{cog_filename}",
                )

                files_processed = files_processed._append(
                    {"file_name": name, "COGs_created": cog_filename},
                    ignore_index=True,
                )

                print(f"Generated and saved COG: {cog_filename}")
print("Done generating COGs")
