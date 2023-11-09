import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
from datetime import datetime
import s3fs
from dotenv import load_dotenv

load_dotenv()


raw_data_bucket = "cmip6-staging"
cog_data_s3_bucket = "climatedashboard-data"
model_name = "climdex/tmaxXF/ACCESS-CM2"
# session = boto3.Session(
#     aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
#     aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
# )
session = boto3.Session(profile_name="vs_code_user")

s3_client = session.client("s3")
files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])


def get_all_s3_keys(bucket, model_name):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {"Bucket": bucket, "Prefix": f"{model_name}/"}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            if obj["Key"].endswith(".nc") and "historical" not in obj["Key"]:
                keys.append(obj["Key"])

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

    return keys


keys = get_all_s3_keys(raw_data_bucket, model_name)
fs = s3fs.S3FileSystem(profile="vs_code_user", anon=False)

for key in keys:
    file_obj = fs.open(f"s3://{raw_data_bucket}/{key}")
    xds = xarray.open_dataset(file_obj, engine="h5netcdf")
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]

    filename = key.split("/")[-1]

    # filename_elements = filename.split("_")

    # date_fmt = None
    # if timestep == "day":
    #     date_fmt = "%Y_%m_%d"
    # elif timestep == "month":
    #     date_fmt = "%Y%m"
    # elif timestep == "CrossingYear":
    #     pass
    # else:
    #     raise ValueError(f"Unrecognized date format in key: {key}")
    date_fmt = "%Y"

    for time_increment in range(0, len(xds.time)):
        for var in variable:
            filename_elements = filename.split("_")
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            date = data.time.dt.strftime(date_fmt).item(0)

            filename_elements[-1] = date
            filename_elements.append(var)
            cog_filename = "_".join(filename_elements)
            # add extension
            cog_filename = f"{cog_filename}.tif"
            # cog_filepath = "/".join(key.split("/")[1:-1])

            with tempfile.NamedTemporaryFile() as temp_file:
                data.rio.to_raster(temp_file.name, driver="COG", compress="DEFLATE")
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=cog_data_s3_bucket,
                    Key=f"climdex/tmaxXF/ACCESS-CM2/{cog_filename}",
                )

            files_processed = files_processed._append(
                {
                    "file_name": key,
                    "COGs_created": f"climdex/tmaxXF/ACCESS-CM2/{cog_filename}",
                },
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")

files_processed.to_csv(
    f"s3://{cog_data_s3_bucket}/CMIP6/files_converted.csv",
)
print("Done generating COGs")
