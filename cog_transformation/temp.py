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

session = boto3.Session(profile_name="veda-smce-mfa")
s3_client = session.client("s3")
raster_io_session = rasterio.env.Env(profile_name="veda-smce-mfa")

bucket_name = "ghgc-data-store-dev"
new_cog_folder = "updated_with_nodata"
collection_name = "lpjeosim-wetlandch4-daygrid-v2"
prefix = "lpjwsl-wetlandch4-daygrid-v2-new-units-20240418/"


def get_all_s3_keys(bucket, prefix):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            if obj["Key"].endswith(".tif"):
                keys.append(obj["Key"])

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

    return keys


source_keys = get_all_s3_keys(bucket_name, prefix)
target_keys = get_all_s3_keys(
    bucket_name, "updated_with_nodata/lpjeosim-wetlandch4-daygrid-v2/"
)
# keys = list(
#     set("/".join(string.split("/")[1:]) for string in source_keys)
#     ^ set("/".join(string.split("/")[2:]) for string in target_keys)
# )

with raster_io_session:
    for key in source_keys:
        with rasterio.open(f"s3://{bucket_name}/{key}") as src:
            # Read the data
            data = src.read()

            # Get the metadata of the source file
            meta = src.meta.copy()

            # Update the metadata with the new "no data" value
            meta.update(nodata=-9999)

            # Replace original "no data" values in data with new "no data" value
            data[data == src.nodata] = -9999

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name

            # Write the updated data to the temporary file
            with rasterio.open(temp_file_path, "w", **meta) as dst:
                dst.write(data)

            # Upload the temporary file to S3
            s3_client.upload_file(
                Filename=temp_file_path,
                Bucket=bucket_name,
                Key=f"{new_cog_folder}/{collection_name}/{'/'.join(key.split('/')[1:])}",
            )

            # Clean up the temporary file
            os.remove(temp_file_path)
