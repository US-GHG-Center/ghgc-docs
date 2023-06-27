import re
import pandas as pd
import json
import tempfile
import boto3

session_ghgc = boto3.session.Session(profile_name="ghg_user")
s3_client_ghgc = session_ghgc.client("s3")
session_veda_smce = boto3.session.Session()
s3_client_veda_smce = session_veda_smce.client("s3")


SOURCE_BUCKET_NAME = "ghgc-data-staging-uah"
TARGET_BUCKET_NAME = "ghgc-data-store-dev"


keys = []
resp = s3_client_ghgc.list_objects_v2(Bucket=SOURCE_BUCKET_NAME)
for obj in resp["Contents"]:
    if "l3" in obj["Key"]:
        keys.append(obj["Key"])

for key in keys:
    s3_obj = s3_client_ghgc.get_object(Bucket=SOURCE_BUCKET_NAME, Key=key)[
        "Body"
    ]
    filename = key.split("/")[-1]
    filename_elements = re.split("[_ .]", filename)

    date = re.search("t\d\d\d\d\d\d\d\dt", key).group(0)
    filename_elements.insert(-1, date[1:-1])
    filename_elements.pop()

    cog_filename = "_".join(filename_elements)
    # # add extension
    cog_filename = f"{cog_filename}.tif"
    s3_client_veda_smce.upload_fileobj(
        Fileobj=s3_obj,
        Bucket=TARGET_BUCKET_NAME,
        Key=f"plum_data/{cog_filename}",
    )
