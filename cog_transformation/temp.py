import os
import xarray
import re
import numpy as np
import pandas as pd
import json
import tempfile
import boto3
import rasterio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from glob import glob

session_veda_smce = boto3.Session(profile_name="veda-smce-mfa")
s3_client_veda = session_veda_smce.client("s3")
raster_io_session = rasterio.env.Env(profile_name="veda-smce-mfa")
# raster_io_session = rasterio.env.Env(profile_name="ghgc-smce-mfa")
session_ghgc_smce = boto3.Session(profile_name="ghgc-smce-mfa")
s3_client_ghgc = session_ghgc_smce.client("s3")

source_bucket_name = "ghgc-data-store-dev"
target_bucket_name = "ghgc-data-store-develop"
collection_name = "sedac-popdensity-yeargrid5yr-v4.11"

prefix = "lpjwsl-wetlandch4-monthgrid-v2/"


# def get_all_s3_keys(bucket, prefix):
#     """Get a list of all keys in an S3 bucket."""
#     keys = []

#     kwargs = {"Bucket": bucket, "Prefix": prefix}
#     while True:
#         resp = s3_client_veda.list_objects_v2(**kwargs)
#         for obj in resp["Contents"]:
#             if obj["Key"].endswith(".tif"):
#                 keys.append(obj["Key"])

#         try:
#             kwargs["ContinuationToken"] = resp["NextContinuationToken"]
#         except KeyError:
#             break

#     return keys


# source_keys = get_all_s3_keys(source_bucket_name, prefix)
# # target_keys = get_all_s3_keys(
# #     bucket_name, "updated_with_nodata/lpjeosim-wetlandch4-daygrid-v2/"
# # )
# # keys = list(
# #     set("/".join(string.split("/")[1:]) for string in source_keys)
# #     ^ set("/".join(string.split("/")[2:]) for string in target_keys)
# # )

# with raster_io_session:
#     for key in source_keys:
#         with rasterio.open(f"s3://{source_bucket_name}/{key}") as src:
#             # Read the data
#             data = src.read()

#             # Get the metadata of the source file
#             meta = src.meta.copy()

#             # Update the metadata with the new "no data" value 9.96921e+36
#             meta.update(nodata=-9999)

#             # Replace original "no data" values in data with new "no data" value
#             # data = np.nan_to_num(data, nan=-9999)
#             # data[data == data.max()] = -9999

#             with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#                 temp_file_path = temp_file.name

#             # Write the updated data to the temporary file
#             with rasterio.open(temp_file_path, "w", **meta) as dst:
#                 dst.write(data)

#             # Upload the temporary file to S3
#             s3_client_ghgc.upload_file(
#                 Filename=temp_file_path,
#                 Bucket=target_bucket_name,
#                 Key=f"{collection_name}/{key.split('/')[1]}",
#             )

#             # Clean up the temporary file
#             os.remove(temp_file_path)

fold_names = os.listdir("/Users/vgaur/Scripts/CMIP6/gpw")

files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # A dataframe to keep track of the files that we have transformed into COGs

# Reading the raw netCDF files from local machine
# for fol_ in fold_names:
for name in glob(f"/Users/vgaur/Scripts/CMIP6/gpw/*.tif", recursive=False):
    xds = xarray.open_dataarray(name)

    filename = name.split("/")[-1]
    filename_elements = re.split("[_ .]", filename)
    # # insert date of generated COG into filename
    filename_elements.pop()
    filename_elements.append(filename_elements[-3])

    xds.data = np.nan_to_num(xds.data, nan=-9999)
    xds.rio.set_spatial_dims("x", "y", inplace=True)
    xds.rio.write_crs("epsg:4326", inplace=True)
    xds.rio.write_nodata(-9999, inplace=True, encoded= True)

    cog_filename = "_".join(filename_elements)
    # # add extension
    cog_filename = f"{cog_filename}.tif"

    with raster_io_session:
        with tempfile.NamedTemporaryFile() as temp_file:
            xds.rio.to_raster(temp_file.name, driver="COG")
            s3_client_ghgc.upload_file(
                Filename=temp_file.name,
                Bucket=target_bucket_name,
                Key=f"{collection_name}/{cog_filename}",
            )

        files_processed = files_processed._append(
            {"file_name": name, "COGs_created": cog_filename},
            ignore_index=True,
        )

    print(f"Generated and saved COG: {cog_filename}")


# creating the csv file with the names of files transformed.
# files_processed.to_csv(
#     f"s3://{bucket_name}/gridded_population_cog/files_converted.csv",
# )
print("Done generating COGs")
