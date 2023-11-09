import os
import numpy as np
import matplotlib.pyplot as plt
import rasterio
from rasterio.plot import show
from rasterio.plot import show_hist
from glob import glob
import pathlib
import boto3
import pandas as pd
import calendar
import seaborn as sns
import json

from dotenv import load_dotenv

load_dotenv()

# session_veda_smce = boto3.session.Session()
session_veda_smce = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
)
s3_client_veda_smce = session_veda_smce.client("s3")
raster_io_session = rasterio.env.Env(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
)
bucket_name = "ghgc-data-store-dev"

keys = []
resp = s3_client_veda_smce.list_objects_v2(
    Bucket=bucket_name, Prefix="NASA_GSFC_ch4_wetlands_monthly/"
)
for obj in resp["Contents"]:
    if obj["Key"].endswith(".tif"):
        keys.append(obj["Key"])

# List all TIFF files in the folder
tif_files = glob("../../data/wetlands-monthly/*.nc", recursive=True)
# tif_files = glob("data/wetlands-monthly/*.nc", recursive=True)
session = rasterio.env.Env()
summary_dict_netcdf, summary_dict_cog = {}, {}
overall_stats_netcdf, overall_stats_cog = {}, {}
full_data_df_netcdf, full_data_df_cog = pd.DataFrame(), pd.DataFrame()

for key in keys:
    with raster_io_session:
        s3_file = s3_client_veda_smce.generate_presigned_url(
            "get_object", Params={"Bucket": bucket_name, "Key": key}
        )
        with rasterio.open(s3_file) as src:
            for band in src.indexes:
                idx = pd.MultiIndex.from_product(
                    [
                        [s3_file.split("_")[-1]],
                        [s3_file.split("_")[-1][5]],
                        [x for x in np.arange(1, src.height + 1)],
                    ]
                )
                # Read the raster data
                raster_data = src.read(band)
                raster_data[raster_data == -9999] = np.nan
                temp = pd.DataFrame(index=idx, data=raster_data)
                full_data_df_cog = full_data_df_cog._append(temp, ignore_index=False)

                # Calculate summary statistics
                min_value = np.float64(temp.values.min())
                max_value = np.float64(temp.values.max())
                mean_value = np.float64(temp.values.mean())
                std_value = np.float64(temp.values.std())

                summary_dict_cog[
                    f'{s3_file.split("_")[-1][:4]}_{calendar.month_name[int(s3_file.split("_")[-1][4:6])]}'
                ] = {
                    "min_value": min_value,
                    "max_value": max_value,
                    "mean_value": mean_value,
                    "std_value": std_value,
                }

# Iterate over each TIFF file
for tif_file in tif_files:
    file_name = pathlib.Path(tif_file).name[:-3]

    # Open the TIFF file
    with rasterio.open(tif_file) as src:
        for band in src.indexes:
            idx = pd.MultiIndex.from_product(
                [
                    [tif_file],
                    [band],
                    [x for x in np.arange(1, src.height + 1)],
                ]
            )
            # Read the raster data
            raster_data = src.read(band)
            raster_data[raster_data == -9999] = np.nan
            temp = pd.DataFrame(index=idx, data=raster_data)
            full_data_df_netcdf = full_data_df_netcdf._append(temp, ignore_index=False)

            # Calculate summary statistics
            min_value = np.float64(temp.values.min())
            max_value = np.float64(temp.values.max())
            mean_value = np.float64(temp.values.mean())
            std_value = np.float64(temp.values.std())

            summary_dict_netcdf[
                f'{tif_file.split(".")[-2]}_{calendar.month_name[band]}'
            ] = {
                "min_value": min_value,
                "max_value": max_value,
                "mean_value": mean_value,
                "std_value": std_value,
            }

full_data_df_cog = full_data_df_cog/1000
overall_stats_netcdf["min_value"] = np.float64(full_data_df_netcdf.values.min())
overall_stats_netcdf["max_value"] = np.float64(full_data_df_netcdf.values.max())
overall_stats_netcdf["mean_value"] = np.float64(full_data_df_netcdf.values.mean())
overall_stats_netcdf["std_value"] = np.float64(full_data_df_netcdf.values.std())

overall_stats_cog["min_value"] = np.float64(full_data_df_cog.values.min())
overall_stats_cog["max_value"] = np.float64(full_data_df_cog.values.max())
overall_stats_cog["mean_value"] = np.float64(full_data_df_cog.values.mean())
overall_stats_cog["std_value"] = np.float64(full_data_df_cog.values.std())


with open("monthly_stats.json", "w") as fp:
    json.dump("Stats for raw netCDF files.", fp)
    fp.write("\n")
    json.dump(summary_dict_netcdf, fp)
    fp.write("\n")
    json.dump("Stats for transformed COG files.", fp)
    fp.write("\n")
    json.dump(summary_dict_cog, fp)

with open("overall_stats.json", "w") as fp:
    json.dump("Stats for raw netCDF files.", fp)
    fp.write("\n")
    json.dump(overall_stats_netcdf, fp)
    fp.write("\n")
    json.dump("Stats for transformed COG files.", fp)
    fp.write("\n")
    json.dump(overall_stats_cog, fp)

fig, ax = plt.subplots(2, 2, figsize=(10, 10))
plt.Figure(figsize=(10, 10))
sns.histplot(
    data=full_data_df_netcdf.to_numpy().flatten(),
    kde=False,
    bins=100,
    legend=False,
    ax=ax[0][0],
)
ax[0][0].set_title("distribution plot for overall raw data")

sns.histplot(
    data=full_data_df_cog.to_numpy().flatten(),
    kde=False,
    bins=100,
    legend=False,
    ax=ax[0][1],
)
ax[0][1].set_title("distribution plot for overall cog data")

temp_df = pd.DataFrame()
for key_value in summary_dict_netcdf.keys():
    if key_value.startswith("2009"):
        temp_df = temp_df._append(summary_dict_netcdf[key_value], ignore_index=True)

sns.lineplot(
    data=temp_df,
    ax=ax[1][0],
)
ax[1][0].set_title("distribution plot for 2009 raw data")
ax[1][0].set_xlabel("Months")

temp_df = pd.DataFrame()
for key_value in summary_dict_cog.keys():
    if key_value.startswith("2009"):
        temp_df = temp_df._append(summary_dict_cog[key_value], ignore_index=True)
sns.lineplot(
    data=temp_df,
    ax=ax[1][1],
)
ax[1][1].set_title("distribution plot for 2009 cog data")
ax[1][1].set_xlabel("Months")


plt.savefig("stats_summary.png")
plt.show()
