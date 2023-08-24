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
import re
from rasterio.vrt import WarpedVRT
import xarray

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
resp = s3_client_veda_smce.list_objects_v2(Bucket=bucket_name, Prefix="geos-oco2/")
for obj in resp["Contents"]:
    if obj["Key"].endswith(".tif"):
        keys.append(obj["Key"])

# List all TIFF files in the folder
tif_files = glob("../../data/oco2geos-co2-daygrid-v10r/*.nc4", recursive=True)
session = rasterio.env.Env()
summary_dict_netcdf, summary_dict_cog = {}, {}
overall_stats_netcdf, overall_stats_cog = {}, {}
full_data_df_netcdf, full_data_df_cog = pd.DataFrame(), pd.DataFrame()

for key in keys:
    with raster_io_session:
        s3_file = s3_client_veda_smce.generate_presigned_url(
            "get_object", Params={"Bucket": bucket_name, "Key": key}
        )
        filename_elements = re.split("[_ ? . ]", s3_file)
        with rasterio.open(s3_file) as src:
            for band in src.indexes:
                idx = pd.MultiIndex.from_product(
                    [
                        ["_".join(filename_elements[4:9])],
                        [filename_elements[9]],
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
                    f"{filename_elements[5]}_{filename_elements[9][:4]}_{calendar.month_name[int(filename_elements[9][4:6])]}_{filename_elements[9][6:]}"
                ] = {
                    "min_value": min_value,
                    "max_value": max_value,
                    "mean_value": mean_value,
                    "std_value": std_value,
                }

COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}
# Iterate over each TIFF file
for tif_file in tif_files:
    file_name = pathlib.Path(tif_file).name[:-4].split("_")

    xds = xarray.open_dataset(tif_file, engine="netcdf4")
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]
    for time_increment in range(0, len(xds.time)):
        for var in variable:
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            idx = pd.MultiIndex.from_product(
                [
                    [
                        "_".join(
                            [
                                file_name[1],
                                var,
                                file_name[2],
                                file_name[3],
                                file_name[-1],
                            ]
                        )
                    ],
                    [file_name[-2]],
                    [x for x in np.arange(1, len(data.lat) + 1)],
                ]
            )

            temp = pd.DataFrame(index=idx, data=data)
            full_data_df_netcdf = full_data_df_netcdf._append(temp, ignore_index=False)

            # Calculate summary statistics
            min_value = np.float64(temp.values.min())
            max_value = np.float64(temp.values.max())
            mean_value = np.float64(temp.values.mean())
            std_value = np.float64(temp.values.std())

            summary_dict_netcdf[
                f"{var}_{file_name[-2][:4]}_{calendar.month_name[int(file_name[-2][4:6])]}_{file_name[-2][6:]}"
            ] = {
                "min_value": min_value,
                "max_value": max_value,
                "mean_value": mean_value,
                "std_value": std_value,
            }


overall_stats_netcdf["min_value"] = np.float64(full_data_df_netcdf.values.min())
overall_stats_netcdf["max_value"] = np.float64(full_data_df_netcdf.values.max())
overall_stats_netcdf["mean_value"] = np.float64(full_data_df_netcdf.values.mean())
overall_stats_netcdf["std_value"] = np.float64(full_data_df_netcdf.values.std())

overall_stats_cog["min_value"] = np.float64(full_data_df_cog.values.min())
overall_stats_cog["max_value"] = np.float64(full_data_df_cog.values.max())
overall_stats_cog["mean_value"] = np.float64(full_data_df_cog.values.mean())
overall_stats_cog["std_value"] = np.float64(full_data_df_cog.values.std())


with open(
    "monthly_stats.json",
    "w",
) as fp:
    json.dump("\n Stats for raw netCDF files. \n", fp)
    json.dump(summary_dict_netcdf, fp)
    json.dump("\n Stats for transformed COG files. \n", fp)
    json.dump(summary_dict_cog, fp)

with open("overall_stats.json", "w") as fp:
    json.dump("\n Stats for raw netCDF files. \n", fp)
    json.dump(overall_stats_netcdf, fp)
    json.dump("\n Stats for transformed COG files. \n", fp)
    json.dump(overall_stats_cog, fp)

fig, ax = plt.subplots(2, 2, figsize=(10, 10))
plt.Figure(figsize=(10, 10))
sns.histplot(data=full_data_df_netcdf, kde=False, bins=10, legend=False, ax=ax[0][0])
ax[0][0].set_title("distribution plot for overall raw data")

sns.histplot(data=full_data_df_cog, kde=False, bins=10, legend=False, ax=ax[0][1])
ax[0][1].set_title("distribution plot for overall cog data")

temp_df = pd.DataFrame()
for key_value in summary_dict_netcdf.keys():
    if key_value.startswith("XCO2_2015"):
        temp_df = temp_df._append(summary_dict_netcdf[key_value], ignore_index=True)

sns.lineplot(
    data=temp_df,
    ax=ax[1][0],
)
ax[1][0].set_title("distribution plot for XCO2 variable for 2015 raw data")
ax[1][0].set_xlabel("Months")

temp_df = pd.DataFrame()
for key_value in summary_dict_cog.keys():
    if key_value.startswith("XCO2_2015"):
        temp_df = temp_df._append(summary_dict_cog[key_value], ignore_index=True)
sns.lineplot(
    data=temp_df,
    ax=ax[1][1],
)
ax[1][1].set_title("distribution plot for XCO2 variable for 2015 cog data")
ax[1][1].set_xlabel("Months")


plt.savefig("stats_summary.png")
plt.show()
