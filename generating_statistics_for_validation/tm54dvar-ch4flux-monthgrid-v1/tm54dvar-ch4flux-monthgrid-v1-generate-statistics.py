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
import xarray
import re

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


def get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {"Bucket": bucket, "Prefix": "tm5-ch4-inverse-flux-mask"}
    while True:
        resp = s3_client_veda_smce.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            if obj["Key"].endswith(".tif"):
                keys.append(obj["Key"])

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

    return keys


keys = get_all_s3_keys(bucket_name)

# List all TIFF files in the folder
tif_files = glob("../../data/tm54dvar-ch4flux-mask-monthgrid-v5/*.nc", recursive=True)
session = rasterio.env.Env()
summary_dict_netcdf, summary_dict_cog = {}, {}
overall_stats_netcdf, overall_stats_cog = {}, {}
full_data_df_netcdf, full_data_df_cog = pd.DataFrame(), pd.DataFrame()

for key in keys:
    try:
        with raster_io_session:
            s3_file = s3_client_veda_smce.generate_presigned_url(
                "get_object", Params={"Bucket": bucket_name, "Key": key}
            )
            filename_elements = re.split("[_ ? . ]", s3_file)
            with rasterio.open(s3_file) as src:
                for band in src.indexes:
                    idx = pd.MultiIndex.from_product(
                        [
                            ["_".join(filename_elements[4:6])],
                            [filename_elements[6]],
                            [x for x in np.arange(1, src.height + 1)],
                        ]
                    )
                    # Read the raster data
                    raster_data = src.read(band)
                    raster_data[raster_data == -9999] = np.nan
                    raster_data[raster_data == 9.969209968386869e36] = np.nan
                    temp = pd.DataFrame(index=idx, data=raster_data)
                    full_data_df_cog = full_data_df_cog._append(
                        temp, ignore_index=False
                    )

                    # Calculate summary statistics
                    min_value = np.float64(np.nanmin(temp.values))
                    max_value = np.float64(np.nanmax(temp.values))
                    mean_value = np.float64(np.nanmean(temp.values))
                    std_value = np.float64(np.nanstd(temp.values))

                    if "surface" not in filename_elements:
                        summary_dict_cog[
                            f"{filename_elements[5]}_{filename_elements[6][:4]}_{calendar.month_name[int(filename_elements[6][4:6])]}"
                        ] = {
                            "min_value": min_value,
                            "max_value": max_value,
                            "mean_value": mean_value,
                            "std_value": std_value,
                        }
    except:
        print(s3_file)

# Iterate over each TIFF file

COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}
# Iterate over each TIFF file
for tif_file in tif_files:
    file_name = pathlib.Path(tif_file).name[:-3].split("_")

    xds = xarray.open_dataset(tif_file, engine="netcdf4")
    xds = xds.rename({"latitude": "lat", "longitude": "lon"})
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars if "global" not in var]
    for time_increment in range(0, len(xds.months)):
        for var in variable:
            data = getattr(xds.isel(months=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            idx = pd.MultiIndex.from_product(
                [
                    [
                        "_".join(
                            [
                                file_name[1],
                                var,
                            ]
                        )
                    ],
                    [f"{file_name[2]}{time_increment+1}"],
                    [x for x in np.arange(1, len(data.lat) + 1)],
                ]
            )
            temp = pd.DataFrame(index=idx, data=data)
            temp[temp == 9.969209968386869e36] = np.nan
            full_data_df_netcdf = full_data_df_netcdf._append(temp, ignore_index=False)

            # Calculate summary statistics
            min_value = np.float64(np.nanmin(temp.values))
            max_value = np.float64(np.nanmax(temp.values))
            mean_value = np.float64(np.nanmean(temp.values))
            std_value = np.float64(np.nanstd(temp.values))

            summary_dict_netcdf[
                f"{var}_{file_name[2]}_{calendar.month_name[time_increment+1]}"
            ] = {
                "min_value": min_value,
                "max_value": max_value,
                "mean_value": mean_value,
                "std_value": std_value,
            }

overall_stats_netcdf["min_value"] = np.float64(np.nanmin(full_data_df_netcdf.values))
overall_stats_netcdf["max_value"] = np.float64(np.nanmax(full_data_df_netcdf.values))
overall_stats_netcdf["mean_value"] = np.float64(np.nanmean(full_data_df_netcdf.values))
overall_stats_netcdf["std_value"] = np.float64(np.nanstd(full_data_df_netcdf.values))

overall_stats_cog["min_value"] = np.float64(np.nanmin(full_data_df_cog.values))
overall_stats_cog["max_value"] = np.float64(np.nanmax(full_data_df_cog.values))
overall_stats_cog["mean_value"] = np.float64(np.nanmean(full_data_df_cog.values))
overall_stats_cog["std_value"] = np.float64(np.nanstd(full_data_df_cog.values))


with open(
    "monthly_stats.json",
    "w",
) as fp:
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
temp_df = pd.DataFrame()
for key_value in full_data_df_netcdf.index.values:
    if key_value[0].startswith("emis_total"):
        temp_df = temp_df._append(full_data_df_netcdf.loc[key_value])
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[0][0])
ax[0][0].set_title("overall raw data for total ch4")

temp_df = pd.DataFrame()
for key_value in full_data_df_cog.index.values:
    if key_value[0].startswith("emis_total"):
        temp_df = temp_df._append(full_data_df_cog.loc[key_value])
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[0][1])
ax[0][1].set_title("overall cog data for total ch4")

temp_df = pd.DataFrame()
for key_value in full_data_df_netcdf.index.values:
    if key_value[0].startswith("emis_microbial"):
        temp_df = temp_df._append(full_data_df_netcdf.loc[key_value])
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[1][0])
ax[1][0].set_title("overall raw data for microbial")

temp_df = pd.DataFrame()
for key_value in full_data_df_cog.index.values:
    if key_value[0].startswith("emis_microbial"):
        temp_df = temp_df._append(full_data_df_cog.loc[key_value])
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[1][1])
ax[1][1].set_title("overall cog data for microbial")

plt.savefig("overall_stats_summary.png")
plt.show()


fig, ax = plt.subplots(2, 2, figsize=(10, 10))
temp_df = pd.DataFrame()
for key_value in summary_dict_netcdf.keys():
    if key_value.startswith("total_2015"):
        temp_df = temp_df._append(summary_dict_netcdf[key_value], ignore_index=True)

sns.lineplot(
    data=temp_df,
    ax=ax[0][0],
)
ax[0][0].set_title("total ch4 netCDF data for 2015")
ax[0][0].set_xlabel("Months")

temp_df = pd.DataFrame()
for key_value in summary_dict_cog.keys():
    if key_value.startswith("total_2015"):
        temp_df = temp_df._append(summary_dict_cog[key_value], ignore_index=True)
sns.lineplot(
    data=temp_df,
    ax=ax[0][1],
)
ax[0][1].set_title("Total ch4 cog data for 2015")
ax[0][1].set_xlabel("Months")

temp_df = pd.DataFrame()
for key_value in summary_dict_netcdf.keys():
    if key_value.startswith("microbial_2015"):
        temp_df = temp_df._append(summary_dict_netcdf[key_value], ignore_index=True)

sns.lineplot(
    data=temp_df,
    ax=ax[1][0],
)
ax[1][0].set_title("Microbial netCDF data for 2015")
ax[1][0].set_xlabel("Months")

temp_df = pd.DataFrame()
for key_value in summary_dict_cog.keys():
    if key_value.startswith("microbial_2015"):
        temp_df = temp_df._append(summary_dict_cog[key_value], ignore_index=True)
sns.lineplot(
    data=temp_df,
    ax=ax[1][1],
)
ax[1][1].set_title("Microbial cog data for 2015")
ax[1][1].set_xlabel("Months")

plt.savefig("monthly_stats_summary.png")
plt.show()
