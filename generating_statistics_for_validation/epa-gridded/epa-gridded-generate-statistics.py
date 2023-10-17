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
import collections

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

    kwargs = {
        "Bucket": bucket,
        "Prefix": "epa_emissions_express_extension/Express_Extension_emi_",
    }
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
tif_files = glob("../../data/epa_emissions_express_extension/*.nc", recursive=True)
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
                        ["_".join(filename_elements[8:17])],
                        [filename_elements[-3]],
                        [x for x in np.arange(1, src.height + 1)],
                    ]
                )
                # Read the raster data
                raster_data = src.read(band)
                raster_data[raster_data == -9999] = np.nan
                temp = pd.DataFrame(index=idx, data=raster_data)
                full_data_df_cog = full_data_df_cog._append(temp, ignore_index=False)

                # Calculate summary statistics
                min_value = np.float64(np.nanmin(temp.values))
                max_value = np.float64(np.nanmax(temp.values))
                mean_value = np.float64(np.nanmean(temp.values))
                std_value = np.float64(np.nanstd(temp.values))

                summary_dict_cog[
                    f"{'_'.join(filename_elements[8:17])}_{filename_elements[-3]}"
                ] = {
                    "min_value": min_value,
                    "max_value": max_value,
                    "mean_value": mean_value,
                    "std_value": std_value,
                }

COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}
# Iterate over each TIFF file
for tif_file in tif_files:
    file_name = re.split("[_ ? . ]", pathlib.Path(tif_file).name[:-3])

    xds = xarray.open_dataset(f"{tif_file}", engine="netcdf4")
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]
    # start_time = datetime(int(filename_elements[-3]), 1, 1)

    for time_increment in range(0, len(xds.time)):
        for var in variable:
            data = getattr(xds.isel(time=time_increment), var)
            data = np.round(data / pow(10, 9), 2)
            data = data.isel(lat=slice(None, None, -1))
            idx = pd.MultiIndex.from_product(
                [
                    [
                        "_".join(
                            [
                                var,
                                file_name[2],
                                file_name[3],
                                file_name[4],
                                file_name[5],
                            ]
                        )
                    ],
                    [file_name[-1]],
                    [x for x in np.arange(1, len(data.lat) + 1)],
                ]
            )

            temp = pd.DataFrame(index=idx, data=data)
            full_data_df_netcdf = full_data_df_netcdf._append(temp, ignore_index=False)

            # Calculate summary statistics
            min_value = np.float64(np.nanmin(temp.values))
            max_value = np.float64(np.nanmax(temp.values))
            mean_value = np.float64(np.nanmean(temp.values))
            std_value = np.float64(np.nanstd(temp.values))

            summary_dict_netcdf[f"{var}_{'_'.join(file_name[2:6])}_{file_name[-1]}"] = {
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

summary_dict_cog = collections.OrderedDict(sorted(summary_dict_cog.items()))
summary_dict_netcdf = collections.OrderedDict(sorted(summary_dict_netcdf.items()))

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


fig, ax = plt.subplots(2, 2, figsize=(13, 11))
temp_df = pd.DataFrame()
for key_value in full_data_df_netcdf.index.values:
    if key_value[0].startswith("emi_ch4_3A_Enteric_Fermentation"):
        temp_df = temp_df._append(full_data_df_netcdf.loc[key_value])
temp_df = temp_df.to_numpy().flatten()
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[0][0])
ax[0][0].set_title("Agriculture - Enteric Fermentation \n (Original Data)")

temp_df = pd.DataFrame()
for key_value in full_data_df_cog.index.values:
    if key_value[0].startswith("emi_ch4_3A_Enteric_Fermentation"):
        temp_df = temp_df._append(full_data_df_cog.loc[key_value])
temp_df = temp_df.to_numpy().flatten()
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[0][1])
ax[0][1].set_title("Agriculture - Enteric Fermentation \n (Transformed COG Data)")

temp_df = pd.DataFrame()
for key_value in full_data_df_netcdf.index.values:
    if key_value[0].startswith("emi_ch4_1B2b_Natural_Gas_Production"):
        temp_df = temp_df._append(full_data_df_netcdf.loc[key_value])
temp_df = temp_df.to_numpy().flatten()
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[1][0])
ax[1][0].set_title("Natural Gas-Production \n (Original Data)")

temp_df = pd.DataFrame()
for key_value in full_data_df_cog.index.values:
    if key_value[0].startswith("emi_ch4_1B2b_Natural_Gas_Production"):
        temp_df = temp_df._append(full_data_df_cog.loc[key_value])
temp_df = temp_df.to_numpy().flatten()
sns.histplot(data=temp_df, kde=False, bins=10, legend=False, ax=ax[1][1])
ax[1][1].set_title("Natural Gas-Production \n (Transformed COG data)")

fig.tight_layout(pad=1)
fig.suptitle("Overall distribution of data", fontsize=12)
plt.savefig("overall_stats_summary.png")
plt.show()

x_label = ["2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020"]
fig, ax = plt.subplots(2, 2, figsize=(12, 12))
temp_df1, temp_df2 = pd.DataFrame(), pd.DataFrame()
for key_value in summary_dict_netcdf.keys():
    if key_value.startswith("emi_ch4_3A_Enteric_Fermentation"):
        temp_df1 = temp_df1._append(summary_dict_netcdf[key_value], ignore_index=True)
        temp_df2 = temp_df2._append(summary_dict_cog[key_value], ignore_index=True)
temp_df1["years"] = x_label
temp_df2["years"] = x_label
temp_df1.set_index("years", inplace=True)
temp_df2.set_index("years", inplace=True)
sns.lineplot(
    data=temp_df1,
    ax=ax[0][0],
)
ax[0][0].set_title("Agriculture - Enteric Fermentation \n (Original Data)")
ax[0][0].set_xlabel("Years")

sns.lineplot(
    data=temp_df2,
    ax=ax[0][1],
)
ax[0][1].set_title("Agriculture - Enteric Fermentation \n (Transformed COG Data)")
ax[0][1].set_xlabel("Years")

temp_df = pd.DataFrame()
for key_value in summary_dict_netcdf.keys():
    if key_value.startswith("emi_ch4_1B2b_Natural_Gas_Production"):
        temp_df = temp_df._append(summary_dict_netcdf[key_value], ignore_index=True)

temp_df["years"] = x_label
temp_df.set_index("years", inplace=True)
sns.lineplot(
    data=temp_df,
    ax=ax[1][0],
)
ax[1][0].set_title("Natural Gas - Production\n (Original Data)")
ax[1][0].set_xlabel("Years")

temp_df = pd.DataFrame()
for key_value in summary_dict_cog.keys():
    if key_value.startswith("emi_ch4_1B2b_Natural_Gas_Production"):
        temp_df = temp_df._append(summary_dict_cog[key_value], ignore_index=True)
temp_df["years"] = x_label
temp_df.set_index("years", inplace=True)
sns.lineplot(
    data=temp_df,
    ax=ax[1][1],
)
ax[1][1].set_title("Natural Gas - Production \n (Transformed COG Data)")
ax[1][1].set_xlabel("Years")

fig.suptitle("Plot for the Statistical values of data", fontsize=12)
plt.savefig("Yearly_stats_summary.png")
plt.show()
