import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3
from glob import glob
import netCDF4 as nc

session = boto3.session.Session(profile_name="veda-smce-mfa")
s3_client = session.client("s3")
bucket_name = "ghgc-data-store-dev"
date_fmt = "%Y%m"

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])
data_files = glob("data/no2-l3-v01/*.nc", recursive=True)
for name in data_files[:1]:
    xds = xarray.open_dataset(name, engine="netcdf4", group="product")
    # xds = nc.Dataset(name)['product']['vertical_column_troposphere']
    # xds = xds.assign_coords(
    #     longitude=(((xds.longitude + 180) % 360) - 180)
    # ).sortby("longitude")
    variable = "vertical_column_troposphere"

    # for time_increment in range(0, len(xds.time)):
    # for var in variable[:-1]:
    # filename = name.split("/ ")[-1]
    # filename_elements = re.split("[_ .]", filename)
    data = getattr(xds, variable)
    # data = data.isel(latitude=slice(None, None, -1))
    data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
    data.rio.write_crs("epsg:4326", inplace=True)
    data.rio.write_nodata(-9999, inplace=True)

    COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

    # date = data.time.dt.strftime(date_fmt).item(0)
    # # # insert date of generated COG into filename
    # filename_elements.pop()
    # filename_elements[-1] = date
    # filename_elements.insert(2, var)
    # cog_filename = "_".join(filename_elements)
    # # add extension
    cog_filename = f"{name}.tif"

    with tempfile.NamedTemporaryFile() as temp_file:
        data.rio.to_raster(
            temp_file.name,
            **COG_PROFILE,
        )
        s3_client.upload_file(
            Filename=temp_file.name,
            Bucket=bucket_name,
            Key=f"no2-l3-v01/{cog_filename}",
        )

    # files_processed = files_processed._append(
    #     {"file_name": name, "COGs_created": cog_filename},
    #     ignore_index=True,
    # )

    print(f"Generated and saved COG: {cog_filename}")

# with tempfile.NamedTemporaryFile(mode="w+") as fp:
#     json.dump(xds.attrs, fp)
#     json.dump({"data_dimensions": dict(xds.dims)}, fp)
#     json.dump({"data_variables": list(xds.data_vars)}, fp)
#     fp.flush()

#     s3_client.upload_file(
#         Filename=fp.name,
#         Bucket=bucket_name,
#         Key="GEOS-Carbs/metadata.json",
#     )
# files_processed.to_csv(
#     f"s3://{bucket_name}/GEOS-Carbs/files_converted.csv",
# )
print("Done generating COGs")
