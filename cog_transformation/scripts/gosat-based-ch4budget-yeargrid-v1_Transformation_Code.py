# This script was used to transform the GOSAT-based Top-down Methane dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.

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


session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = (
    "ghgc-data-store-dev"  # S3 bucket where the COGs are stored after transformation
)
year_ = datetime(2019, 1, 1)
folder_name = "new_data/CH4-inverse-flux"

COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # A dataframe to keep track of the files that we have transformed into COGs

# Reading the raw netCDF files from local machine
for name in os.listdir(folder_name):
    ds = xarray.open_dataset(
        f"{folder_name}/{name}",
        engine="netcdf4",
    )

    ds = ds.rename({"dimy": "lat", "dimx": "lon"})
    # assign coords from dimensions
    ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180)).sortby("lon")
    ds = ds.assign_coords(lat=((ds.lat / 180) * 180) - 90).sortby("lat")

    variable = [var for var in ds.data_vars]

    for var in variable[2:]:
        filename = name.split("/ ")[-1]
        filename_elements = re.split("[_ .]", filename)
        data = ds[var]
        filename_elements.pop()
        filename_elements.insert(2, var)
        cog_filename = "_".join(filename_elements)
        # # add extension
        cog_filename = f"{cog_filename}.tif"

        data = data.reindex(lat=list(reversed(data.lat)))

        data.rio.set_spatial_dims("lon", "lat")
        data.rio.write_crs("epsg:4326", inplace=True)

        # generate COG
        COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

        with tempfile.NamedTemporaryFile() as temp_file:
            data.rio.to_raster(temp_file.name, **COG_PROFILE)
            s3_client.upload_file(
                Filename=temp_file.name,
                Bucket=bucket_name,
                Key=f"ch4_inverse_flux/{cog_filename}",
            )

        files_processed = files_processed._append(
            {"file_name": name, "COGs_created": cog_filename},
            ignore_index=True,
        )

        print(f"Generated and saved COG: {cog_filename}")

# Generate the json file with the metadata that is present in the netCDF files.
with tempfile.NamedTemporaryFile(mode="w+") as fp:
    json.dump(ds.attrs, fp)
    json.dump({"data_dimensions": dict(ds.dims)}, fp)
    json.dump({"data_variables": list(ds.data_vars)}, fp)
    fp.flush()

    s3_client.upload_file(
        Filename=fp.name,
        Bucket=bucket_name,
        Key="ch4_inverse_flux/metadata.json",
    )

# creating the csv file with the names of files transformed.
files_processed.to_csv(
    f"s3://{bucket_name}/ch4_inverse_flux/files_converted.csv",
)
print("Done generating COGs")
