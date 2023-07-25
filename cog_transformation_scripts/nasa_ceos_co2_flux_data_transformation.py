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
bucket_name = "ghgc-data-store-dev"
year_ = datetime(2015, 1, 1)

COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])
for name in os.listdir("new_data"):
    ds = xarray.open_dataset(
        f"new_data/{name}",
        engine="netcdf4",
    )
    ds = ds.rename({"latitude": "lat", "longitude": "lon"})
    # assign coords from dimensions
    ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180)).sortby("lon")
    ds = ds.assign_coords(lat=list(ds.lat))

    variable = [var for var in ds.data_vars]

    for time_increment in range(0, len(ds.year)):
        for var in variable[2:]:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            try:
                data = ds[var].sel(year=time_increment)
                date = year_ + relativedelta(years=+time_increment)
                filename_elements[-1] = date.strftime("%Y")
                # # insert date of generated COG into filename
                filename_elements.insert(2, var)
                cog_filename = "_".join(filename_elements)
                # # add extension
                cog_filename = f"{cog_filename}.tif"
            except KeyError:
                data = ds[var]
                date = year_ + relativedelta(years=+(len(ds.year) - 1))
                filename_elements.pop()
                filename_elements.append(year_.strftime("%Y"))
                filename_elements.append(date.strftime("%Y"))
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
                    Key=f"ceos_co2_flux/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")

files_processed.to_csv(
    f"s3://{bucket_name}/ceos_co2_flux/files_converted.csv",
)
print("Done generating COGs")
