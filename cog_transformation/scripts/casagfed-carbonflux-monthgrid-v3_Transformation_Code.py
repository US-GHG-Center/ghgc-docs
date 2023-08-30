# This script was used to transform the CASA GFED monthly dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.

import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = "ghgc-data-store-dev"  # S3 bucket where the COGs are stored
date_fmt = "%Y%m"

files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # a dataframe to keep track of the files converted

# Reading the raw netCDF files from local machine
for name in os.listdir("geoscarb"):
    xds = xarray.open_dataset(
        f"geoscarb/{name}",
        engine="netcdf4",
    )
    xds = xds.assign_coords(longitude=(((xds.longitude + 180) % 360) - 180)).sortby(
        "longitude"
    )
    variable = [var for var in xds.data_vars]

    for time_increment in range(0, len(xds.time)):
        for var in variable[:-1]:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(latitude=slice(None, None, -1))
            data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)

            date = data.time.dt.strftime(date_fmt).item(0)
            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = date
            filename_elements.insert(2, var)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"

            with tempfile.NamedTemporaryFile() as temp_file:
                data.rio.to_raster(
                    temp_file.name,
                    driver="COG",
                )
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=bucket_name,
                    Key=f"GEOS-Carbs/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")

# creating the json file with metadata gievn in the netCDF file
with tempfile.NamedTemporaryFile(mode="w+") as fp:
    json.dump(xds.attrs, fp)
    json.dump({"data_dimensions": dict(xds.dims)}, fp)
    json.dump({"data_variables": list(xds.data_vars)}, fp)
    fp.flush()

    s3_client.upload_file(
        Filename=fp.name,
        Bucket=bucket_name,
        Key="GEOS-Carbs/metadata.json",
    )
# CSV file with the names of netCDF files that have been transformed into the COG
files_processed.to_csv(
    f"s3://{bucket_name}/GEOS-Carbs/files_converted.csv",
)
print("Done generating COGs")
