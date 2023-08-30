# This script was used to transform SEDAC population density dataset from netCDF to Cloud Optimized GeoTIFF (COG) format for display in the Greenhouse Gas (GHG) Center.

import os
import xarray
import re
import pandas as pd

import tempfile
import boto3

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = (
    "ghgc-data-store-dev"  # S3 bucket where the COGs are stored after transformation
)

fold_names = os.listdir("gpw")

files_processed = pd.DataFrame(
    columns=["file_name", "COGs_created"]
)  # A dataframe to keep track of the files that we have transformed into COGs

# Reading the raw netCDF files from local machine
for fol_ in fold_names:
    for name in os.listdir(f"gpw/{fol_}"):
        if name.endswith(".tif"):
            xds = xarray.open_dataarray(f"gpw/{fol_}/{name}")

            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements.append(filename_elements[-3])

            xds.rio.set_spatial_dims("x", "y", inplace=True)
            xds.rio.write_crs("epsg:4326", inplace=True)

            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"

            with tempfile.NamedTemporaryFile() as temp_file:
                xds.rio.to_raster(temp_file.name, driver="COG")
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=bucket_name,
                    Key=f"gridded_population_cog/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")


# creating the csv file with the names of files transformed.
files_processed.to_csv(
    f"s3://{bucket_name}/gridded_population_cog/files_converted.csv",
)
print("Done generating COGs")
