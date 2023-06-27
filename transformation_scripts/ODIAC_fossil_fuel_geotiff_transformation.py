import os
import xarray
import re
import pandas as pd

import tempfile
import boto3

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = "ghgc-data-store-dev"

fold_names = os.listdir("ODIAC")

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])


for fol_ in fold_names:
    for name in os.listdir(f"ODIAC/{fol_}"):
        xds = xarray.open_dataarray(f"ODIAC/{fol_}/{name}")

        filename = name.split("/ ")[-1]
        filename_elements = re.split("[_ .]", filename)
        # # insert date of generated COG into filename
        filename_elements.pop()
        filename_elements[-1] = fol_ + filename_elements[-1][-2:]

        xds.rio.set_spatial_dims("x", "y", inplace=True)
        xds.rio.write_nodata(-9999, inplace=True)
        xds.rio.write_crs("epsg:4326", inplace=True)

        cog_filename = "_".join(filename_elements)
        # # add extension
        cog_filename = f"{cog_filename}.tif"

        with tempfile.NamedTemporaryFile() as temp_file:
            xds.rio.to_raster(
                temp_file.name,
                driver="COG",
            )
            s3_client.upload_file(
                Filename=temp_file.name,
                Bucket=bucket_name,
                Key=f"ODIAC_geotiffs_COGs/{cog_filename}",
            )

        files_processed = files_processed._append(
            {"file_name": name, "COGs_created": cog_filename},
            ignore_index=True,
        )

        print(f"Generated and saved COG: {cog_filename}")

files_processed.to_csv(
    f"s3://{bucket_name}/ODIAC_COGs/files_converted.csv",
)
print("Done generating COGs")
