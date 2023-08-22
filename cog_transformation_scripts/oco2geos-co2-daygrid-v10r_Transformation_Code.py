import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3

session = boto3.session.Session()
s3_client = session.client("s3")
bucket_name = "ghgc-data-store-dev"
FOLDER_NAME = "earth_data/geos_oco2"
s3_folder_name = "geos-oco2"

error_files = []
files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])
for name in os.listdir(FOLDER_NAME):
    try:
        xds = xarray.open_dataset(f"{FOLDER_NAME}/{name}", engine="netcdf4")
    except OSError:
        error_files.append(name)
        print(name)
        break
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]
    filename = name.split("/ ")[-1]
    filename_elements = re.split("[_ .]", filename)

    for time_increment in range(0, len(xds.time)):
        for var in variable:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)

            # # insert date of generated COG into filename
            filename_elements[-1] = filename_elements[-3]
            filename_elements.insert(2, var)
            filename_elements.pop(-3)
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
                    Key=f"{s3_folder_name}/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")

print(len(error_files))


with tempfile.NamedTemporaryFile(mode="w+") as fp:
    json.dump(xds.attrs, fp)
    json.dump({"data_dimensions": dict(xds.dims)}, fp)
    json.dump({"data_variables": list(xds.data_vars)}, fp)
    fp.flush()

    s3_client.upload_file(
        Filename=fp.name,
        Bucket=bucket_name,
        Key=f"{s3_folder_name}/metadata.json",
    )
files_processed.to_csv(
    f"s3://{bucket_name}/{s3_folder_name}/files_converted.csv",
)
print("Done generating COGs")
