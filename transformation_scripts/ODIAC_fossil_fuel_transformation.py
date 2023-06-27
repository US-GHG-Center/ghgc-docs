import os
import xarray
import re
import pandas as pd
import json
import tempfile
import boto3

session = boto3.session.Session(profile_name="vs_code_user")
s3_client = session.client("s3")
# bucket_name = "veda-west2-shared"
bucket_name = "sample-bucket-cmip6"

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])
for name in os.listdir("1deg_ncdf"):
    xds = xarray.open_dataset(
        f"1deg_ncdf/{name}",
        engine="netcdf4",
    )
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]

    for time_increment in range(0, len(xds.month)):
        for var in variable:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = getattr(xds.isel(month=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)

            date = (
                f"0{data.month.item(0)}"
                if len(str(data.month.item(0))) == 1
                else f"{data.month.item(0)}"
            )
            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = filename_elements[-1] + date
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
                    Key=f"ODIAC_COGs/{cog_filename}",
                )

            files_processed = files_processed._append(
                {"file_name": name, "COGs_created": cog_filename},
                ignore_index=True,
            )

            print(f"Generated and saved COG: {cog_filename}")

with tempfile.NamedTemporaryFile(mode="w+") as fp:
    json.dump(xds.attrs, fp)
    json.dump({"data_dimensions": dict(xds.dims)}, fp)
    json.dump({"data_variables": list(xds.data_vars)}, fp)
    fp.flush()

    s3_client.upload_file(
        Filename=fp.name,
        Bucket=bucket_name,
        Key="ODIAC_COGs/metadata.json",
    )
files_processed.to_csv(
    f"s3://{bucket_name}/ODIAC_COGs/files_converted.csv",
    storage_options={"profile": "vs_code_user"},
)
print("Done generating COGs")
