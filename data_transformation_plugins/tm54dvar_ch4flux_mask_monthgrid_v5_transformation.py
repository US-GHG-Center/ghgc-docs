import xarray
from datetime import datetime
import re

def tm54dvar_ch4flux_mask_monthgrid_v5_transformation(file_obj, name, nodata):
    """Tranformation function for the tm5 ch4 influx dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """

    var_data_netcdf = {}
    xds = xarray.open_dataset(file_obj)
    xds = xds.rename({"latitude": "lat", "longitude": "lon"})
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars if "global" not in var]

    for time_increment in range(0, len(xds.months)):
        filename = name.split("/")[-1]
        filename_elements = re.split("[_ .]", filename)
        start_time = datetime(int(filename_elements[-2]), time_increment + 1, 1)
        for var in variable:
            data = getattr(xds.isel(months=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data = data.where(data == nodata, -9999)
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)

            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = start_time.strftime("%Y%m")
            filename_elements.insert(2, var)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"
            var_data_netcdf[cog_filename] = data

    return var_data_netcdf