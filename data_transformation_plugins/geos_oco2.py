import xarray
import re

def geos_oco2_transformation(file_obj, name, nodata):
    """Tranformation function for the oco2 geos dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """
    var_data_netcdf = {}
    xds = xarray.open_dataset(file_obj)
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]
    for time_increment in range(0, len(xds.time)):
        for var in variable:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data = data.where(data == nodata, -9999)
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)
            # # insert date of generated COG into filename
            filename_elements[-1] = filename_elements[-3]
            filename_elements.insert(2, var)
            filename_elements.pop(-3)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"
            var_data_netcdf[cog_filename] = data

    return var_data_netcdf