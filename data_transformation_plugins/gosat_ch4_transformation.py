import re

import xarray


def gosat_ch4_transformation(file_obj, name, nodata):
    """Tranformation function for the ecco darwin dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """
    var_data_netcdf = {}
    ds = xarray.open_dataset(file_obj)
    variable = [var for var in ds.data_vars]

    for var in variable:
        filename = name.split("/")[-1]
        filename_elements = re.split("[_ .]", filename)
        data = ds[var]
        filename_elements.pop()
        filename_elements.insert(2, var)
        cog_filename = "_".join(filename_elements)
        # # add extension
        cog_filename = f"{cog_filename}.tif"

        data = data.reindex(lat=list(reversed(data.lat)))
        data = data.where(data != -9999, -9999)
        data.rio.write_nodata(-9999, inplace=True)

        data.rio.set_spatial_dims("lon", "lat")
        data.rio.write_crs("epsg:4326", inplace=True)
        var_data_netcdf[cog_filename] = data

    return var_data_netcdf
