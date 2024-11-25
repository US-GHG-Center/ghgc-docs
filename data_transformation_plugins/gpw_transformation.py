import re
from typing import Dict

import xarray
from s3fs import S3File
from xarray import DataArray


def gpw_transformation(
    file_obj: S3File, name: str, nodata: int
) -> Dict[str, DataArray]:
    """Transformation function for the gridded population dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """

    var_data_netcdf = {}
    xds = xarray.open_dataarray(file_obj, engine="rasterio")

    filename = name.split("/")[-1]
    filename_elements = re.split("[_ .]", filename)
    # # insert date of generated COG into filename
    filename_elements.pop()
    filename_elements.append(filename_elements[-3])
    xds = xds.where(xds != nodata, -9999)
    xds.rio.set_spatial_dims("x", "y", inplace=True)
    xds.rio.write_crs("epsg:4326", inplace=True)
    xds.rio.write_nodata(-9999, inplace=True)

    cog_filename = "_".join(filename_elements)
    # # add extension
    cog_filename = f"{cog_filename}.tif"
    var_data_netcdf[cog_filename] = xds

    return var_data_netcdf
