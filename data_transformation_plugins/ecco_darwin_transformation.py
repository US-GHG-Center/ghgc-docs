import xarray
import re

def ecco_darwin_transformation(file_obj, name, nodata):
    """Tranformation function for the ecco darwin dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """
    var_data_netcdf = {}
    xds = xarray.open_dataset(file_obj)
    xds = xds.rename({"y": "latitude", "x": "longitude"})
    xds = xds.assign_coords(longitude=((xds.longitude / 1440) * 360) - 180).sortby(
        "longitude"
    )
    xds = xds.assign_coords(latitude=((xds.latitude / 721) * 180) - 90).sortby(
        "latitude"
    )

    variable = [var for var in xds.data_vars]

    for _ in xds.time.values:
        for var in variable[2:]:
            filename = name.split("/")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = xds[var]

            data = data.reindex(latitude=list(reversed(data.latitude)))
            data = data.where(data != nodata, -9999)
            data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)

            filename_elements.pop()
            filename_elements[-1] = filename_elements[-2] + filename_elements[-1]
            filename_elements.pop(-2)
            # # insert date of generated COG into filename
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"
            var_data_netcdf[cog_filename] = data
    return var_data_netcdf