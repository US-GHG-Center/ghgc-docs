import xarray as xr
from typing import Dict


def odiac_ffco2_monthgrid_v2024_transformation(file_obj, name: str, nodata: int) -> Dict[str, xr.DataArray]:
    """Process a single ODIAC tif file from S3File object and return transformed DataArray."""
    var_data_netcdf = {}
    with xr.open_dataarray(file_obj,engine="rasterio") as ds:
        xds = xds.where(xds!=0, -9999)
        xds.rio.set_spatial_dims("x", "y", inplace=True)
        xds.rio.write_nodata(-9999, inplace=True)
        xds.rio.write_crs("epsg:4326", inplace=True)

        var_data_netcdf[name] = xds

        return var_data_netcdf


