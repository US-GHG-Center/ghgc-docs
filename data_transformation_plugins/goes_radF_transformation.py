import xarray as xr
import rioxarray as rio
from rio_cogeo import cogeo
from pyproj import CRS
import geopandas as gpd
from shapely.geometry import Polygon
import numpy as np
from typing import Dict
from rasterio.enums import Resampling
from rasterio.io import MemoryFile

# Define AOI as a shapely Polygon
aoi_coords = [[-102.8148701375, 6.1943456775], [-13.3448605043, 6.1943456775], 
              [-13.3448605043, 49.6429910636], [-102.8148701375, 49.6429910636], 
              [-102.8148701375, 6.1943456775]]
aoi_polygon = Polygon(aoi_coords)
aoi_gdf = gpd.GeoDataFrame({'geometry': [aoi_polygon]}, crs="EPSG:4326")

def goes_to_wgs(ds, variable_name):
    """Reproject GOES data to WGS84"""
    sat_height = ds["goes_imager_projection"].attrs["perspective_point_height"]
    ds = ds.assign_coords({
        "x": ds["x"].values * sat_height,
        "y": ds["y"].values * sat_height,
    })
    
    crs = CRS.from_cf(ds["goes_imager_projection"].attrs)
    ds.rio.write_crs(crs.to_string(), inplace=True)
    
    da = ds[variable_name]
    return da.rio.reproject("EPSG:4326", resampling=Resampling.nearest)

def goes_transformation(file_obj, name: str, nodata: int) -> Dict[str, xr.DataArray]:
    """Process a single GOES NetCDF file from S3File object and return transformed DataArray."""
    with xr.open_dataset(file_obj, engine="h5netcdf") as ds:
        time_coverage_start = ds.attrs.get("time_coverage_start", None)
        if not time_coverage_start:
            raise ValueError("No 'time_coverage_start' attribute found.")
        
        time_parts = time_coverage_start.split("T")
        formatted_time = f"{time_parts[0]}T{time_parts[1].replace('.', '')}"[:19]

        # Create the output filename
        band_name = name.split("_G16")[0][-5:]  # Split before "G16" and take band
        cog_filename = f"G16ABI_{band_name}_{formatted_time}Z.tif"
        print(filename)
        
        da = goes_to_wgs(ds, variable_name="Rad")
        
        # Clip to AOI
        da = da.rio.clip(aoi_gdf.geometry, aoi_gdf.crs, drop=True)
        da = da.fillna(nodata)

        var_data_netcdf[cog_filename] = da
        
        return var_data_netcdf
