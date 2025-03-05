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
from datetime import datetime, timedelta

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

def goes_radF_C08_transformation(file_obj, name: str, nodata: int) -> Dict[str, xr.DataArray]:
    """Process a single GOES NetCDF file from S3File object and return transformed DataArray."""
    var_data_netcdf = {}
    with xr.open_dataset(file_obj, engine="h5netcdf") as ds:
        time_coverage_start = ds.attrs.get("time_coverage_start", None)
        if not time_coverage_start:
            raise ValueError("No 'time_coverage_start' attribute found.")
        
        time_parts = time_coverage_start.split("T")
        formatted_time = f"{time_parts[0]}T{time_parts[1].replace('.', '')}"[:19]

        

        # Create the output filename
        band_name = name.split("_G16_s")[0][-5:]  # Split before "G16" and take band
        timestamp = name.split("_G16_s")[-1][:11]
        
        year = int(timestamp[:4])      # 2024
        doy = int(timestamp[4:7])      # 181
        hour = int(timestamp[7:9])     # 06
        minute = int(timestamp[9:11])  # 00
        second = 0
        
        # Convert DOY to MM-DD
        date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        formatted_date = date.strftime("%Y-%m-%d")
        
        # Format the final timestamp
        iso_timestamp = f"{formatted_date}T{hour:02d}:{minute:02d}:{second:02d}Z"

        cog_filename = f"G16ABI_{band_name}_{iso_timestamp}.tif"
        
        da = goes_to_wgs(ds, variable_name="Rad")
        
        # Clip to AOI
        da = da.rio.clip(aoi_gdf.geometry, aoi_gdf.crs, drop=True)
        da = da.fillna(nodata)

        da.rio.write_crs("EPSG:4326", inplace=True)
        da = da.rio.set_nodata(-9999)

        var_data_netcdf[cog_filename] = da
        
        return var_data_netcdf
