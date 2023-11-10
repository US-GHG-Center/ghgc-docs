import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns

def convert_tif(file):
    src = rasterio.open(file)
    data = src.read(1)
    df = pd.DataFrame(data)
    return df

years = [
    '2000',
    '2005',
    '2010',
    '2015',
    '2020'
]
#GeoTIFF
geotiff_dict = {}

geotiff_file_path = 'LOCAL_FILE_PATH'
geotiff_data = convert_tif(geotiff_file_path+'gpw_v4_population_density_rev11_2000_30_sec.tif')

# for year in years:
#     geotiff_dict[year] = convert_tif(geotiff_file_path + f'gpw_v4_population_density_rev11_{year}_30_sec.tif')


def calculate_stats(df):
    
    stats_dict = {
         'Min':np.float64(np.nanmin(df.values)),
         'Max':np.float64(np.nanmax(df.values)),
         'Mean':np.float64(np.nanmean(df.values)),
         'Std. Deviation':np.float64(np.nanstd(df.values))
    }
    
    return stats_dict


geotiff_2000_stats = calculate_stats(geotiff_data)

#COG
cog_dict = {}

cog_file_path = 'LOCAL_FILE_PATH'
cog_data = convert_tif(cog_file_path+'gpw_v4_population_density_rev11_2005_30_sec_2005.tif')

for year in years:
    cog_dict[year] = convert_tif(cog_file_path + f'gpw_v4_population_density_rev11_{year}_30_sec_{year}.tif')

cog_2000_stats = calculate_stats(cog_dict['2000'])
