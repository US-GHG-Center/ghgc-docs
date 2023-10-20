import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns

ds = xr.open_dataset('LOCAL/FILE/PATH')

# 14 variables
variables = [
    'LNLGIS_NBE',
    'LNLGIS_NBE_std',
    'LNLGIS_NCE',
    'LNLGIS_NCE_std',
    'LNLGIS_dC_loss',
    'LNLGIS_dC_loss_std',
    'Crop',
    'Crop_std',
    'FF',
    'FF_std',
    'River',
    'River_std',
    'Wood',
    'Wood_std'    
]

index_vals = {
    0:'2015',
    1:'2016',
    2:'2017',
    3:'2018',
    4:'2019',
    5:'2020'
}

# raw data to dataframe
def nc_to_df(ds, var):
    dataset = ds[var]
    arr_3d = dataset.to_numpy()
    arr = arr_3d.reshape( len(arr_3d), -1 )
    df = pd.DataFrame(arr)
    # reindex df to each year
    df = df.rename(index = index_vals)
    return df.T


# Dictionaries of variables for raw data and COGs
cog_dict, ncdf_dict = {}, {}

# populate raw data dictionary
for variable in variables:
    ncdf_dict[variable] = nc_to_df(ds, variable)

   
# convert cog data to a dataframe
def cog_to_df(file):
    src = rasterio.open(file)
    data = src.read(1)
    df = pd.DataFrame(data).values.flatten()
    return df

file_path = 'LOCAL/FILE/PATH'

years = [
    2015, 2016, 2017, 2018, 2019, 2020
]

# populate COG dictionary
for variable in variables:
    cog_data = {
        year: cog_to_df(file_path + f'pilot_topdown_{variable}_CO2_Budget_grid_v1_{year}.tif') for year in years
    }

    cog_dict[variable] = pd.DataFrame(cog_data)


# comparing values of raw data and COGs
def calculate_stats(df, df2):
    
    stats_dict = {         
        'COG Min': np.float64(np.nanmin(df.values)), #min
        'Raw Min': np.float64(np.nanmin(df2.values)), 
        
        'COG Max': np.float64(np.nanmax(df.values)), # max
        'Raw Max': np.float64(np.nanmax(df2.values)), 
        
        'COG Mean': np.float64(np.nanmean(df.values)), # mean
        'Raw Mean': np.float64(np.nanmean(df2.values)),
        
        'COG Std': np.float64(np.nanstd(df.values)), # std dev.
        'Raw Std': np.float64(np.nanstd(df2.values)),     
    }
    
    return stats_dict



# comparing df statistics
calculate_stats(cog_dict['LNLGIS_NBE'], ncdf_dict['LNLGIS_NBE'])
calculate_stats(cog_dict['LNLGIS_NBE_std'], ncdf_dict['LNLGIS_NBE_std'])
calculate_stats(cog_dict['Crop'], ncdf_dict['Crop'])

# Side by side Distribution plots    
def distribution_plot(df1, df2, title1, title2):
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    
    sns.histplot(df1, bins=100, palette='Blues', ax=axes[0])
    axes[0].set_title(title1)
    
    sns.histplot(df2, bins=100, palette='Blues', ax=axes[1])
    axes[1].set_title(title2)
    
    axes[0].legend().set_visible(False)
    axes[1].legend().set_visible(False)
    
    plt.tight_layout()
    
    return fig

# Plot distribution of values for each variable
distribution_plot(ncdf_dict['LNLGIS_NBE'], cog_dict['LNLGIS_NBE'], 'LNLGIS NBE (Raw data) Distribution', 'LNLGIS NBE (COG) Distribution')
distribution_plot(ncdf_dict['LNLGIS_NBE_std'], cog_dict['LNLGIS_NBE_std'], 'LNLGIS NBE std (Raw data) Distribution', 'LNLGIS NBE std (COG) Distribution')
distribution_plot(ncdf_dict['LNLGIS_NCE'], cog_dict['LNLGIS_NCE'], 'LNLGIS NCE (Raw data) Distribution', 'LNLGIS NCE std (COG) Distribution')
distribution_plot(ncdf_dict['LNLGIS_NCE_std'], cog_dict['LNLGIS_NCE_std'], 'LNLGIS NCE std (Raw data) Distribution', 'LNLGIS NCE std (COG) Distribution')
distribution_plot(ncdf_dict['LNLGIS_dC_loss'], cog_dict['LNLGIS_dC_loss'], 'LNLGIS dC Loss (Raw data) Distribution', 'LNLGIS dC Loss (COG) Distribution')
distribution_plot(ncdf_dict['LNLGIS_NCE_std'], cog_dict['LNLGIS_NCE_std'], 'LNLGIS NCE std (Raw data) Distribution', 'LNLGIS NCE std (COG) Distribution')
distribution_plot(ncdf_dict['Crop'], cog_dict['Crop'], 'Crop (Raw data) Distribution', 'Crop (COG) Distribution')
distribution_plot(ncdf_dict['Crop_std'], cog_dict['Crop_std'], 'Crop std (Raw data) Distribution', 'Crop std (COG) Distribution')
distribution_plot(ncdf_dict['FF'], cog_dict['FF'], 'FF (Raw data) Distribution', 'FF (COG) Distribution')
distribution_plot(ncdf_dict['FF_std'], cog_dict['FF_std'], 'FF std (Raw data) Distribution', 'FF std (COG) Distribution')
distribution_plot(ncdf_dict['River'], cog_dict['River'], 'River (Raw data) Distribution', 'River (COG) Distribution')
distribution_plot(ncdf_dict['River_std'], cog_dict['River_std'], 'River std (Raw data) Distribution', 'River std (COG) Distribution')
distribution_plot(ncdf_dict['Wood'], cog_dict['Wood'], 'Wood (Raw data) Distribution', 'Wood (COG) Distribution')
distribution_plot(ncdf_dict['Wood_std'], cog_dict['Wood_std'], 'Wood std (Raw data) Distribution', 'Wood std (COG) Distribution')
