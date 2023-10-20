import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns

# read netCDF file
ds = xr.open_dataset('FILE/PATH')

def nc_variable(ds, variable):
    var = ds[variable]
    df = pd.DataFrame(var)
    return df

variables = [
    'prior_total',
    'post_total',
    'prior_wetland',
    'post_wetland',
    'prior_unc_wetland',
    'post_unc_wetland'
]

cog_dict, ncdf_dict = {}, {}

# populate ncdf dictionary
for variable in variables:
    ncdf_dict[variable] = nc_variable(ds, variable)  


# COGs
def convert_cog(file):
    src = rasterio.open(file)
    data = src.read(1)
    df = pd.DataFrame(data)
    return df

file_path = '/FILE/PATH'

# populate COG dictionary 
for variable in variables:
    cog_dict[variable] = convert_cog(file_path+variable+'.tif')
    
    
# calculate statistics for COG and raw data
def compare_stats(cog, ncdf):
    
    stats_dict = {
        'COG': (
            round(np.float64(np.nanmin(cog.values)), 2), # min
            round(np.float64(np.nanmax(cog.values)), 2), # max
            round(np.float64(np.nanmean(cog.values)), 2), # mean
            round(np.float64(np.nanstd(cog.values)), 2), # std dev.
        ),
        'netCDF': (
            round(np.float64(np.nanmin(ncdf.values)), 2),
            round(np.float64(np.nanmax(ncdf.values)), 2),
            round(np.float64(np.nanmean(ncdf.values)), 2),
            round(np.float64(np.nanstd(ncdf.values)), 2),
        )
    }
    
    return stats_dict

# histogram function
def plot_hist(df1, df2, title, xlim=0, ylim=3):
    
    cols = ('Min', 'Max','Mean','Std. Deviation')
    stats = compare_stats(df1, df2)
    x = np.arange(len(cols))
    w, mult = 0.25, 0
    fig, ax = plt.subplots()
    
    # iterate through dictionary of statistics
    for key, value in stats.items():
        offset = w * mult
        bars = ax.bar(x + offset, value, w, label=key)
        ax.bar_label(bars, padding=3)
        mult += 1
        
    ax.set_ylabel('Value')
    ax.set_title(title)
    ax.set_xticks(x+0.125, cols)
    ax.set_ylim(xlim, ylim)
    ax.legend(loc="upper left", ncols=2)
    plt.axhline(0, color='black')
    
    return plt.show()

# Plot figure for each variable
plot_hist(ncdf_dict['prior_total'], cog_dict['prior_total'], 'Priori Total variable Plot', 0, 2.5)
plot_hist(ncdf_dict['post_total'], cog_dict['post_total'], 'Posterior Total variable Plot', 0, 4.5)
plot_hist(ncdf_dict['post_wetland'], cog_dict['post_wetland'], 'Posterior Wetland variable Plot', -1.25, 2.5)
plot_hist(ncdf_dict['post_unc_wetland'], cog_dict['post_unc_wetland'], 'Posterior Uncertain Wetland Stats', 0, 0.5)
plot_hist(ncdf_dict['prior_wetland'], cog_dict['prior_wetland'], 'Priori Wetlands Variable Stats', 0, 2)
plot_hist(ncdf_dict['prior_unc_wetland'], cog_dict['prior_unc_wetland'], 'Priori Uncertain Wetland',0, 2)


def calculate_stats(df):
    
    stats_dict = {
         'Min':np.float64(np.nanmin(df.values)),
         'Max':np.float64(np.nanmax(df.values)),
         'Mean':np.float64(np.nanmean(df.values)),
         'Std. Deviation':np.float64(np.nanstd(df.values))
    }
    
    return stats_dict