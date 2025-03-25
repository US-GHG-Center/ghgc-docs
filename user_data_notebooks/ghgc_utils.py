import requests
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from matplotlib.colors import rgb2hex
import numpy as np

def raster_stats(item, geojson,**kwargs):
    """
    Returns Raster API statistics for an item. Inputs: item, geojson, url = Raster API url, asset = asset name within item. Outputs: dictionary containing statistics over the bounding box and item's datetime information.
    """

    # A POST request is made to submit the data associated with the item of interest (specific observation) within the boundaries of the polygon to compute its statistics
    try:
        result = requests.post(

            # Raster API Endpoint for computing statistics
            f"{kwargs['url']}/cog/statistics",

            # Pass the URL to the item, asset name, and raster identifier as parameters
            params={"url": item["assets"][kwargs['asset']]["href"]},

            # Send the GeoJSON object (polygon) along with the request
            json=geojson,

        # Return the response in JSON format
        ).json()
    except KeyError as err:
        print('Make sure you include \'url\' and \'asset\' as keyword arguments!')
        raise err

    # Print the result
    ##print(result)

    # Return a dictionary containing the computed statistics along with the item's datetime information.
    try:
        return {
            **result["properties"],
            "datetime": item["properties"]["start_datetime"],
        }
    except KeyError as err:
        return {
            **result["features"][0]["properties"],
            'datetime': item["properties"]["start_datetime"],
        }

def clean_stats(stats_json) -> pd.DataFrame:
    """
    Takes dictionary output from generate_stats() and returns a neater, more intuitively-titled pandas DataFrame.
    """
    pd.set_option('display.float_format', '{:.20f}'.format)
    stats_json_ = [stats_json[datetime] for datetime in stats_json] 
    # Normalize the JSON data 
    df = pd.json_normalize(stats_json_)

    # Replace the naming "statistics.b1" in the columns
    df.columns = [col.replace("statistics.b1.", "") for col in df.columns]

    # Set the datetime format
    df["date"] = pd.to_datetime(df["datetime"])

    # Return the cleaned format
    return df

def generate_stats(items,geojson,**kwargs):
    """
    Runs raster_stats() and clean-stats() on all items. Inputs: List containing multiple items; geojson; url = URL for Raster API, asset = asset name for item field. Outputs: Pandas DataFrame of cleaned statistics for all items in list.
    """
    stats = {}
    print('Generating stats...')
    for item in items:
        date = item["properties"]["start_datetime"]  # Get the associated date
        year_month = date[:7].replace('-', '')  # Convert datetime to year-month
        stats[year_month] = raster_stats(item, geojson,**kwargs)
    df = clean_stats(stats)
    print('Done!')
    return df

def generate_html_colorbar(color_map,rescale_values,label=None,dark=False):
    """
    Creates html-formatted string which can be added to Folium maps to display a colorbar. Required inputs: colormap (matplotlib-accepted string), rescale_values in the form of a dictionary containing keys 'max' and 'min' which specify the desired colorbar range. Optional inputs: label, which will display above the colorbar. Output: html-formatted string detailing construction of the colorbar.
    """
    # Pull out colors from our chosen colormap
    cmap = plt.get_cmap(color_map)
    colors = cmap(np.linspace(0,1,11))
    colors = [rgb2hex(c) for c in colors]
    # Define custom tick values for the legend bar
    tick_val = np.round(np.linspace(rescale_values['min'],rescale_values['max'],5),decimals=6)
    # Create a HTML representation
    legend_html = cmap._repr_html_()

    # Create a customized HTML structure for the legend
#    legend_html = f'''
#    <div style="position: fixed; bottom: 50px; left: 175px; z-index: 1000; width: 400px; height: auto; #background-color: rgba(255, 255, 255, 0.8);
#             border-radius: 5px; border: 1px solid grey; padding: 10px; font-size: 12px; color: black;">
#        <b>{label}</b><br>
#        <div style="display: flex; justify-content: space-between;">
#            <div>{tick_val[0]}</div> 
#            <div>{tick_val[1]}</div> 
#            <div>{tick_val[2]}</div> 
#            <div>{tick_val[3]}</div> 
#            <div>{tick_val[4]}</div> 
#        </div>
#        <div style="background: linear-gradient(to right,
#                {colors[0]}, {colors[1]} {20}%,
#                {colors[1]} {20}%, {colors[2]} {40}%,
#                {colors[2]} {40}%, {colors[3]} {50}%,
#                {colors[3]} {50}%, {colors[4]} {80}%,
#                {colors[4]} {80}%, {colors[5]}); height: 10px;"></div>
#    </div>
#    '''
    if dark:
        bg_color = "rgba(0, 0, 0, 0.8)"
        font_color="white"
    else:
        bg_color = "rgba(255, 255, 255, 0.8)"
        font_color="black"
    
    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 175px; z-index: 1000; width: 400px; height: auto; background-color: {bg_color};
             border-radius: 5px; border: 1px solid grey; padding: 10px; font-size: 12px; color: {font_color};">
        <b>{label}</b><br>
        <div style="display: flex; justify-content: space-between;">
            <div>{tick_val[0]}</div> 
            <div>{tick_val[1]}</div> 
            <div>{tick_val[2]}</div> 
            <div>{tick_val[3]}</div> 
            <div>{tick_val[4]}</div>
        </div>
        <div style="background: linear-gradient(to right,
                {colors[0]}, {colors[1]} {10}%,
                {colors[1]} {10}%, {colors[2]} {20}%,
                {colors[2]} {20}%, {colors[3]} {30}%,
                {colors[3]} {30}%, {colors[4]} {40}%,
                {colors[4]} {40}%, {colors[5]} {50}%,
                {colors[5]} {50}%, {colors[6]} {60}%,
                {colors[6]} {60}%, {colors[7]} {70}%,
                {colors[7]} {70}%, {colors[8]} {80}%,
                {colors[8]} {80}%, {colors[9]} {90}%,
                {colors[9]} {90}%, {colors[10]}); height: 10px;"></div>
    </div>
    '''
    return legend_html