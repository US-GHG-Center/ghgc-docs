import requests
import pandas as pd

RASTER_API_URL = "https://earth.gov/ghgcenter/api/raster"

def generate_stats(item, geojson, asset_name):
    """
    Retrieve statistics for a specific granule (item) within a GeoJSON-defined polygon.

    Args:
        item (dict): The granule containing item details (including assets and metadata).
        geojson (dict): A GeoJSON Feature or FeatureCollection specifying the bounding box.
        asset_name (str): The asset name or raster identifier to be used.

    Returns:
        dict: A dictionary with computed statistics and the item's datetime information.
    """
    result = requests.post(
        f"{RASTER_API_URL}/cog/statistics",
        params={"url": item["assets"][asset_name]["href"]},
        json=geojson,
    ).json()

    print(result)

    # Handle cases where either "start_datetime" or "datetime" is present
    datetime_value = item["properties"].get("start_datetime", item["properties"].get("datetime"))

    return {
        **result["properties"],
        "datetime": datetime_value,
    }



def clean_stats(stats_json):
    """
    Clean and normalize the statistics JSON data and convert it into a pandas DataFrame.

    Args:
        stats_json (list of dict): List of statistics dictionaries for each granule.

    Returns:
        pd.DataFrame: A DataFrame with flattened and cleaned statistics.
    """
    df = pd.json_normalize(stats_json)
    df.columns = [col.replace("statistics.b1.", "") for col in df.columns]
    df["date"] = pd.to_datetime(df["datetime"])
    return df


def display_stats(df, num_rows=5):
    """
    Display the top rows of the cleaned statistics DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the cleaned statistics.
        num_rows (int): Number of rows to display (default is 5).
    """
    print(df.head(num_rows))
