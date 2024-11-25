import numpy as np
import boto3
import pandas as pd
import json
import yaml

# Step 1: Connect to the S3 bucket
s3 = boto3.client('s3')
bucket_name = 'ghgc-data-store-develop'
prefix = 'transformed_cogs/tm5-4dvar-update-noaa/'  # optional: specify if files are in a specific folder within the bucket

# Step 2: List all JSON files in the bucket
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
files = [file['Key'] for file in response.get('Contents', []) if file['Key'].endswith('.json') and 'fossil_emis_2015' in file['Key']]
print(len(files))
# Step 3: Load each JSON file into a DataFrame and combine
dataframes = []
for file_key in files:
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = json.load(obj['Body'])  # Load JSON content
    df = pd.json_normalize(data)    # Normalize JSON to flatten nested structures if needed
    dataframes.append(df)
# Combine all DataFrames into one
all_data = pd.concat(dataframes, ignore_index=True)
for name in all_data.columns[3:]:
    all_data[name] = all_data[name].astype(np.float32)

# Step 4: Calculate mean, std, min, and max for overall data
mean_values_netcdf = all_data["mean_value_netcdf"].mean()
std_values_netcdf = all_data["std_value_netcdf"].std()
min_values_netcdf = all_data["minimum_value_netcdf"].min()
max_values_netcdf = all_data["maximum_value_netcdf"].max()
mean_values_cog = all_data["mean_value_cog"].mean()
std_values_cog = all_data["std_value_cog"].std()
min_values_cog = all_data["minimum_value_cog"].min()
max_values_cog = all_data["maximum_value_cog"].max()

data_to_save = {
    "netcdf":{
        "mean":mean_values_netcdf,
        "std":std_values_netcdf,
        "min":min_values_netcdf,
        "max":max_values_netcdf,
    },
    "COG": {
        "mean":mean_values_cog,
        "std":std_values_cog,
        "min":min_values_cog,
        "max":max_values_cog,
    }

}
print(data_to_save)
# # Write the data to a YAML file
# output_yaml_path = "config.yaml"
# with open(output_yaml_path, "w") as yaml_file:
#     yaml.dump(data_to_save, yaml_file, default_flow_style=False)

