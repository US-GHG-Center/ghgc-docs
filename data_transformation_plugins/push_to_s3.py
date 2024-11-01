import boto3
import os

import boto3
import os

def upload_files_to_s3(folder_path, bucket_name, s3_folder, exclude_files):
    """
    Uploads all files in a folder to a specified S3 folder only if the file does not already exist in S3,
    and excluding specified files.

    Parameters:
    - folder_path (str): Path to the local folder containing files to upload.
    - bucket_name (str): Name of the S3 bucket.
    - s3_folder (str): Destination folder path in the S3 bucket.
    - exclude_files (list): List of files to exclude from uploading.
    """
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Loop through files in the local folder
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        
        # Check if it's a file and not in the exclude list
        if os.path.isfile(file_path) and file_name not in exclude_files:
            s3_key = os.path.join(s3_folder, file_name)
            
            try:
                # Check if the file already exists in S3
                s3.head_object(Bucket=bucket_name, Key=s3_key)
                print(f"Skipped {file_name} (already exists in S3)")
            except s3.exceptions.ClientError as e:
                # If the file does not exist, upload it
                if e.response['Error']['Code'] == '404':
                    try:
                        s3.upload_file(file_path, bucket_name, s3_key)
                        print(f"Uploaded {file_name} to {s3_key}")
                    except Exception as upload_error:
                        print(f"Error uploading {file_name}: {upload_error}")
                else:
                    print(f"Error checking existence of {file_name}: {e}")

# Example usage:
# upload_folder_to_s3("path/to/local/folder", "my-s3-bucket", "my/s3/folder", ["exclude1.ext", "exclude2.ext"])
if __name__ == "__main__":
    upload_files_to_s3("data_transformation_plugins", "ghgc-data-store-develop", "data_transformation_plugins", ["__init__.py", "push_to_s3.py", "README.md", "sample_transformation.ipynb"])