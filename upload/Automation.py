import boto3
import os
import botocore
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from common.constants import S3_CLIENT
from common.configManager import ConfigManager
from utils.utils import get_root_paths
from utils.logging import setup_logger

setup_logger()
config = ConfigManager()

#Upload Marker file
def upload_marker_file():
    S3_CLIENT.put_object(Bucket=config.get('bucket_name'), Key="DONE.txt", Body=b'Upload complete')

# Uploads files into the S3 Bucket 
def upload_files_to_s3_bucket(file_path, bucket_name, s3_key):
    try:
        S3_CLIENT.head_object(Bucket=bucket_name, Key=s3_key)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404': 
            try:
                S3_CLIENT.upload_file(file_path, bucket_name, s3_key)
            except botocore.exceptions.ClientError as upload_error:
                logging.error(f"Error uploading {file_path}: {e}")
        else:
            logging.error(f"Error checking {s3_key}: {e}")

# Gets all npy files and places them into a list of tuples
def get_npy_files():
    start_time = time.time()
    files_to_be_uploaded = []
    root_path = config.get('root_path')
    bucket_name = config.get('bucket_name')

    for patagonia_path in get_root_paths():
        directory = os.path.basename(patagonia_path)
        boundingbox_path = os.path.join(patagonia_path, bounding_box_folder_name)
        radar_path = os.path.join(patagonia_path, radar_data_folder_name)
        for dirNames2 in os.listdir(patagonia_path):
            if dirNames2 == bounding_box_folder_name:
                for dirpath, _, files in os.walk(boundingbox_path):
                    for filename in files:
                        if filename.endswith('.npy'):
                            full_path = os.path.join(dirpath, filename)
                            files_to_be_uploaded.append((full_path, bucket_name, f"{bounding_box_bucket_name}/{directory}/{filename}"))
            if dirNames2 == radar_data_folder_name:
                for dirpath, _, files in os.walk(radar_path):
                    for filename in files:
                        if filename.endswith('.npy'):
                            full_path = os.path.join(dirpath, filename)
                            files_to_be_uploaded.append((full_path, bucket_name, f"{radar_data_bucket_name}/{directory}/{filename}"))

    # Using multithreading to speed up upload speed. 
    with ThreadPoolExecutor(config.get("thread_pool_size")) as executor:
        futures = [executor.submit(upload_files_to_s3_bucket, *task) for task in files_to_be_uploaded]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                 logging.error(f"Upload Failed : {e}")

    # Uploading marker file, which eventbridge will look for to know upload is all done
    upload_marker_file()
    
    duration = time.time() - start_time 
    # Logging the time it took to finish uploading
    logging.info(f"Finished in {duration} seconds.")
get_npy_files()
