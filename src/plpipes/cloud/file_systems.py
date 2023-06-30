import os
import json
import sys
from pathlib import Path
import plpipes.cloud.aws.s3 as aws
import plpipes.cloud.azure.storage as az
import plpipes.cloud.gcp.gcs as gcp

sys.path.append(str(Path(os.getcwd()).joinpath("src")))

from plpipes.config import cfg

# Get the cloud service providers
cfg_json = cfg.to_json()
cfg_dict = json.loads(cfg_json)
user_file_systems = list(cfg_dict['cloud'].keys())

def show_fss():
    print("Cloud providers you can be authenticated in within this project:")
    for fs in user_file_systems:
        if fs == "azure":
            print(f"{fs}: {fs.capitalize()}")
        else:
            print(f"{fs}: {fs.upper()}")
    print("Select one of them by using the \"select_fs()\" method (see --h --help for usage)")

def select_fs(fs):
    if fs in user_file_systems:
        if fs == "azure" or fs == "az" or fs == "Azure" or fs == "AZURE":
            keys_list = list(cfg_dict['cloud']['azure']['auth'].keys())
            fs = az.fs(keys_list[0])
        elif fs == "aws" or fs == "Amazon Web Services" or fs == "AWS":
            keys_list = list(cfg_dict['cloud']['aws']['auth'].keys())
            fs = aws.fs(keys_list[0])
        elif fs == "gcp" or fs == "Google Cloud Platform" or fs == "GCP":
            keys_list = list(cfg_dict['cloud']['gcp']['auth'].keys())
            fs = gcp.fs(keys_list[0])
        return fs
    else:
        raise ValueError("The file system you attempted to select is not available within your project")
    