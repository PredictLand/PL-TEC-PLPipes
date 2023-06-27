import os
import sys
from pathlib import Path
import plpipes.cloud.aws.s3 as aws
import plpipes.cloud.azure.storage as az
import plpipes.cloud.gcp.gcs as gcp

sys.path.append(str(Path(os.getcwd()).joinpath("src")))

from plpipes.config import cfg

# Get the cloud service providers
user_file_systems = list(cfg['cloud'].keys())

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
        if fs == "azure":
            fs = az.fs(cfg['cloud']['azure']['auth'])
        elif fs == "aws":
            fs = aws.fs(cfg['cloud']['aws']['auth'])
        elif fs == "gcp":
            fs = gcp.fs(cfg['cloud']['gcp']['auth'])
        return fs
    else:
        raise ValueError("The file system you attempted to select is not available within your project")
    