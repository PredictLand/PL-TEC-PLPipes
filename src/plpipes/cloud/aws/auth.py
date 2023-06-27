
import os
import sys
from pathlib import Path
import boto3

sys.path.append(str(Path(os.getcwd()).joinpath("src")))


from plpipes.config import cfg

import pathlib
import logging

from plpipes.exceptions import AuthenticationError

_registry = {}
def credentials(account_name):
    if account_name not in _registry:
        _authenticate(account_name)
    return _registry[account_name]

def _authenticate(account_name):
    cfg_path = f"cloud.aws.auth.{account_name}"
    acfg = cfg.cd(cfg_path)

    # Read access and secret keys
    cred = boto3.Session(aws_access_key_id = acfg['ACCESS_KEY'],
                        aws_secret_acces_key = acfg['SECRET_KEY'],
                        region_name = acfg['REGION'])
    _registry[account_name] = cred
    return cred

