from plpipes.config import cfg

import openai
from openai import *
openai.api_key = cfg["cloud.openai.auth.api_key"]
