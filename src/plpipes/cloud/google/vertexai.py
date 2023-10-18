from plpipes.config import cfg
import plpipes.cloud.google.auth

c = cfg.cd("cloud.google.vertexai")
creds = plpipes.cloud.google.auth.credentials(c["auth"])

import vertexai
vertexai.init(project=c["project"], location=c["location"], credentials=creds)

import sys
sys.modules[__name__] = vertexai
