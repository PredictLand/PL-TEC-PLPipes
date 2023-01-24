import urllib
import requests
import json
from getpass import getpass
import os
import sys
from pprint import pprint
from http.server import HTTPServer, BaseHTTPRequestHandler
from plpipes.config import ConfigStack
import subprocess
import pathlib
import logging
import time
from datetime import datetime

client_secret="Hpe8Q~G2rfEMMNxFGH_XVJ0Pbupn4nISroYuqclD"

client_id = "fd413368-305f-41e4-9039-4a76126ea57f"
permissions = ['offline_access', 'files.readwrite', 'User.Read', 'Directory.Read.All', 'Sites.Read.All']
response_type = 'code'
cb_port = 8283
cb_URL = f'http://localhost:{cb_port}/'
scope = "+".join(permissions)
token = None
authorize_URL = 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize'
token_URL = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
graph_URL = 'https://graph.microsoft.com/v1.0/'

auth_cache_fn = "~/.config/plpipes/auth-cache.json"

cache = ConfigStack().root()
azure = cache.cd("azure.foo")

try:
    logging.info("Refreshing authentication token")
    cache.merge_file(auth_cache_fn)

    refresh_token = azure["refresh_token"]
    response = requests.post(token_URL,
                             data = {"client_id": client_id,
                                     "scope": " ".join(permissions),
                                     "refresh_token": refresh_token,
                                     "redirect_uri": cb_URL,
                                     "grant_type": 'refresh_token',
                                     "client_secret": client_secret})
    r = json.loads(response.text)
    token = r["access_token"]
    refresh_token = r["refresh_token"]

except Exception as ex:
    logging.warn("Unable to refresh token: ", ex)
    logging.info("Authenticating")
    code = None

    class OneShotHandler(BaseHTTPRequestHandler):
        def log_message(*args, **kwargs):
            pass

        def do_GET(self):
            global code
            try:
                p = urllib.parse.urlparse(self.path)
                q = urllib.parse.parse_qs(p.query)
                code = q["code"][0]
            except Exception as ex:
                self.send_response(400)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                s = "Something went wrong, unable to retrieve authentication code!"
                self.wfile.write(bytes(s, "utf-8"))
                logging.error(s)
            else:
                logging.debug("Got an authentication code back")
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Authentication code retrieved successfully.\nYou can close this page now and go back to your console application!")

    #auth_url = f"{URL}?client_id={client_id}&scope={scope}&response_type=code&redirect_uri={urllib.parse.quote(redirect_uri)}"

    s = HTTPServer(("localhost", cb_port), OneShotHandler)

    url = (authorize_URL + "?" +
           urllib.parse.urlencode({'client_id': client_id,
                                   'scope': " ".join(permissions),
                                   'response_type': 'code',
                                   'redirect_uri': cb_URL}))
    subprocess.run(["xdg-open", url])
    logging.info("Waiting for browser...")
    s.handle_request()
    #s.shutdown()

    if code is None:
        raise Exception("Authentication failed")

    else:
        print(f"code set to {code}")

    response = requests.post(token_URL,
                             data = {
                                 "client_id": client_id,
                                 "scope": " ".join(permissions),
                                 "code": code,
                                 "redirect_uri": cb_URL,
                                 "grant_type": 'authorization_code',
                                 "client_secret": "Hpe8Q~G2rfEMMNxFGH_XVJ0Pbupn4nISroYuqclD"
                             })
    r = json.loads(response.text)
    token = r["access_token"]
    refresh_token = r["refresh_token"]

logging.info("Storing new authentication information in cache")

azure["refresh_token"] = refresh_token
azure["last_updated"] = time.mktime(datetime.now().timetuple())

path = pathlib.Path(auth_cache_fn)
path.parent.mkdir(exist_ok=True, parents=True)
pprint(cache.to_tree())

with open(path, "w") as f:
    json.dump(cache.to_tree(), f)



headers = {'Authorization': f'Bearer {token}'}

response = requests.get(graph_URL + 'me/drive/',
                        headers = headers)

if (response.status_code == 200):
    r = json.loads(response.text)
    print('Connected to the OneDrive of', r['owner']['user']['displayName']+' (',r['driveType']+' ).', \
         '\nConnection valid for one hour. Refresh token if required.')

    # print(f"response:\n{response.text}")
elif (response.status_code == 401):
    response = json.loads(response.text)
    print('API Error! : ', response['error']['code'],\
         '\nSee response for more details.')
else:
    response = json.loads(response.text)
    print('Unknown error! See response for more details.')

#items = json.loads(requests.get(graph_URL + 'me/drive/root/children', headers=headers).text)
#pprint(items)
#items = items['value']
#for entries in range(len(items)):
#    print(items[entries]['name'], '| item-id >', items[entries]['id'])


response = requests.get(graph_URL + "groups",
                        headers = headers)

#print(f"groups {response.status_code}")
#pprint(json.loads(response.text))

for v in json.loads(response.text)["value"]:
    dn = v['displayName']
    if dn == "PredictLand":


        response = requests.get(graph_URL + f"groups/{v['id']}/drive",
                                headers = headers)

        print(f"group files {response.status_code}")
        pprint(json.loads(response.text))



response = requests.get(graph_URL + "sites/predictland.sharepoint.com",
                        headers=headers)

print(f"site {response.status_code}")
pprint(json.loads(response.text))

id = json.loads(response.text)["id"]

#response = requests.get(graph_URL + f"/sites/{id}/drive/root/delta",
#                        headers=headers)

response = requests.get(graph_URL + f"/sites/{id}/drive/items/root/Documents/children",
                        headers=headers)

print(f"site {response.status_code}")
pprint(json.loads(response.text))

