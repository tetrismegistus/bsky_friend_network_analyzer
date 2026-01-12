import os
from atproto import Client

handle = os.environ["BSKY_HANDLE"]
app_password = os.environ["BSKY_APP_PASSWORD"]

client = Client()
client.login(handle, app_password)

