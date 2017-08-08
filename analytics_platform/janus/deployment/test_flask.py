import time
import requests

time.sleep(7)
while True:
    res = requests.get('http://0.0.0.0:8080/')
    if res.json().get('status') == 'ok':
        break
    time.sleep(3)