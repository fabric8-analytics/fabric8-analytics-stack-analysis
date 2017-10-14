import requests
import json


def post(url, json_data):
    response = requests.post(url, json=json_data)

    # TODO: check for error and raise exception
    if response.status_code != 200:
        print("ERROR {s}: {r}".format(s=response.status_code, r=response.reason))

    json_response = response.json()
    return json_response
