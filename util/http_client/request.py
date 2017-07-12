import requests
import json


def post(url, json_data):
    response = requests.post(url, json=json_data)

    # TODO: check for error and raise exception
    if response.status_code != 200:
        print ("ERROR %d: %s") % (response.status_code, response.reason)

    json_response = response.json()
    # print json.dumps(json_response, indent=4)
    return json_response
