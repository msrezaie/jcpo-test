import requests
import json
import base64

"""
Method for getting the auth string needed to access the Duke Power API
"""


def get_auth():
    # For config data and auth key & secret
    config_url = "https://outagemap.duke-energy.com/config/config.prod.json"
    random_url = "https://outagemap.duke-energy.com/"

    r = requests.get(config_url)
    # configjson = json.loads(r.content)
    # # So the auth string we need for the countyurl is a base64 concatentation of two JSON values separated by a colon,
    # # prefixed by "Basic "
    # # For this, Thomas Wilburn is owed many frosty beverages

    # authstring = bytes("Basic ", "utf-8") + base64.b64encode(
    #     bytes(
    #         f"{configjson['consumer_key_emp']}:{configjson['consumer_secret_emp']}",
    #         "utf-8",
    #     )
    # )

    # cookies = r.cookies

    # headers = {
    #     "Accept": "application/json, text/plain, */*",
    #     "Origin": "https://outagemap.duke-energy.com",
    #     "Referer": "https://outagemap.duke-energy.com/",
    #     "Sec-Fetch-Mode": "cors",
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
    # }

    # headers["Authorization"] = authstring

    return "returned:", r.status_code
