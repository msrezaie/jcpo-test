from flask import Flask, jsonify
import json
import requests

app = Flask(__name__)


@app.route('/')
def home():
    config_url = "https://outagemap.duke-energy.com/config/config.prod.json"

    r = requests.get(config_url)
    configjson = json.loads(r.content)
    return jsonify(configjson)


if __name__ == '__main__':
    app.run(debug=True)
