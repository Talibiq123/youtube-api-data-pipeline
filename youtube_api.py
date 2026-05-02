import os
import json
import config
import helper
import requests

class youtube_v3_api:
    def __init__(self):
        self.base_url = config.base_url
        self.key = config.api_key

    def get_data(self, endpoint: str, params: dict):
        params["key"] = self.key

        url = self.base_url + endpoint
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()

            destination_path = helper.generate_destination_path(endpoint)

            with open(destination_path, "w") as file:
                json.dump(data, file, indent=4)

            return data  # ✅ return only if valid

        else:
            print(f"API Error: {response.status_code}")
            return None  # ✅ explicit
