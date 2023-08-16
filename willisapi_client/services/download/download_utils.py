import requests
import json
import time
import random

class DownloadUtils:
    def request(url, data, headers, try_number):
        try:
            response = requests.get(url, json=data, headers=headers)
            res_json = response.json()
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as ex:
            if try_number == 3:
                raise 
            time.sleep(random.random()*2)
            return DownloadUtils.request(url, data, headers, try_number=try_number+1)
        else:
            return res_json