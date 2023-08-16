import requests
import time
import json
import random

from willisapi_client.services.exceptions import (
    UnableToLoginClientError,
    UnableToOnboardClientError
)

class AuthUtils:
    @staticmethod
    def login(url, data, headers, try_number):
        """
        This is an internal login function which makes a POST API call to brooklyn.health API server

        Returns:
            json
        """
        try:
            response = requests.post(url, json=data, headers=headers)
            res_json = response.json()
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as ex:
            if try_number == 3:
                raise UnableToLoginClientError
            time.sleep(random.random()*2)
            return AuthUtils.login(url, data, headers, try_number=try_number+1)
        else:
            return res_json

    @staticmethod
    def signup(url, data, headers, try_number):
        """
        This is an internal signup function which makes a POST API call to brooklyn.health API server

        Returns:
            json
        """
        try:
            response = requests.post(url, json=data, headers=headers)
            res_json = response.json()
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as ex:
            if try_number == 3:
                raise UnableToOnboardClientError
            time.sleep(random.random()*2)
            return AuthUtils.signup(url, data, headers, try_number=try_number+1)
        else:
            return res_json
