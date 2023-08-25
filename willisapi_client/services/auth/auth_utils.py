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
        ------------------------------------------------------------------------------------------------------
        Class: AuthUtils

        Function: login

        Description: This function makes a POST API call to the brooklyn.health API server to authenticate a user.

        Parameters:
        ----------
        url: The URL of the API endpoint.
        data: The data to be sent in the request body.
        headers: The headers to be sent in the request.
        try_number: The number of times the function has been tried.
        
        Returns:
        ----------
        json: The JSON response from the API server.
        
        Raises:
        ----------
        UnableToLoginClientError: If the function fails to login after 3 tries.
        ------------------------------------------------------------------------------------------------------
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
        ------------------------------------------------------------------------------------------------------
        Class: AuthUtils

        Function: signup

        Description: This is an internal signup function which makes a POST API call to the brooklyn.health API server.

        Parameters:
        ----------
        url: The URL of the API endpoint.
        data: The data to be sent in the request body.
        headers: The headers to be sent in the request.
        try_number: The number of times the function has been tried.
        
        Returns:
        ----------
        json: The JSON response from the API server.
        
        Raises:
        ----------
        UnableToOnboardClientError: If the function fails to signup after 3 tries.
        ------------------------------------------------------------------------------------------------------
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
