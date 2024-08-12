import requests
import json
import time
import random


class DiarizeUtils:
    def request_call_remaining(url, headers, try_number):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DiarizeUtils

        Function: request

        Description: This is an internal diarize function which makes a GET API call to brooklyn.health API server

        Parameters:
        ----------
        url: The URL of the API endpoint.
        headers: The headers to be sent in the request.
        try_number: The number of times the function has been tried.

        Returns:
        ----------
        json: The JSON response from the API server.
        ------------------------------------------------------------------------------------------------------
        """
        try:
            response = requests.get(url, headers=headers)
            res_json = response.json()
        except (
            requests.exceptions.ConnectionError,
            json.decoder.JSONDecodeError,
        ) as ex:
            if try_number == 3:
                raise
            time.sleep(random.random() * 2)
            return DiarizeUtils.request(url, headers, try_number=try_number + 1)
        else:
            return res_json
