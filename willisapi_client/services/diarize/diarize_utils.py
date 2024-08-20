import requests
import json
import time
import random
import os
import gzip
import base64
from willisapi_client.logging_setup import logger as logger


class DiarizeUtils:
    def is_valid_file_path(file_path: str):
        return file_path.endswith(".json") and os.path.exists(file_path)

    def read_json_file(file_path: str):
        data = None
        try:
            with open(file_path) as f:
                json_data = json.load(f)
            data = dict(json_data=json_data)
        except json.decoder.JSONDecodeError:
            logger.info("No data found in the file or JSON is invalid.")
            logger.info("Failed!")
        return data

    def decode_response(encoded_response):
        return json.loads(gzip.decompress(base64.b64decode(encoded_response)))

    def request_diarize(url, data, headers, try_number):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DiarizeUtils

        Function: request_diarize

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
            response = requests.post(url, json=data, headers=headers)
            res_json = response.json()
        except (
            requests.exceptions.ConnectionError,
            json.decoder.JSONDecodeError,
        ) as ex:
            if try_number == 3:
                raise
            time.sleep(random.random() * 2)
            return DiarizeUtils.request_diarize(
                url, data, headers, try_number=try_number + 1
            )
        else:
            return res_json

    def request_call_remaining(url, headers, try_number):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DiarizeUtils

        Function: request_call_remaining

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
            return DiarizeUtils.request_call_remaining(
                url, headers, try_number=try_number + 1
            )
        else:
            return res_json
