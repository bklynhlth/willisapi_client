from http import HTTPStatus
import json

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.diarize.diarize_utils import DiarizeUtils


def willis_diarize(key: str, file_path: str, **kwargs):
    """
    ---------------------------------------------------------------------------------------------------
    Function: willis_diarize

    Description: This function makes call to WillisDiarize Model

    Parameters:
    ----------
    key: AWS access id token (str)
    file_path: String

    Returns:
    ----------
    json: JSON
    ---------------------------------------------------------------------------------------------------
    """

    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_diarize()
    headers = wc.get_headers()
    headers["Authorization"] = key
    corrected_transcript = None

    if not DiarizeUtils.is_valid_file_path(file_path):
        logger.info("Incorrect file type")

    with open(file_path) as f:
        json_data = json.load(f)
    data = dict(json_data=json_data)

    response = DiarizeUtils.request_diarize(url, data, headers, try_number=1)
    if response["status_code"] != HTTPStatus.OK:
        logger.info(response["message"])
    else:
        corrected_transcript = response["data"]
    return corrected_transcript
