from http import HTTPStatus

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.diarize.diarize_utils import DiarizeUtils


def willis_diarize_call_remaining(key: str, **kwargs):
    """
    ---------------------------------------------------------------------------------------------------
    Function: willis_diarize_call_remaining

    Description: This function returns the number of remaining calls for willisdiarize

    Parameters:
    ----------
    key: AWS access id token (str)
    ---------------------------------------------------------------------------------------------------
    """

    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_diarize_remaining_calls_url()
    headers = wc.get_headers()
    headers["Authorization"] = key

    response = DiarizeUtils.request_call_remaining(url, headers, try_number=1)
    if response:
        logger.info(response["message"])
