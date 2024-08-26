from http import HTTPStatus

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

    logger.info("Passing through WillisDiarize...")
    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_diarize()
    headers = wc.get_headers()
    headers["Authorization"] = key
    corrected_transcript = None

    if not DiarizeUtils.is_valid_file_path(file_path):
        logger.info("Input file type is incorrect. We only accept JSON files.")
        logger.info("Failed!")
        return corrected_transcript

    data = DiarizeUtils.read_json_file(file_path)

    if not data:
        return corrected_transcript

    response = DiarizeUtils.request_diarize(url, data, headers, try_number=1)
    if response["status_code"] != HTTPStatus.OK:
        logger.info(response["message"])
        logger.info("Failed!")
    else:
        logger.info("Returning processed JSON...")
        encoded_response = response["data"]
        corrected_transcript = DiarizeUtils.decode_response(encoded_response)
        logger.info("Done!")
    return corrected_transcript
