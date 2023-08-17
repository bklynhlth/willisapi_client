# website:   https://www.brooklyn.health
from http import HTTPStatus

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.download.download_utils import DownloadUtils
from willisapi_client.logging_setup import logger as logger
from willisapi_client.timer import measure

@measure
def download(key: str, project_name: str):
    """
    ---------------------------------------------------------------------------------------------------

    This function to download data using willis download API from secure database

    Parameters:
    ............
    key: str
        Temporary access token
    project_name: str
        name of the project

    Returns:
    ............
    summary : json
        download summary

    ---------------------------------------------------------------------------------------------------
    """

    wc = WillisapiClient()
    url = wc.get_download_url() + f"?project_name={project_name}"
    headers = wc.get_headers()
    headers['Authorization'] = key
    response = DownloadUtils.request(url, headers, try_number=1)
    if "status_code" in response:
        if response["status_code"] == HTTPStatus.FORBIDDEN:
            logger.error("Invalid key")
        if response["status_code"] == HTTPStatus.UNAUTHORIZED:
            logger.error("No access to project/data for download.")
        if response["status_code"] == HTTPStatus.OK:
            response_df, err = DownloadUtils.generate_response_df(response)
            if err:
                return response
            return response_df
    else:
        return {}
