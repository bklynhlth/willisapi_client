# website:   https://www.brooklyn.health
from http import HTTPStatus
import pandas as pd

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.download.download_utils import DownloadUtils
from willisapi_client.logging_setup import logger as logger
from willisapi_client.timer import measure

@measure
def download(key: str, project_name: str):
    """
    ---------------------------------------------------------------------------------------------------
    Function: download

    Description: This function to download data using willis download API from secure database

    Parameters:
    ----------
    key: AWS access id token (str)
    project_name: name of the project (str)

    Returns:
    ----------
    measures: A pandas dataframe
    ---------------------------------------------------------------------------------------------------
    """

    wc = WillisapiClient()
    url = wc.get_download_url() + f"?project_name={project_name}"
    headers = wc.get_headers()
    headers['Authorization'] = key
    response = DownloadUtils.request(url, headers, try_number=1)
    empty_response_df = pd.DataFrame()
    if "status_code" in response:
        if response["status_code"] == HTTPStatus.FORBIDDEN:
            logger.error("Invalid key")
        if response["status_code"] == HTTPStatus.UNAUTHORIZED:
            logger.error("No access to project/data for download.")
        if response["status_code"] == HTTPStatus.OK:
            response_df, err = DownloadUtils.generate_response_df(response)
            if not err:
                return response_df
    return empty_response_df
