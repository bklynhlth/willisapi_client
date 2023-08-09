# website:   https://www.brooklyn.health
import pandas as pd

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.download.download_utils import DownloadUtils

def download(key, project_name = 'project_name'):
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
    summary : pandas Dataframe
        download summary

    ---------------------------------------------------------------------------------------------------
    """

    wc = WillisapiClient()
    url = wc.get_download_url()
    headers = wc.get_headers()
    headers['Authorization'] = key
    data = dict(project_name=project_name)
    response = DownloadUtils.request(url, data, headers, try_number=1)
    return response
