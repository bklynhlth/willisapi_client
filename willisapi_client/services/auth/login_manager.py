# website:   https://www.brooklyn.health
from typing import Tuple
from http import HTTPStatus

# import datetime
from datetime import datetime, timedelta

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.auth.auth_utils import AuthUtils
from willisapi_client.logging_setup import logger as logger


def login(username: str, password: str, **kwargs) -> Tuple[str, int]:
    """
    ---------------------------------------------------------------------------------------------------
    Function: login

    Description: This is the login function to access willisAPI login API

    Parameters:
    ----------
    username: string representation of email id
    password: string representation of password

    Returns:
    ----------
    key : AWS access key token (str/None)
    expiration: AWS token expiration time (int/None)

    ---------------------------------------------------------------------------------------------------
    """
    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_login_url()
    headers = wc.get_headers()
    data = dict(username=username, password=password)
    response = AuthUtils.login(url, data, headers, try_number=1)
    if (
        response
        and "status_code" in response
        and response["status_code"] == HTTPStatus.OK
    ):
        logger.info("Login Successful; Key acquired")
        logger.info(
            f"Key expiration: {datetime.now() + timedelta(seconds=response['result']['expires_in'])}"
        )
        required_format = (
            f"{datetime.now() + timedelta(seconds=response['result']['expires_in'])}"
        )
        return (response["result"]["id_token"], required_format)
    else:
        logger.error(f"Login Failed")
        return (None, None)
