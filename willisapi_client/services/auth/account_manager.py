# website:   https://www.brooklyn.health
from http import HTTPStatus

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.auth.auth_utils import AuthUtils
from willisapi_client.logging_setup import logger as logger

from datetime import datetime


def account_create(
    key: str,
    account: str,
    **kwargs,
) -> str:
    """
    ---------------------------------------------------------------------------------------------------
    Function: account_create

    Description: This function to creates account in RDS and AWS Cognito using willis account create API

    Parameters:
    ----------
    key: Admin access token
    account: Client Name

    Returns:
    ----------
    None
    ---------------------------------------------------------------------------------------------------
    """
    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_account_create_url()
    headers = wc.get_headers()
    headers["Authorization"] = key
    data = dict(
        account=account.lower(),
    )
    logger.info(f'{datetime.now().strftime("%H:%M:%S")}: Creating account')
    response = AuthUtils.account_create(url, data, headers, try_number=1)
    if response and "status_code" in response:
        if response["status_code"] == HTTPStatus.OK:
            logger.info(response["message"])
        else:
            logger.error(response["message"])
        return response["message"]
    else:
        logger.error(f"Failed")
        return None
