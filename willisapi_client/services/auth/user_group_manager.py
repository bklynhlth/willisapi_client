# website:   https://www.brooklyn.health
from http import HTTPStatus

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.auth.auth_utils import AuthUtils
from willisapi_client.logging_setup import logger as logger

from datetime import datetime


def create_account(
    key: str,
    client_email: str,
    client_name: str,
    **kwargs,
) -> str:
    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_create_account_url()
    headers = wc.get_headers()
    headers["Authorization"] = key
    data = dict(
        client_email=client_email,
        client_name=client_name,
    )
    logger.info(f'{datetime.now().strftime("%H:%M:%S")}: Create account')
    response = AuthUtils.create_account(url, data, headers, try_number=1)
    if response and "status_code" in response:
        if response["status_code"] == HTTPStatus.BAD_REQUEST:
            # Invalid argument
            logger.error("Python error")
        if response["status_code"] == HTTPStatus.UNAUTHORIZED:
            logger.error("Not an Admin User")
        if response["status_code"] == HTTPStatus.CONFLICT:
            logger.error("User already exists or already in some group")
        if response["status_code"] == HTTPStatus.NOT_FOUND:
            # No user in cognito
            logger.error("User not found")
        if response["status_code"] == HTTPStatus.OK:
            logger.info(
                f'{datetime.now().strftime("%H:%M:%S")}: User added to the group successfully'
            )
            return response["message"]
    else:
        logger.error(f"Login Failed")
        return None