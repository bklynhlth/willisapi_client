from http import HTTPStatus

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.auth.auth_utils import AuthUtils
from willisapi_client.logging_setup import logger as logger


def user_permissions(
    key: str, user_email: str, account: str, role: str, **kwargs
) -> str:
    """
    ---------------------------------------------------------------------------------------------------
    Function: user_permissions

    Description: This function is responsible for making a call to the /user_permissions API for access control

    Parameters:
    ----------
    key: string representation of id_token generate after login
    user_email: string representation of user's email
    account: string representation of account/client name
    role: r/rw/*

    Returns:
    ----------
    None

    ---------------------------------------------------------------------------------------------------
    """
    wc = WillisapiClient(env=kwargs.get("env"))
    url = wc.get_user_permissions_url()
    headers = wc.get_headers()
    headers["Authorization"] = key
    data = dict(user_email=user_email, account=account, role=role)
    is_role_valid = AuthUtils.validate_role(role)
    if is_role_valid:
        response = AuthUtils.user_permissions(url, data, headers, try_number=1)
        message = response["message"]
        logger.info(message)
    else:
        message = "Invalid role"
        logger.info(message)

    return message
