# website:   https://www.brooklyn.health
from http import HTTPStatus

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.auth.auth_utils import AuthUtils
from willisapi_client.logging_setup import logger as logger

def create_user(key: str, client_email: str , client_name: str) -> str:
    """
    ---------------------------------------------------------------------------------------------------
    Function: create_user
    
    Description: This is the signup function to access willisAPI signup API

    Parameters:
    ----------
    key: Admin access token
    client_email: string representation of client email id
    client_name: stringbrepresentation of expected client name without empty spaces

    Returns:
    ----------
    status : Onboard succes/fail message (str/None)
    ---------------------------------------------------------------------------------------------------
    """

    wc = WillisapiClient()
    url = wc.get_signup_url()
    headers = wc.get_headers()
    headers['Authorization'] = key
    data = dict(client_email=client_email, client_name=client_name)
    response = AuthUtils.signup(url, data, headers, try_number=1)
    if response and 'status_code' in response and response['status_code'] == HTTPStatus.OK:
        logger.info(f"Signup Successful for client: {client_name}, client_email: {client_email}")
        return response['message']
    else:
        logger.error(f"Signup Failed")
        return None
