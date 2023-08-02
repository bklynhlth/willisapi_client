# website:   https://www.brooklyn.health
import pandas as pd

def create_user(key, username, group):
    """
    ---------------------------------------------------------------------------------------------------

    This is the signup function to access willisAPI signup API

    Parameters:
    ............
    key: str
        Temporary access token
    client_email: str
        expected onboarded userid
    client_name: str
        expected group name without empty spaces

    Returns:
    ............
    status : str
        Onboard succes/fail

    ---------------------------------------------------------------------------------------------------
    """

    # SIGNUP_URL = f"{BASE_URL}/signup"
    # headers = {
    #     'Authorization': id_token,
    #     'Content-Type': 'application/json'
    # }
    # data = {
    #     "client_email": client_email,
    #     "client_name": client_name
    # }
    # data = json.dumps(data)
    # res = requests.post(SIGNUP_URL, json=data, headers=headers).json()
    c = 1 + 1
    d = 1 + 2
    return c, d
