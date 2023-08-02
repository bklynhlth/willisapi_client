import setuptools
import math

def get_client_version():
    try:
        cv = float(setuptools.version.metadata.version('willisapi_client'))
    except Exception as e:
        cv = 1.0
    return cv


class WillisapiClient():
    def __init__(self, *args, **kwargs) -> None:
        self.client_version = get_client_version()
        self.api_version = math.floor(self.client_version)
        self.api_uri = "api.brooklyn.health"
        self.env = kwargs['env'] if 'env' in kwargs else None
    
    def get_login_url(self):
        if self.env:
            return f"https://{self.env}-{self.api_uri}/v{self.api_version}/login"
        return f"https://{self.api_uri}/v{self.api_version}/login"
    
    def get_login_headers(self):
        return {'Content-Type': 'application/json'}
