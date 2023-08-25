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
        self.app_url = f"app.brooklyn.health/api/v{self.api_version}/"
        self.env = kwargs['env'] if 'env' in kwargs else None
        # uncomment self.env = 'dev' while development
        # self.env = 'dev' 
    
    def get_base_url(self):
        if self.env:
            return f"https://{self.env}-{self.api_uri}/v{self.api_version}/"
        return f"https://{self.api_uri}/v{self.api_version}/"
    
    def get_login_url(self):
        return self.get_base_url() + "login"
    
    def get_signup_url(self):
        return self.get_base_url() + "signup"
    
    def get_upload_url(self):
        return self.get_base_url() + "upload"
    
    def get_download_url(self):
        if self.env:
            return f"https://{self.env}-{self.app_url}download"
        return f"https://{self.app_url}download"
    
    def get_headers(self):
        return {'Content-Type': 'application/json', 'Accept': 'application/json'}
