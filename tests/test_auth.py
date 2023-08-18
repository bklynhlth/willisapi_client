from willisapi_client.services.auth.login_manager import login
from willisapi_client.services.auth.user_manager import create_user

from unittest.mock import patch

class TestLoginFunction:
    def setup_method(self):
        self.username = "dummy"
        self.password = "password"
        self.id_token = "dummy_token"
        self.expires_in = 100

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.login')
    def test_login_failed(self, mocked_login):
        mocked_login.return_value = {}
        key, expire_in = login("", "")
        assert key == None
        assert expire_in == None

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.login')
    def test_login_success(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 200,
            "result":{
                "id_token": self.id_token,
                "expires_in": self.expires_in
            }
        }
        key, expire_in = login(self.username, self.password)
        assert key == self.id_token
        assert expire_in == self.expires_in

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.login')
    def test_login_400_status(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 400,
            "result":{
                "id_token": self.id_token,
                "expires_in": self.expires_in
            }
        }
        key, expire_in = login(self.username, self.password)
        assert key == None
        assert expire_in == None

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.login')
    def test_login_403_status(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 403,
            "result":{
                "id_token": self.id_token,
                "expires_in": self.expires_in
            }
        }
        key, expire_in = login(self.username, self.password)
        assert key == None
        assert expire_in == None

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.login')
    def test_login_500_status(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 500,
            "result":{
                "id_token": self.id_token,
                "expires_in": self.expires_in
            }
        }
        key, expire_in = login(self.username, self.password)
        assert key == None
        assert expire_in == None


class TestSignupFunction:
    def setup_method(self):
        self.username = "dummy"
        self.password = "password"
        self.admin_key = "admin_key"
        self.non_admin_key = "non_admin_key"
        self.client_email = "client@email.com"
        self.client_name = "client"

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.signup')
    def test_signup_failed(self, mocked_signup):
        mocked_signup.return_value = {
            "status_code": 200,
            "message": "Not an admin user"
            }
        message = create_user(self.non_admin_key, self.client_email, self.client_name)
        assert message == "Not an admin user"

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.signup')
    def test_signup_success(self, mocked_signup):
        mocked_signup.return_value = {
            "status_code": 200,
            "message": "User created"
            }
        message = create_user(self.admin_key, self.client_email, self.client_name)
        assert message == "User created"

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.signup')
    def test_signup_failed_400_status(self, mocked_signup):
        mocked_signup.return_value = {
            "status_code": 400,
            }
        message = create_user(self.admin_key, self.client_email, self.client_name)
        assert message == None

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.signup')
    def test_signup_failed_500_status(self, mocked_signup):
        mocked_signup.return_value = {
            "status_code": 500,
            }
        message = create_user(self.admin_key, self.client_email, self.client_name)
        assert message == None

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.signup')
    def test_signup_failed_401_status(self, mocked_signup):
        mocked_signup.return_value = {
            "status_code": 401,
            }
        message = create_user(self.admin_key, self.client_email, self.client_name)
        assert message == None

    @patch('willisapi_client.services.auth.login_manager.AuthUtils.signup')
    def test_signup_failed_403_status(self, mocked_signup):
        mocked_signup.return_value = {
            "status_code": 403,
            }
        message = create_user(self.admin_key, self.client_email, self.client_name)
        assert message == None