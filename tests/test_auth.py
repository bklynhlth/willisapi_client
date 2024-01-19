from willisapi_client.services.auth.login_manager import login
from willisapi_client.services.auth.user_group_manager import create_account
from unittest.mock import patch
from datetime import timedelta, datetime


class TestLoginFunction:
    def setup_method(self):
        self.dt = datetime.now()
        self.username = "dummy"
        self.password = "password"
        self.id_token = "dummy_token"
        self.client_email = "dummy@gmail.com"
        self.client_name = "dummy_group"
        self.expires_in = 100
        self.expires_in_date = str(
            self.dt.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(seconds=self.expires_in)
        )

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.login")
    def test_login_failed(self, mocked_login):
        mocked_login.return_value = {}
        key, expire_in = login("", "")
        assert key == None
        assert expire_in == None

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.login")
    def test_login_success(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 200,
            "result": {"id_token": self.id_token, "expires_in": self.expires_in},
        }

        key, expire_in = login(self.username, self.password)
        expire_in = str(self.dt.replace(hour=0, minute=1, second=40, microsecond=0))
        assert key == self.id_token
        assert expire_in == self.expires_in_date

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.login")
    def test_login_400_status(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 400,
            "result": {"id_token": self.id_token, "expires_in": self.expires_in},
        }
        key, expire_in = login(self.username, self.password)
        assert key == None
        assert expire_in == None

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.login")
    def test_login_403_status(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 403,
            "result": {"id_token": self.id_token, "expires_in": self.expires_in},
        }
        key, expire_in = login(self.username, self.password)
        assert key == None
        assert expire_in == None

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.login")
    def test_login_500_status(self, mocked_login):
        mocked_login.return_value = {
            "status_code": 500,
            "result": {"id_token": self.id_token, "expires_in": self.expires_in},
        }
        key, expire_in = login(self.username, self.password)
        assert key == None
        assert expire_in == None

    @patch("willisapi_client.services.auth.user_group_manager.AuthUtils.create_account")
    def test_create_account_success(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 200,
            "message": "User already present in the group",
        }
        result = create_account(
            self.id_token,
            self.client_email,
            self.client_name,
        )
        print(result)
        assert result == "User already present in the group"

    @patch("willisapi_client.services.auth.user_group_manager.AuthUtils.create_account")
    def test_create_account_failed(self, mocked_create_account):
        mocked_create_account.return_value = {
            "message": "Login failed",
        }
        result = create_account(
            self.id_token,
            self.client_email,
            self.client_name,
        )
        print(result)
        assert result == None

    @patch("willisapi_client.services.auth.user_group_manager.AuthUtils.create_account")
    def test_create_account_failed_400_status(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 400,
            "message": "Python error",
        }
        result = create_account(
            self.id_token,
            self.client_email,
            self.client_name,
        )
        print(result)
        assert result == "Python error"

    @patch("willisapi_client.services.auth.user_group_manager.AuthUtils.create_account")
    def test_create_account_failed_401_status(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 401,
            "message": "Not an Admin User",
        }
        result = create_account(
            self.id_token,
            self.client_email,
            self.client_name,
        )
        print(result)
        assert result == "Not an Admin User"

    @patch("willisapi_client.services.auth.user_group_manager.AuthUtils.create_account")
    def test_create_account_failed_409_status(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 409,
            "message": "User already exists or already in some group",
        }
        result = create_account(
            self.id_token,
            self.client_email,
            self.client_name,
        )
        print(result)
        assert result == "User already exists or already in some group"

    @patch("willisapi_client.services.auth.user_group_manager.AuthUtils.create_account")
    def test_create_account_failed_404_status(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 404,
            "message": "User not found",
        }
        result = create_account(
            self.id_token,
            self.client_email,
            self.client_name,
        )
        print(result)
        assert result == "User not found"
