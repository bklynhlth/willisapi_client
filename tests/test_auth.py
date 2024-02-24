from willisapi_client.services.auth.login_manager import login

from willisapi_client.services.auth.account_manager import account_create
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
        self.account = "Group1"
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

    @patch("willisapi_client.services.auth.account_manager.AuthUtils.account_create")
    def test_create_account_success(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 200,
            "message": "Account Created Successfully",
        }
        result = account_create(
            self.id_token,
            self.account,
        )
        assert result == "Account Created Successfully"

    @patch("willisapi_client.services.auth.account_manager.AuthUtils.account_create")
    def test_create_account_failed(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 400,
            "message": "Validation Error",
        }
        result = account_create(
            self.id_token,
            self.account,
        )
        assert result == "Validation Error"

    @patch("willisapi_client.services.auth.account_manager.AuthUtils.account_create")
    def test_create_account_failed_for_non_admin_user(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 401,
            "message": "Not an Admin",
        }
        result = account_create(
            self.id_token,
            self.account,
        )
        assert result == "Not an Admin"

    @patch("willisapi_client.services.auth.account_manager.AuthUtils.account_create")
    def test_create_account_failed_unexpected(self, mocked_create_account):
        mocked_create_account.return_value = {
            "status_code": 500,
            "message": None,
        }
        result = account_create(
            self.id_token,
            self.account,
        )
        assert result == None

    @patch("willisapi_client.services.auth.account_manager.AuthUtils.account_create")
    def test_create_account_failed_no_response_from_server(self, mocked_create_account):
        mocked_create_account.return_value = {}
        result = account_create(
            self.id_token,
            self.account,
        )
        assert result == None
