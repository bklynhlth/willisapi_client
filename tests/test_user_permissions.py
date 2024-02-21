from unittest.mock import patch
from willisapi_client.services.auth.permissions_manager import user_permissions

import sys
import logging

logger = logging.getLogger()
logger.level = logging.INFO
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)


class TestUserPermissions:
    def setup_method(self):
        self.username = "dummy"
        self.password = "password"
        self.id_token = "dummy_token"
        self.user_email = "dummy@gmail.com"
        self.client_name = "dummy_group"
        self.expires_in = 100
        self.failed_message = "Something went wrong"
        self.invalid_role = "Invalid role"

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.user_permissions")
    def test_user_permissions_failed(self, mocked_user_permissions):
        mocked_user_permissions.return_value = {
            "status_code": 500,
            "message": self.failed_message,
        }
        message = user_permissions(
            self.id_token, self.user_email, self.client_name, "rw"
        )
        assert message == self.failed_message

    def test_user_permissions_invalidrole(self):
        message = user_permissions(
            self.id_token, self.user_email, self.client_name, "rww"
        )
        assert message == self.invalid_role

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.user_permissions")
    def test_user_permissions_unauthoristed(self, mocked_user_permissions):
        mocked_user_permissions.return_value = {
            "status_code": 401,
            "message": "not an admin",
        }
        message = user_permissions(
            self.id_token, self.user_email, self.client_name, "rw"
        )
        assert message == "not an admin"

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.user_permissions")
    def test_user_permissions_success(self, mocked_user_permissions):
        mocked_user_permissions.return_value = {
            "status_code": 200,
            "message": "success",
        }
        message = user_permissions(
            self.id_token, self.user_email, self.client_name, "rw"
        )
        assert message == "success"

    @patch("willisapi_client.services.auth.login_manager.AuthUtils.user_permissions")
    def test_user_permissions_invalid_group(self, mocked_user_permissions):
        mocked_user_permissions.return_value = {
            "status_code": 404,
            "message": "Account not found",
        }
        message = user_permissions(
            self.id_token, self.user_email, self.client_name, "rw"
        )
        assert message == "Account not found"
