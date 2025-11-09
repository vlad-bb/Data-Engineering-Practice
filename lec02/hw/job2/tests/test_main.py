"""
Tests for main.py
"""

from unittest import TestCase, mock

from .. import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch("job2.main.load_to_stage")
    def test_return_400_raw_dir_param_missed(self, load_to_stage_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'raw_dir' param
        """
        resp = self.client.post(
            "/",
            json={
                "stg_dir": "/foo/bar/",
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid raw_dir or stg_dir parameter", resp.json["message"])

    @mock.patch("job2.main.load_to_stage")
    def test_return_400_stg_dir_param_missed(self, load_to_stage_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'stg_dir' param
        """
        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                # no 'stg_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid raw_dir or stg_dir parameter", resp.json["message"])

    @mock.patch("job2.main.load_to_stage")
    def test_return_400_both_params_missed(self, load_to_stage_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when both params are missing
        """
        resp = self.client.post(
            "/",
            json={},
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid raw_dir or stg_dir parameter", resp.json["message"])

    @mock.patch("job2.main.load_to_stage")
    def test_return_400_invalid_path_with_file_extension(
        self, load_to_stage_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when path looks like a file (has extension)
        """
        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/file.txt",
                "stg_dir": "/foo/baz/",
            },
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid raw_dir or stg_dir parameter", resp.json["message"])

    @mock.patch("job2.main.load_to_stage")
    def test_load_to_stage_called_with_proper_params(
        self, load_to_stage_mock: mock.MagicMock
    ):
        """
        Test whether load_to_stage is called with proper params
        """
        load_to_stage_mock.return_value = (True, "Success")

        fake_raw_dir = "/foo/bar/"
        fake_stg_dir = "/foo/baz/"
        self.client.post(
            "/",
            json={
                "raw_dir": fake_raw_dir,
                "stg_dir": fake_stg_dir,
            },
        )

        load_to_stage_mock.assert_called_with(
            raw_dir=fake_raw_dir,
            stg_dir=fake_stg_dir,
        )

    @mock.patch("job2.main.load_to_stage")
    def test_return_201_when_all_is_ok(self, load_to_stage_mock: mock.MagicMock):
        """
        Return 201 HTTP code when everything is successful
        """
        load_to_stage_mock.return_value = (True, "Data loaded successfully")

        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                "stg_dir": "/foo/baz/",
            },
        )

        self.assertEqual(201, resp.status_code)
        self.assertIn("message", resp.json)
        self.assertEqual("Data loaded successfully", resp.json["message"])

    @mock.patch("job2.main.load_to_stage")
    def test_return_500_when_load_fails(self, load_to_stage_mock: mock.MagicMock):
        """
        Return 500 HTTP code when load_to_stage fails
        """
        load_to_stage_mock.return_value = (False, "Error loading data")

        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                "stg_dir": "/foo/baz/",
            },
        )

        self.assertEqual(500, resp.status_code)
        self.assertIn("message", resp.json)
        self.assertEqual("Error loading data", resp.json["message"])
