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

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_return_400_date_param_missed(self, save_sales_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'date' param or date is None
        """
        # Test with missing date (None)
        with self.assertRaises((TypeError, AttributeError)):
            resp = self.client.post(
                "/",
                json={
                    "raw_dir": "/foo/bar/",
                    # no 'date' set!
                },
            )

        # Test with empty string date
        resp = self.client.post(
            "/",
            json={
                "date": "",
                "raw_dir": "/foo/bar/",
            },
        )
        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid date parameter", resp.json["message"])

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_return_400_raw_dir_param_missed(self, save_sales_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'raw_dir' param
        """
        resp = self.client.post(
            "/",
            json={
                "date": "2022-08-09",
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid raw_dir parameter", resp.json["message"])

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_return_400_invalid_date_format(self, save_sales_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when date has invalid format
        """
        resp = self.client.post(
            "/",
            json={
                "date": "invalid-date",
                "raw_dir": "/foo/bar/",
            },
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid date parameter", resp.json["message"])

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_return_400_invalid_path_with_file_extension(
        self, save_sales_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when raw_dir looks like a file (has extension)
        """
        resp = self.client.post(
            "/",
            json={
                "date": "2022-08-09",
                "raw_dir": "/foo/bar/file.txt",
            },
        )

        self.assertEqual(400, resp.status_code)
        self.assertIn("Invalid raw_dir parameter", resp.json["message"])

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_save_sales_to_local_disk(
        self, save_sales_to_local_disk_mock: mock.MagicMock
    ):
        """
        Test whether save_sales_to_local_disk is called with proper params
        """
        save_sales_to_local_disk_mock.return_value = (True, "Success")

        fake_date = "1970-01-01"
        fake_raw_dir = "/foo/bar/"
        self.client.post(
            "/",
            json={
                "date": fake_date,
                "raw_dir": fake_raw_dir,
            },
        )

        save_sales_to_local_disk_mock.assert_called_with(
            date=fake_date,
            raw_dir=fake_raw_dir,
        )

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_return_201_when_all_is_ok(self, save_sales_mock: mock.MagicMock):
        """
        Return 201 HTTP code when everything is successful
        """
        save_sales_mock.return_value = (True, "Data retrieved successfully")

        resp = self.client.post(
            "/",
            json={
                "date": "2022-08-09",
                "raw_dir": "/foo/bar/",
            },
        )

        self.assertEqual(201, resp.status_code)
        self.assertIn("message", resp.json)
        self.assertEqual("Data retrieved successfully", resp.json["message"])

    @mock.patch("job1.main.save_sales_to_local_disk")
    def test_return_500_when_save_fails(self, save_sales_mock: mock.MagicMock):
        """
        Return 500 HTTP code when save_sales_to_local_disk fails
        """
        save_sales_mock.return_value = (False, "Error saving data")

        resp = self.client.post(
            "/",
            json={
                "date": "2022-08-09",
                "raw_dir": "/foo/bar/",
            },
        )

        self.assertEqual(500, resp.status_code)
        self.assertIn("message", resp.json)
        self.assertEqual("Error saving data", resp.json["message"])
