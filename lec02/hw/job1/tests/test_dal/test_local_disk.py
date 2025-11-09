"""
Tests dal.local_disk.py module
"""

import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest import TestCase, mock

from job1.dal.local_disk import save_to_disk


class SaveToDiskTestCase(TestCase):
    """
    Test dal.local_disk.save_to_disk function.
    """

    def setUp(self):
        """Create temporary directory for tests"""
        self.temp_dir = tempfile.mkdtemp()
        self.base_dir_patcher = mock.patch(
            "job1.dal.local_disk.BASE_DIR", Path(self.temp_dir)
        )
        self.base_dir_patcher.start()

    def tearDown(self):
        """Clean up temporary directory after tests"""
        self.base_dir_patcher.stop()

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_save_to_disk_creates_file(self):
        """
        Test that save_to_disk creates file with correct content
        """
        test_data = [
            {"id": 1, "product": "Product A", "amount": 100},
            {"id": 2, "product": "Product B", "amount": 200},
        ]
        test_path = "/raw/sales/2022-08-09"
        test_date = "2022-08-09"

        save_to_disk(test_data, test_path, test_date)

        expected_file = (
            Path(self.temp_dir)
            / "file_storage"
            / "raw"
            / "sales"
            / "2022-08-09"
            / f"sales_{test_date}.json"
        )
        self.assertTrue(expected_file.exists())

        with open(expected_file, "r") as f:
            saved_data = json.load(f)

        self.assertEqual(saved_data, test_data)

    def test_save_to_disk_creates_directory_structure(self):
        """
        Test that save_to_disk creates nested directory structure
        """
        test_data = [{"id": 1}]
        test_path = "/raw/sales/2022/08/09"
        test_date = "2022-08-09"

        save_to_disk(test_data, test_path, test_date)

        expected_dir = (
            Path(self.temp_dir)
            / "file_storage"
            / "raw"
            / "sales"
            / "2022"
            / "08"
            / "09"
        )
        self.assertTrue(expected_dir.exists())
        self.assertTrue(expected_dir.is_dir())

    def test_save_to_disk_overwrites_existing_directory(self):
        """
        Test that save_to_disk removes existing directory before creating new one
        """
        test_path = "/raw/sales/2022-08-09"
        test_date = "2022-08-09"

        # Create directory with old data
        old_data = [{"id": 1, "old": True}]
        save_to_disk(old_data, test_path, test_date)

        # Save new data
        new_data = [{"id": 2, "new": True}]
        save_to_disk(new_data, test_path, test_date)

        # Verify only new data exists
        expected_file = (
            Path(self.temp_dir)
            / "file_storage"
            / "raw"
            / "sales"
            / "2022-08-09"
            / f"sales_{test_date}.json"
        )
        with open(expected_file, "r") as f:
            saved_data = json.load(f)

        self.assertEqual(saved_data, new_data)

    def test_save_to_disk_strips_leading_slash(self):
        """
        Test that save_to_disk correctly strips leading slash from path
        """
        test_data = [{"id": 1}]
        test_date = "2022-08-09"

        # Test with leading slash
        save_to_disk(test_data, "/raw/sales", test_date)

        expected_file = (
            Path(self.temp_dir)
            / "file_storage"
            / "raw"
            / "sales"
            / f"sales_{test_date}.json"
        )
        self.assertTrue(expected_file.exists())

    def test_save_to_disk_json_format(self):
        """
        Test that save_to_disk saves JSON with correct formatting
        """
        test_data = [
            {"id": 1, "name": "Test", "unicode": "Тест"},
        ]
        test_path = "/raw/sales"
        test_date = "2022-08-09"

        save_to_disk(test_data, test_path, test_date)

        expected_file = (
            Path(self.temp_dir)
            / "file_storage"
            / "raw"
            / "sales"
            / f"sales_{test_date}.json"
        )

        with open(expected_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Check that unicode is preserved (ensure_ascii=False)
        self.assertIn("Тест", content)
        # Check that file is indented (indent=4)
        self.assertIn("    ", content)

    def test_save_to_disk_empty_data(self):
        """
        Test that save_to_disk handles empty data list
        """
        test_data = []
        test_path = "/raw/sales"
        test_date = "2022-08-09"

        save_to_disk(test_data, test_path, test_date)

        expected_file = (
            Path(self.temp_dir)
            / "file_storage"
            / "raw"
            / "sales"
            / f"sales_{test_date}.json"
        )
        self.assertTrue(expected_file.exists())

        with open(expected_file, "r") as f:
            saved_data = json.load(f)

        self.assertEqual(saved_data, [])
