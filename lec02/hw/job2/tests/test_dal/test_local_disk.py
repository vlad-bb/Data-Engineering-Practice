"""
Tests dal.local_disk.py module
"""

import json
import os
import tempfile
from pathlib import Path
from unittest import TestCase, mock

from job2.dal.local_disk import load_to_stage, save_avro


class LoadToStageTestCase(TestCase):
    """
    Test dal.local_disk.load_to_stage function.
    """

    def setUp(self):
        """Create temporary directory for tests"""
        self.temp_dir = tempfile.mkdtemp()
        self.base_dir_patcher = mock.patch(
            "job2.dal.local_disk.BASE_DIR", Path(self.temp_dir)
        )
        self.base_dir_patcher.start()

    def tearDown(self):
        """Clean up temporary directory after tests"""
        self.base_dir_patcher.stop()
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_load_to_stage_source_directory_not_exists(self):
        """
        Test that load_to_stage returns error when source directory doesn't exist
        """
        stg_dir = "/stg/sales/2022-08-09"
        raw_dir = "/raw/sales/2022-08-09"

        is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)

        self.assertFalse(is_success)
        self.assertIn("does not exist", msg)

    def test_load_to_stage_creates_target_directory(self):
        """
        Test that load_to_stage creates target directory
        """
        # Create source directory with JSON file
        source_dir = (
            Path(self.temp_dir) / "file_storage" / "raw" / "sales" / "2022-08-09"
        )
        source_dir.mkdir(parents=True)

        test_data = [
            {
                "client": "Client A",
                "purchase_date": "2022-08-09",
                "product": "Product 1",
                "price": 100,
            }
        ]
        with open(source_dir / "sales_2022-08-09.json", "w") as f:
            json.dump(test_data, f)

        stg_dir = "/stg/sales/2022-08-09"
        raw_dir = "/raw/sales/2022-08-09"

        is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)

        self.assertTrue(is_success)
        target_dir = (
            Path(self.temp_dir) / "file_storage" / "stg" / "sales" / "2022-08-09"
        )
        self.assertTrue(target_dir.exists())

    def test_load_to_stage_removes_existing_target_directory(self):
        """
        Test that load_to_stage removes existing target directory before creating new one
        """
        # Create source directory with JSON file
        source_dir = (
            Path(self.temp_dir) / "file_storage" / "raw" / "sales" / "2022-08-09"
        )
        source_dir.mkdir(parents=True)

        test_data = [
            {
                "client": "Client A",
                "purchase_date": "2022-08-09",
                "product": "Product 1",
                "price": 100,
            }
        ]
        with open(source_dir / "sales_2022-08-09.json", "w") as f:
            json.dump(test_data, f)

        # Create existing target directory with old file
        target_dir = (
            Path(self.temp_dir) / "file_storage" / "stg" / "sales" / "2022-08-09"
        )
        target_dir.mkdir(parents=True)
        old_file = target_dir / "old_file.txt"
        old_file.write_text("old data")

        stg_dir = "/stg/sales/2022-08-09"
        raw_dir = "/raw/sales/2022-08-09"

        is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)

        self.assertTrue(is_success)
        self.assertFalse(old_file.exists())

    def test_load_to_stage_creates_avro_file(self):
        """
        Test that load_to_stage creates AVRO file with correct name
        """
        # Create source directory with JSON file
        source_dir = (
            Path(self.temp_dir) / "file_storage" / "raw" / "sales" / "2022-08-09"
        )
        source_dir.mkdir(parents=True)

        test_data = [
            {
                "client": "Client A",
                "purchase_date": "2022-08-09",
                "product": "Product 1",
                "price": 100,
            }
        ]
        with open(source_dir / "sales_2022-08-09.json", "w") as f:
            json.dump(test_data, f)

        stg_dir = "/stg/sales/2022-08-09"
        raw_dir = "/raw/sales/2022-08-09"

        is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)

        self.assertTrue(is_success)
        target_file = (
            Path(self.temp_dir)
            / "file_storage"
            / "stg"
            / "sales"
            / "2022-08-09"
            / "sales_2022-08-09.avro"
        )
        self.assertTrue(target_file.exists())

    @mock.patch("job2.dal.local_disk.save_avro")
    def test_load_to_stage_calls_save_avro(self, save_avro_mock: mock.MagicMock):
        """
        Test that load_to_stage calls save_avro with correct parameters
        """
        # Create source directory
        source_dir = (
            Path(self.temp_dir) / "file_storage" / "raw" / "sales" / "2022-08-09"
        )
        source_dir.mkdir(parents=True)

        stg_dir = "/stg/sales/2022-08-09"
        raw_dir = "/raw/sales/2022-08-09"

        is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)

        self.assertTrue(is_success)
        save_avro_mock.assert_called_once()

    def test_load_to_stage_handles_exception(self):
        """
        Test that load_to_stage handles exceptions gracefully
        """
        with mock.patch(
            "job2.dal.local_disk.os.makedirs", side_effect=Exception("Test error")
        ):
            # Create source directory
            source_dir = (
                Path(self.temp_dir) / "file_storage" / "raw" / "sales" / "2022-08-09"
            )
            source_dir.mkdir(parents=True)

            stg_dir = "/stg/sales/2022-08-09"
            raw_dir = "/raw/sales/2022-08-09"

            is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)

            self.assertFalse(is_success)
            self.assertIn("Error loading data", msg)


class SaveAvroTestCase(TestCase):
    """
    Test dal.local_disk.save_avro function.
    """

    def setUp(self):
        """Create temporary directory for tests"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory after tests"""
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_save_avro_reads_json_and_writes_avro(self):
        """
        Test that save_avro reads JSON files and writes AVRO file
        """
        # Create source directory with JSON files
        source_dir = Path(self.temp_dir) / "source"
        source_dir.mkdir()

        test_data1 = [
            {
                "client": "Client A",
                "purchase_date": "2022-08-09",
                "product": "Product 1",
                "price": 100,
            }
        ]
        test_data2 = [
            {
                "client": "Client B",
                "purchase_date": "2022-08-09",
                "product": "Product 2",
                "price": 200,
            }
        ]

        with open(source_dir / "sales1.json", "w") as f:
            json.dump(test_data1, f)
        with open(source_dir / "sales2.json", "w") as f:
            json.dump(test_data2, f)

        target_file = Path(self.temp_dir) / "output.avro"

        save_avro(source_dir, target_file)

        self.assertTrue(target_file.exists())
        self.assertGreater(target_file.stat().st_size, 0)

    def test_save_avro_handles_multiple_json_files(self):
        """
        Test that save_avro correctly processes multiple JSON files
        """
        # Create source directory with multiple JSON files
        source_dir = Path(self.temp_dir) / "source"
        source_dir.mkdir()

        for i in range(3):
            test_data = [
                {
                    "client": f"Client {i}",
                    "purchase_date": "2022-08-09",
                    "product": f"Product {i}",
                    "price": 100 * i,
                }
            ]
            with open(source_dir / f"sales{i}.json", "w") as f:
                json.dump(test_data, f)

        target_file = Path(self.temp_dir) / "output.avro"

        save_avro(source_dir, target_file)

        self.assertTrue(target_file.exists())

    def test_save_avro_with_empty_directory(self):
        """
        Test that save_avro handles empty source directory
        """
        source_dir = Path(self.temp_dir) / "source"
        source_dir.mkdir()

        target_file = Path(self.temp_dir) / "output.avro"

        save_avro(source_dir, target_file)

        self.assertTrue(target_file.exists())
