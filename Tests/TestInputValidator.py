import unittest
import json
import tempfile
import os
import uuid
from pathlib import Path
from Pipeline.InputValidator import InputValidator
from Exceptions.ValidateExceptions import (
    InputFileDoesNotExist, InvalidInputKeys, InvalidUUID, ContextPathDoesNotExist
)


class TestInputValidator(unittest.TestCase):

    def setUp(self):
        self.validator = InputValidator(["context_path", "results_path"])
        self.test_uuid = str(uuid.uuid4())

    def test_validate_valid_input(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            context_dir = Path(temp_dir) / self.test_uuid
            context_dir.mkdir()

            dna_file = context_dir / f"{self.test_uuid}_dna.txt"
            metadata_file = context_dir / f"{self.test_uuid}_dna.json"

            dna_file.write_text("ATCG")
            metadata_file.write_text('{"test": "data"}')

            input_data = {
                "context_path": str(context_dir),
                "results_path": "/tmp/output.json"
            }

            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(input_data, f)
                input_path = f.name

            try:
                valid_paths, patient_id = self.validator.validate(input_path)
                self.assertEqual(patient_id, self.test_uuid)
                self.assertEqual(str(valid_paths.context_path), str(context_dir))
            finally:
                os.unlink(input_path)

    def test_validate_missing_input_file(self):
        with self.assertRaises(InputFileDoesNotExist):
            self.validator.validate("/nonexistent/path.json")

    def test_validate_invalid_keys(self):
        input_data = {"wrong_key": "value"}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(input_data, f)
            input_path = f.name

        try:
            with self.assertRaises(InvalidInputKeys):
                self.validator.validate(input_path)
        finally:
            os.unlink(input_path)

    def test_validate_invalid_uuid(self):
        input_data = {
            "context_path": "/path/to/invalid-uuid-format",
            "results_path": "/tmp/output.json"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(input_data, f)
            input_path = f.name

        try:
            with self.assertRaises(InvalidUUID):
                self.validator.validate(input_path)
        finally:
            os.unlink(input_path)

    def test_validate_missing_context_path(self):
        input_data = {
            "context_path": f"/nonexistent/{self.test_uuid}",
            "results_path": "/tmp/output.json"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(input_data, f)
            input_path = f.name

        try:
            with self.assertRaises(ContextPathDoesNotExist):
                self.validator.validate(input_path)
        finally:
            os.unlink(input_path)

    def test_validate_missing_data_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            context_dir = Path(temp_dir) / self.test_uuid
            context_dir.mkdir()

            input_data = {
                "context_path": str(context_dir),
                "results_path": "/tmp/output.json"
            }

            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(input_data, f)
                input_path = f.name

            try:
                with self.assertRaises(InputFileDoesNotExist):
                    self.validator.validate(input_path)
            finally:
                os.unlink(input_path)


if __name__ == '__main__':
    unittest.main()