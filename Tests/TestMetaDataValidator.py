import unittest
from datetime import datetime
from Pipeline.MetaDataValidator import MetaDataValidator
from Exceptions.ValidateExceptions import (
    DateOutOfRange, InvalidDateParsingFormat, InvalidBirthDate, InvalidParticipantAge
)


class TestMetaDataValidator(unittest.TestCase):

    def setUp(self):
        self.validator = MetaDataValidator()

    def test_validate_valid_metadata(self):
        metadata = {
            "name": "John Doe",
            "date_of_birth": "1980-05-15",
            "location": "Boston"
        }

        try:
            self.validator.validate_metadata(metadata)
        except Exception:
            self.fail("validate_metadata raised an exception with valid data")

    def test_validate_nested_metadata(self):
        metadata = {
            "participant": {
                "name": "Jane Doe",
                "date_of_birth": "1975-12-20",
                "contact": {
                    "email": "jane@example.com"
                }
            }
        }

        try:
            self.validator.validate_metadata(metadata)
        except Exception:
            self.fail("validate_metadata raised an exception with valid nested data")

    def test_validate_birth_date_too_young(self):
        metadata = {"date_of_birth": "2010-01-01"}

        with self.assertRaises(InvalidParticipantAge):
            self.validator.validate_metadata(metadata)

    def test_validate_invalid_birth_date_format(self):
        metadata = {"date_of_birth": "invalid-date"}

        with self.assertRaises(InvalidBirthDate):
            self.validator.validate_metadata(metadata)

    def test_validate_date_out_of_range(self):
        metadata = {"visit_date": "2030-12-31"}

        with self.assertRaises(DateOutOfRange):
            self.validator.validate_metadata(metadata)

    def test_validate_string_too_long(self):
        long_string = "a" * 100
        metadata = {"description": long_string}

        with self.assertRaises(InvalidParticipantAge):
            self.validator.validate_metadata(metadata)

    def test_is_date_string_valid(self):
        valid_dates = [
            "2020-01-15",
            "15/01/2020",
            "01/15/2020",
            "2020-01-15 14:30:00",
            "15-01-2020"
        ]

        for date_str in valid_dates:
            self.assertTrue(self.validator._is_date_string(date_str))

    def test_is_date_string_invalid(self):
        invalid_dates = ["not-a-date", "2020/15/30", "abc123"]

        for date_str in invalid_dates:
            self.assertFalse(self.validator._is_date_string(date_str))

    def test_parse_date_string_valid(self):
        date_str = "2020-05-15"
        result = self.validator._parse_date_string(date_str)

        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 5)
        self.assertEqual(result.day, 15)

    def test_parse_date_string_invalid(self):
        result = self.validator._parse_date_string("invalid-date")
        self.assertIsNone(result)

    def test_calculate_age(self):
        birth_date = datetime(1980, 6, 15)
        age = self.validator._calculate_age(birth_date)

        current_year = datetime.now().year
        expected_age = current_year - 1980

        self.assertIn(age, [expected_age - 1, expected_age])


if __name__ == '__main__':
    unittest.main()