from datetime import datetime
from Constants import YEAR_RANGE_LOWER, YEAR_RANGE_UPPER, MAX_VALUE_LEN, MIN_AGE, VALID_DATE_FORMATS

class MetaDataValidator:
    """
    Performs comprehensive validation of metadata dictionaries including age restrictions,
    string length limits, and year range constraints. Supports recursive
    validation for nested dictionary structures.
    """
    def validate_metadata(self, metadata_dict : dict) -> bool:
        """
        Recursively validates all key-value pairs in a metadata dictionary.

        Applies different validation rules based on key names and value types.
        Special handling for 'date_of_birth' fields with age validation,
        and comprehensive string validation for all other fields.

        :param metadata_dict: Dictionary containing metadata to validate
        :return:
            bool: True if all metadata values pass validation, False otherwise
        """
        # Iterate over all items in the given dictionary
        valid_metadata_value = True
        for metadata_key, metadata_value in metadata_dict.items():
            # Recursive call, if the value is a dictionary
            if isinstance(metadata_value, dict):
                valid_metadata_value = valid_metadata_value and self.validate_metadata(metadata_value)
            else:
                # Age validation
                if metadata_key == 'date_of_birth':
                    valid_metadata_value = valid_metadata_value and self._validate_birth_date_value(metadata_value)
                # Value validation
                elif isinstance(metadata_value, str):
                    valid_metadata_value = valid_metadata_value and self._validate_string_value(metadata_value)

            if not valid_metadata_value:
                return False

        return valid_metadata_value

    def _validate_string_value(self, value: str) -> bool:
        """
        Validates string values for date format and length constraints.

        :param value: String value to validate
        :return:
            bool: True if string passes all validation checks, False otherwise
        """
        # In case of a date, validate range
        if self._is_date_string(value):
            date = self._parse_date_string(value)
            if date:
                if not YEAR_RANGE_LOWER <= date.year <= YEAR_RANGE_UPPER:
                    return False
            else:
                return False
        # Value length validation
        return len(value) <= MAX_VALUE_LEN

    def _validate_birth_date_value(self, value) -> bool:
        """
        Validates birth date values and ensures minimum age requirements are met.

        :param value: Birth date value to validate (expected to be a string)
        :return:
            bool: True if birth date is valid and meets minimum age, False otherwise
        """
        if isinstance(value, str) and self._is_date_string(value):
            birth_date = self._parse_date_string(value)
            if birth_date:
                age = self._calculate_age(birth_date)
                return age >= MIN_AGE
            else:
                return False
        else:
            return False

    def _is_date_string(self, value : str, date_formats=None) -> bool:
        """
        Checks if a string matches any of the accepted date formats.

        :param value: String to check for date format
        :param date_formats: Custom date formats to try. Defaults to VALID_DATE_FORMATS
        :return:
            bool: True if string matches a valid date format, False otherwise
        """
        # Common date formats to try
        if date_formats is None:
            date_formats = VALID_DATE_FORMATS

        for date_format in date_formats:
            try:
                datetime.strptime(value, date_format)
                return True
            except ValueError:
                continue
        return False

    def _parse_date_string(self, value : str, date_formats=None) -> datetime:
        """
        Parses a date string into a datetime object using accepted formats.
        Attempts to parse the string using each format in the date_formats list
        until one succeeds or all formats are exhausted.

        :param value: Date string to parse
        :param date_formats: Date formats to try. Defaults to VALID_DATE_FORMATS
        :return:
            datetime: Parsed datetime object if successful, None if parsing fails
        """
        if date_formats is None:
            date_formats = VALID_DATE_FORMATS

        for date_format in date_formats:
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
        return None

    def _calculate_age(self, birth_date : datetime) -> int:
        """
        Calculates a person's current age from their birth date.
        Computes age by comparing birth date to current date.

        :param birth_date: Person's date of birth
        :return:
            int: Current age in years
        """
        today = datetime.now()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))





