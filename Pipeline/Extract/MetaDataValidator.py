from datetime import datetime

from Pipeline.DataModelsAndConstants.Constants import YEAR_RANGE_LOWER, YEAR_RANGE_UPPER, MAX_VALUE_LEN, MIN_AGE, VALID_DATE_FORMATS


class MetaDataValidator:

        # check values lengths
        # check participant age
        # check All other dates should be between [2014-2024]

    # given a participant metadata, check if it meets the conditions :
    def validate_metadata(self, metadata_dict : dict) -> bool:
        valid_metadata_value = True
        for metadata_key, metadata_value in metadata_dict.items():
            # recursive call
            if isinstance(metadata_value, dict):
                valid_metadata_value = valid_metadata_value and self.validate_metadata(metadata_value)
            else:
                # validate age
                if metadata_key == 'date_of_birth':
                    valid_metadata_value = valid_metadata_value and self._validate_birth_date_value(metadata_value)
                # validate value
                elif isinstance(metadata_value, str):
                    valid_metadata_value = valid_metadata_value and self._validate_string_value(metadata_value)

            if not valid_metadata_value:
                return False
        return valid_metadata_value

    # given a value, check if it is a date- if it is, check it is in range
        # check value length is in range
    def _validate_string_value(self, value: str) -> bool:

        if self._is_date_string(value):
            date = self._parse_date_string(value)
            if date:
                if not YEAR_RANGE_LOWER <= date.year <= YEAR_RANGE_UPPER:
                    return False
            else:
                return False

        return len(value) <= MAX_VALUE_LEN


    # validate age.
    def _validate_birth_date_value(self, value) -> bool:
        if isinstance(value, str) and self._is_date_string(value):
            birth_date = self._parse_date_string(value)
            if birth_date:
                age = self._calculate_age(birth_date)
                return age >= MIN_AGE
            else:
                return False
        else:
            return False

    # def is_date_in_range(self, date) -> bool:
    #     return year_range_lower <= date.year <= year_range_upper
    #
    # def is_value_length_valid(self, value: str) -> bool:
    #      return len(value) <= max_value_len

    # given a string, check if it's a date
    def _is_date_string(self, value : str, date_formats=None) -> bool:
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

    # parse a string to a datetime for easy access to year, month etc.
    def _parse_date_string(self, value : str, date_formats=None) -> datetime:
        """Parse a date string and return datetime object"""
        if date_formats is None:
            date_formats = VALID_DATE_FORMATS

        for date_format in date_formats:
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
        return None

    # calculate the age, given a birth date
    def _calculate_age(self, birth_date : datetime) -> int:
        today = datetime.now()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))





