from datetime import datetime


class FileValidator:
    def __init__(self):
        pass


        # check values lengths
        # check participant age
        # check All other dates should be between [2014-2024]

    def validate(self, metadata_dict):
        valid = True
        for key, value in metadata_dict.items():
            if isinstance(value, dict):
                valid = valid and self.recursive_checks(value)
            else:
                if isinstance(value, str):
                    if self.is_date_string(value):
                        date = self.parse_date_string(value)
                        if date:
                            if not (2014 <= date.year <= 2024):
                                valid = False
                        else:
                            valid = False

                    valid = valid and len(value) <= 64

                if key == 'date_of_birth':
                    if isinstance(value, str) and self.is_date_string(value):
                        birth_date = self.parse_date_string(value)
                        if birth_date:
                            age = self.calculate_age(birth_date)
                            valid = valid and age >= 40
                        else:
                            valid = False
                    else:
                        valid = False
            if not valid:
                return False
        return valid


    def is_date_string(self, value, date_formats=None):
        """Check if a string can be parsed as a date"""
        if not isinstance(value, str):
            return False

        # Common date formats to try
        if date_formats is None:
            date_formats = [
                '%Y-%m-%d',  # 2024-01-15
                '%d/%m/%Y',  # 15/01/2024
                '%m/%d/%Y',  # 01/15/2024
                '%Y-%m-%d %H:%M:%S',  # 2024-01-15 14:30:00
                '%d-%m-%Y',  # 15-01-2024
            ]

        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False


    def parse_date_string(self, value, date_formats=None):
        """Parse a date string and return datetime object"""
        if date_formats is None:
            date_formats = [
                '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
                '%Y-%m-%d %H:%M:%S', '%d-%m-%Y'
            ]

        for fmt in date_formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def calculate_age(self, birth_date):
        today = datetime.now()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))





