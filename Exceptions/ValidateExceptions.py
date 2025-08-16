import datetime


class InputFileDoesNotExist(Exception):
    """Raised when the specified input file cannot be found at the given path"""

    def __init__(self, input_path: str):
        self.input_path = input_path
        super().__init__(f"Input file does not exist: {self.input_path}")


class InvalidInputKeys(Exception):
    """Raised when the input file contains keys that don't match the expected valid keys"""

    def __init__(self, input_path: str):
        self.input_path = input_path
        super().__init__(f"Input file don't match valid keys: {self.input_path}")


class InvalidUUID(Exception):
    """Raised when a UUID string doesn't conform to the standard UUID format"""

    def __init__(self, UUID: str):
        self.uuid = UUID
        super().__init__(f"Invalid UUID: {self.uuid}")


class ContextPathDoesNotExist(Exception):
    """Raised when a required context directory cannot be found"""

    def __init__(self, context_path: str):
        self.context_path = context_path
        super().__init__(f"Context path does not exist: {self.context_path}")


class DataFileDoesNotExist(Exception):
    """Raised when expected data files are missing from the specified context path"""

    def __init__(self, context_path: str):
        self.context_path = context_path
        super().__init__(f"Data file does not exist in: {self.context_path}")


class DateOutOfRange(Exception):
    """Raised when a date falls outside the acceptable range for the application"""

    def __init__(self, date: datetime.date):
        self.date = date
        super().__init__(f"Date out of range: {self.date}")


class InvalidDateParsingFormat(Exception):
    """Raised when a date string cannot be parsed using the expected date format"""

    def __init__(self, date: str):
        self.date = date
        super().__init__(f"Invalid date format: {self.date}")


class InvalidBirthDate(Exception):
    """Raised when a birth date is invalid"""

    def __init__(self, date: str):
        self.date = date
        super().__init__(f"Invalid date format: {self.date}")


class InvalidParticipantAge(Exception):
    """Raised when a participant's age is outside the valid range for the study"""

    def __init__(self, participant_age: int):
        self.participant_age = participant_age
        super().__init__(f"Invalid participant age: {self.participant_age}")


class InvalidValueLength(Exception):
    """Raised when a value's length is invalid"""

    def __init__(self, value: str):
        self.value = value
        super().__init__(f"Invalid value length for value: {self.value}")

class InvalidJSONFormat(Exception):
    """Raised when a JSON file cannot be parsed as a valid JSON"""
    def __init__(self, path: str):
        super().__init__(f"Invalid JSON file: {path}")