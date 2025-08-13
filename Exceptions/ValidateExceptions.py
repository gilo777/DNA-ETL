import datetime

class InputFileDoesNotExist(Exception):
    def __init__(self, input_path: str):
        self.input_path = input_path
        super().__init__(f"Input file does not exist: {self.input_path}")

class InvalidInputKeys(Exception):
    def __init__(self, input_path: str):
        self.input_path = input_path
        super().__init__(f"Input file don't match valid keys: {self.input_path}")

class InvalidUUID(Exception):
    def __init__(self, UUID: str):
        self.uuid = UUID
        super().__init__(f"Invalid UUID: {self.uuid}")

class ContextPathDoesNotExist(Exception):
    def __init__(self, context_path: str):
        self.context_path = context_path
        super().__init__(f"Context path does not exist: {self.context_path}")

class DataFileDoesNotExist(Exception):
    def __init__(self, context_path: str):
        self.context_path = context_path
        super().__init__(f"Data file does not exist in: {self.context_path}")

class DateOutOfRange(Exception):
    def __init__(self, date : datetime.date):
        self.date = date
        super().__init__(f"Date out of range: {self.date}")

class InvalidDateParsingFormat(Exception):
    def __init__(self, date: str):
        self.date = date
        super().__init__(f"Invalid date format: {self.date}")

class InvalidBirthDate(Exception):
    def __init__(self, date: str):
        self.date = date
        super().__init__(f"Invalid date format: {self.date}")

class InvalidParticipantAge(Exception):
    def __init__(self, participant_age: int):
        self.participant_age = participant_age
        super().__init__(f"Invalid participant age: {self.participant_age}")
