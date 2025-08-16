## Constants file contains values for constants used in the pipeline.
from Exceptions.LoaderExceptions import LoaderException
from Exceptions.ValidateExceptions import (
    InvalidBirthDate,
    InvalidParticipantAge,
    InvalidValueLength,
    InputFileDoesNotExist,
    InvalidInputKeys,
    InvalidUUID,
    ContextPathDoesNotExist,
    DataFileDoesNotExist,
    DateOutOfRange,
    InvalidDateParsingFormat,
    InvalidJSONFormat
)

VALID_INPUT_KEYS = ["context_path", "results_path"]
MIN_AGE = 40
MAX_VALUE_LEN = 64
YEAR_RANGE_LOWER = 2014
YEAR_RANGE_UPPER = 2024
VALID_DATE_FORMATS = [
    "%Y-%m-%d",  # 2024-01-15
    "%d/%m/%Y",  # 15/01/2024
    "%m/%d/%Y",  # 01/15/2024
    "%Y-%m-%d %H:%M:%S",  # 2024-01-15 14:30:00
    "%d-%m-%Y",  # 15-01-2024
]
valid_exceptions = [
    InputFileDoesNotExist,
    InvalidInputKeys,
    InvalidUUID,
    ContextPathDoesNotExist,
    DataFileDoesNotExist,
    DateOutOfRange,
    InvalidDateParsingFormat,
    InvalidBirthDate,
    InvalidParticipantAge,
    InvalidValueLength,
    LoaderException,
    InvalidJSONFormat
]
