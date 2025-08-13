from typing import Tuple
from Exceptions.LoadExceptions import LoadException
from Exceptions.ValidateExceptions import InputFileDoesNotExist, DataFileDoesNotExist, InvalidInputKeys, InvalidUUID, \
    ContextPathDoesNotExist, InvalidDateParsingFormat, DateOutOfRange, InvalidBirthDate, InvalidParticipantAge, \
    InvalidValueLength


class StatusCodeExceptionTranslator:
    """
    This class provides a centralized mechanism for converting custom exceptions raised
    throughout the ETL pipeline into consistent status codes and human-readable error messages.
    It recognizes all custom exceptions defined in the application and provides fallback
    handling for unknown exceptions.
    """
    def translate_custom_exceptions(self, e: Exception) -> Tuple[int, str]:
        """
         This method checks if the provided exception is one of the recognized custom
        exceptions defined in the application. For recognized exceptions, it extracts
        the formatted error message using str(e). For unknown exceptions, it provides
        a generic error message while preserving the original exception information.

        :param e: The exception instance to be translated.
        :return: A tuple containing the status code and error message:
                - status_code (int): Currently returns 1 for all exceptions (both recognized and unknown)
                - message (str): For recognized exceptions, returns the formatted message from str(e).
                               For unknown exceptions, returns "Unknown exception: <exception_details>"
        """
        valid_exceptions = [InputFileDoesNotExist, InvalidInputKeys, InvalidUUID, ContextPathDoesNotExist,
                            DataFileDoesNotExist, DateOutOfRange, InvalidDateParsingFormat,
                            InvalidBirthDate, InvalidParticipantAge,InvalidValueLength, LoadException]
        if type(e) in valid_exceptions:
            return 1, str(e)
        else:
            return 1, f"Unknown exception: {e}"
