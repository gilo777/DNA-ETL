from Exceptions.ValidateExceptions import InputFileDoesNotExist
class StatusCodeExceptionTranslator:
## think about ENUMS
## fix the euality check of exceptions
    def translate_custom_exceptions(e: Exception) -> int:
        match e:
            case 'InputFileDoesNotExist':
                return 1, e.message ## to save the information
            case 'InvalidInputKeys':
                return 2
            case 'InvalidUUID':
                return 3
            case 'ContextPathDoesNotExist':
                return 4
            case 'DataFileDoesNotExist':
                return 5
            case 'DateOutOfRange':
                return 6
            case 'InvalidDateParsingFormat':
                return 7
            case 'InvalidBirthDate':
                return 8
            case 'InvalidParticipantAge':
                return 9
            case 'LoadException':
                return 10

    def translate_custom_code(code : int) -> str:
        match code:
            case 1:
                return 'InputFileDoesNotExist'
            case 2:
                return 'InvalidInputKeys'
            case 3:
                return 'InvalidUUID'
            case 4:
                return 'ContextPathDoesNotExist'
            case 5:
                return 'DataFileDoesNotExist'
            case 6:
                return 'DateOutOfRange'
            case 7:
                return 'InvalidDateParsingFormat'
            case 8:
                return 'InvalidBirthDate'
            case 9:
                return 'InvalidParticipantAge'
            case 10:
                return 'LoadException'





