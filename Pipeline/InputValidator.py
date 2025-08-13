import json
import uuid
from pathlib import Path
from typing import List, Tuple

from Exceptions.ValidateExceptions import InputFileDoesNotExist, InvalidInputKeys, InvalidqUUID, InvalidUUID, \
    ContextPathDoesNotExist
from Pipeline.DataModels.ValidPaths import ValidPaths
class InputValidator:
    """
    Validates input JSON files and verifies the existence of required genetic data files.

    Ensures that input configuration files contain valid keys and that all referenced
    file paths exist with the expected naming convention based on patient UUIDs.

    Attributes:
        valid_keys (List[str]): List of required keys that must be present in input JSON files

    Methods:
          validate(path : str): Validates an input JSON file and verifies all referenced genetic data files exist.
    """
    def __init__(self, valid_keys: List[str]):
        self.valid_keys = valid_keys

    def validate(self, path : str) -> Tuple[ValidPaths, str]:
        """
        Performs comprehensive validation including JSON structure, key validation,
        UUID format verification, and file existence checks for DNA and metadata files.

        :param path: Path to the JSON input file to validate
        :return:
            Tuple[ValidPaths, str]: A tuple containing:
                - ValidPaths object with verified file paths
                - Patient UUID string extracted from the context path
        """
        # Load Json into a dictionary.
        try:
            with open(path, "r") as file:
                input_dict = json.load(file)
        except FileNotFoundError:
            raise InputFileDoesNotExist(path)
        # Validate keys in the dictionary are as required.
        if set(self.valid_keys) != set(input_dict.keys()):
            raise InvalidInputKeys(path)
        # Load context path and patient UUID
        context_path = Path(input_dict["context_path"])
        patient_uuid = str(context_path.name)
        # Validate patient UUID
        try:
            uuid.UUID(patient_uuid)
        except ValueError:
            raise InvalidUUID(patient_uuid)
        # Validate context path exists.
        if not context_path.exists():
            raise ContextPathDoesNotExist(context_path)
        # Validate paths inside the patient directory.
        dna_path = context_path / (patient_uuid + '_dna.txt')
        metadata_path = context_path / (patient_uuid + '_dna.json')
        if not dna_path.exists() or not metadata_path.exists():
            raise InputFileDoesNotExist(context_path)

        return ValidPaths(dna_path, metadata_path, context_path, Path(input_dict["results_path"])), patient_uuid

