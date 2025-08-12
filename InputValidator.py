import json
import uuid
from importlib.metadata import metadata
from pathlib import Path
from typing import List, Tuple

from DataModels.ValidPaths import ValidPaths


class InputValidator:

    def __init__(self, valid_keys: List[str]):
        self.valid_keys = valid_keys

    def validate(self, path : str) -> Tuple[ValidPaths, str]:
        with open(path, "r") as file:
            input_dict = json.load(file)
        # validate keys
        if set(self.valid_keys) != set(input_dict.keys()):
            raise Exception(f"File {path} contains illegal keys")
        # validate path is real
        context_path = Path(input_dict["context_path"])
        patient_uuid = str(context_path.name)
        try:
            uuid.UUID(patient_uuid)
        except ValueError:
            return None
        if not context_path.exists():
            return None
        dna_path = context_path / (patient_uuid + '_dna.txt')
        metadata_path = context_path / (patient_uuid + '_dna.json')
        if not dna_path.exists() or not metadata_path.exists():
            return None
        return ValidPaths(dna_path, metadata_path, Path(input_dict["results_path"])), patient_uuid

