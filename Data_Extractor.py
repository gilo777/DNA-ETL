import json
from typing import Tuple

from DataModels import ValidPaths
from DataModels.DnaData import DnaData
from FileValidator import FileValidator


class Data_Extractor:

    def __init__(self):
         self.validator = FileValidator()


    def extract(self, paths : ValidPaths) -> Tuple[dict, DnaData]:
        dna_data = self._extract_dna(paths.dna_path)
        metadata = self._extract_metadata(paths.metadata_path)

        if not self.validator.validate_metadata(metadata):
            raise Exception("DNA file not valid")

        return metadata, dna_data



    def _extract_dna(self, dna_file_path: str) -> DnaData:
        dna_data = DnaData()
        with open(dna_file_path, "r") as dna_file:
            for line in dna_file.readlines():
                if line != "":
                    dna_data.sequences.append(line)
        return dna_data

    def _extract_metadata(self, metadata_path: str) -> dict:
        with open(metadata_path, "r") as metadata_file:
            metadata = json.load(metadata_file)
            return metadata