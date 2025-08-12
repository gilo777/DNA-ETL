import json
from typing import Tuple

from Pipeline.DataModels_and_Constants import ValidPaths
from Pipeline.DataModels_and_Constants.DnaData import DnaData
from Pipeline.Extract.MetaDataValidator import MetaDataValidator


class Data_Extractor:

    def __init__(self):
         self.validator = MetaDataValidator()


    def extract(self, paths : ValidPaths) -> Tuple[dict, DnaData]:

        metadata = self._extract_metadata(paths.metadata_path)
        if not self.validator.validate_metadata(metadata):
            raise Exception("metadata file not valid")

        dna_data = self._extract_dna(paths.dna_path)

        return metadata, dna_data



    def _extract_metadata(self, metadata_path: str) -> dict:
        with open(metadata_path, "r") as metadata_file:
            metadata = json.load(metadata_file)
            return metadata

    def _extract_dna(self, dna_file_path: str) -> DnaData:
        dna_data = DnaData()
        with open(dna_file_path, "r") as dna_file:
            for line in dna_file.readlines():
                if line != "":
                    dna_data.sequences.append(line.strip())
        return dna_data