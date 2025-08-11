import json

from DataModels import ValidPaths
from DataModels.DnaData import DnaData
from FileValidator import FileValidator


class Extructor:

    def __init__(self):
         self.validator = FileValidator()


    def extract(self, paths : ValidPaths):
        dna_data = self._extract_dna(paths.dna_path)
        meta_data = self._extract_metadata(paths.metadata_path)

        if not self.validator.validate(meta_data):
            raise Exception("DNA file not valid")

        return meta_data, dna_data



    def _extract_dna(self, dna_file_path: str) -> DnaData:
        dna_data = DnaData()
        with open(dna_file_path, "r") as dna_file:
            for line in dna_file.readlines():
                if line != "":
                    dna_data.sequences.append(line)
        return dna_data

    def _extract_metadata(self, meta_data_path: str) -> dict:
        with open(meta_data_path, "r") as meta_data_file:
            meta_data = json.load(meta_data_file)
            return meta_data