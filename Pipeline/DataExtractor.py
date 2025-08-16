import json
from typing import Tuple

from Pipeline.DataModels import ValidPaths
from Pipeline.DataModels.DNAData import DNAData


class DataExtractor:
    """
    Handles extraction and parsing of metadata and DNA sequence data from files.

    This class provides methods to read JSON metadata files and plain text DNA
    sequence files, returning structured data objects for further processing.

    Methods:
        extract(paths: ValidPaths) -> Tuple[dict, DnaData]:
            Coordinates the extraction of both metadata and DNA data from specified file paths.

        _extract_metadata(metadata_path: str) -> dict:
            Reads and parses a JSON metadata file into a Python dictionary.

        _extract_dna(dna_file_path: str) -> DnaData:
            Reads DNA sequences from a text file and stores them in a DnaData object.
    """

    def extract(self, paths: ValidPaths) -> Tuple[dict, DNAData]:
        """
        Coordinates the extraction of both metadata and DNA data from specified file paths.

        :param paths: ValidPaths object containing file paths for metadata and DNA data
        :return: Tuple containing metadata dictionary and DnaData object with sequences
        """
        metadata = self._extract_metadata(paths.metadata_path)
        dna_data = self._extract_dna(paths.dna_path)  ## assuming DNA data is valid.
        return metadata, dna_data

    def _extract_metadata(self, metadata_path: str) -> dict:
        """
        Extract metadata from a JSON file.

        ** Assume the file path exists

        :param metadata_path: Path to the JSON metadata file
        :return: Parsed metadata as a dictionary
        """
        with open(metadata_path, "r") as metadata_file:
            metadata = json.load(metadata_file)
            return metadata

    def _extract_dna(self, dna_file_path: str) -> DNAData:
        """
        Extract DNA sequences from a text file.

        ** Assume the file path exists.

        :param dna_file_path: Path to the text file containing DNA sequences
        :return: DNAData object containing the parsed DNA sequences
        """
        dna_data = DNAData()
        with open(dna_file_path, "r") as dna_file:
            for line in dna_file.readlines():
                if line.strip() != "":
                    dna_data.sequences.append(line.strip())
        return dna_data
