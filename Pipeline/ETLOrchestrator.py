from datetime import datetime
from Constants import VALID_INPUT_KEYS
from Pipeline.DataExtractor import DataExtractor
from Pipeline.MetaDataValidator import MetaDataValidator
from Pipeline.InputValidator import InputValidator
from Pipeline.Transform.DNAProcessor import DNAProcessor
from Pipeline.Transform.MetaDataProcessor import MetaDataProcessor
from Pipeline.Loader import Loader

class ETLOrchestrator:
    """
    Coordinates a multi-stage data processing pipeline for genetic data analysis.

    The Orchestrator manages the complete workflow from input validation through
    data transformation to output generation. It processes DNA sequences and
    associated metadata while ensuring data integrity and tracking processing metrics.

    Attributes:
        input_validator (InputValidator): Validates input file paths and structure
        data_extractor (DataExtractor): Extracts data from validated input files
        DNA_processor (DNAProcessor): Transforms and processes DNA sequence data
        MetaData_processor (MetaDataProcessor): Processes and sanitizes metadata
        loader (Loader): Handles output file generation and data persistence
        metadata_validator (MetaDataValidator): Validates metadata content.

    Methods:
          orchestrate(input_path : str) -> str:
             Executes the complete data processing pipeline for a given input path.
    """
    def __init__(self):
        self.input_validator = InputValidator(valid_keys = VALID_INPUT_KEYS)
        self.data_extractor = DataExtractor()
        self.DNA_processor = DNAProcessor()
        self.MetaData_processor = MetaDataProcessor()
        self.loader = Loader()
        self.metadata_validator = MetaDataValidator()

    def orchestrate(self, input_path : str) -> str:
        """
        The orchestrate method executes a complete genetic data processing pipeline that validates,
        extracts, transforms, and outputs DNA sequence data and metadata
        while tracking processing time and participant information.

        :param input_path(str): Path to the input directory containing paths to the input data files and output file.

        :return:
            str: Success message containing the participant ID if processing
                succeeds, or failure message if pipeline encounters errors
        """
        # Validate input.
        validation_result = self.input_validator.validate(input_path)
        if validation_result is None:
            raise Exception(f"Input validation failed for path: {input_path}")
        verified_paths, participant_id = validation_result
        # Capture time when started processing
        start_time = datetime.now()
        # Extract data
        metadata, dna_data = self.data_extractor.extract(verified_paths)
        # Validate metadata.
        if not self.metadata_validator.validate_metadata(metadata):
            raise Exception("metadata file not valid")
        # Metadata transformation
        transformed_metadata = self.MetaData_processor.remove_private_keys(metadata)
        # DNA data transformation
        transformed_dna = self.DNA_processor.transform_dna(dna_data)
        # Capture time when finished processing.
        end_time = datetime.now()
        # Generate output file, return participant ID for documentation.
        output =  self.loader.create_output(transformed_metadata, transformed_dna, verified_paths, start_time, end_time, participant_id)
        if(output is None):
            return f"pipline failed"
        else:
            return f"Participant ID: {participant_id}"
