from datetime import datetime
from Pipeline.DataModelsAndConstants.Constants import VALID_INPUT_KEYS
from Pipeline.DataExtractor import DataExtractor
from Pipeline.MetaDataValidator import MetaDataValidator
from Pipeline.InputValidator import InputValidator
from Pipeline.Transform.DNAProcessor import DNAProcessor
from Pipeline.Transform.MetaDataProcessor import MetaDataProcessor
from Pipeline.Load import Load

class Orchestrator:
    """

    """
    def __init__(self):
        self.input_validator = InputValidator(valid_keys = VALID_INPUT_KEYS)
        self.data_extractor = DataExtractor()
        self.DNA_processor = DNAProcessor()
        self.MetaData_processor = MetaDataProcessor()
        self.loader = Load()
        self.metadata_validator = MetaDataValidator()

    def orchestrate(self, input_path : str) -> str:
        """

        :param input_path:
        :return:
        """
        # input path to json with two fields
        # 1. path to input folder with two files .json (metadata) .txt (dna data)
        # 2. path to output folder

        # verify input (input folder exists and contains both files with correct convection)
        # extract input files (metadata json and dna data)
            # read dna data into list\object (assume is valid)
            # read metadata into dict\object (validate format of json)
        # proccess each file :
            # dna : count gc content....
            # meta : remove sensitive fields
        # create output files : to the output address.


         ## input path validation

        validation_result = self.input_validator.validate(input_path)
        if validation_result is None:
            raise Exception(f"Input validation failed for path: {input_path}")

        verified_paths, participant_id = validation_result
        # verified_paths, participant_id = self.input_validator.validate(input_path)

        start_time = datetime.now()

        ## data extraction
        metadata, dna_data = self.data_extractor.extract(verified_paths)

        if not self.metadata_validator.validate_metadata(metadata):
            raise Exception("metadata file not valid")


        ## meta transformation
        transformed_metadata = self.MetaData_processor.remove_private_keys(metadata)

        ## dna transformation
        transformed_dna = self.DNA_processor.transform_dna(dna_data)

        end_time = datetime.now()

        output =  self.loader.create_output(transformed_metadata, transformed_dna, verified_paths, start_time, end_time, participant_id)
        if(output is None):
            return f"pipline failed"
        else:
            return f"Participant ID: {participant_id}"
