
from datetime import datetime


from Pipeline.DataModels_and_Constants.Constants import ValidInputKeys
from Pipeline.Extract.Data_Extractor import Data_Extractor
from Pipeline.InputValidator import InputValidator
from Pipeline.Transform.DNAProcessor import DNAProcessor
from Pipeline.Transform.MetaDataProcessor import MetaDataProcessor
from Pipeline.Loader import Loader

class Orchestrator:

    def __init__(self):
        self.input_validator = InputValidator(valid_keys = ValidInputKeys)
        self.data_extractor = Data_Extractor()
        self.DNA_processor = DNAProcessor()
        self.MetaData_processor = MetaDataProcessor()
        self.loader = Loader()

    def orchestrate(self, input_path : str) -> str:
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
        meta_data, dna_data = self.data_extractor.extract(verified_paths)

        ## meta transformation
        transformed_metadata = self.MetaData_processor.remove_private_keys(meta_data)

        ## dna transformation
        transformed_dna = self.DNA_processor.transform_dna(dna_data)

        end_time = datetime.now()

        output =  self.loader.load(transformed_metadata,transformed_dna, verified_paths, start_time, end_time, participant_id)
        if(output is None):
            return f"pipline failed"
        else:
            return output
