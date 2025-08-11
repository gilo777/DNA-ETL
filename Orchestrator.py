import time
from datetime import datetime
from unittest import loader

from Constants import ValidInputKeys
from Extructor import Extructor
from InputValidator import InputValidator
from Transformer import Transformer
from Loader import Loader

class Orchestrator:

    def __init__(self):
        self.extractor =  Extructor()
        self.input_validator = InputValidator(valid_keys = ValidInputKeys)
        self.file_extractor = Extructor()
        self.transformer = Transformer()
        self.loader = Loader()

    def orchestrate(self, input_path) -> str:
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
        verified_paths, participant_id = self.input_validator.validate(input_path)
        if verified_paths is None:
            raise Exception("File {verified_paths} could not be verified")

        start_time = time.time()

        ## data extruction
        meta_data, dna_data = self.file_extractor.extract(verified_paths)

        ## meta transformation
        transformed_metadata = self.transformer.remove_private_keys(meta_data)

        ## dna transformation
        transformed_dna = self.transformer.transform_dna(dna_data)

        end_time = time.time()

        return self.loader.load(transformed_metadata,transformed_dna, verified_paths, start_time, end_time, participant_id)
