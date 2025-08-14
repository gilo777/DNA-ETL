import unittest
import json
import tempfile
import os
from Pipeline.DataExtractor import DataExtractor
from Pipeline.DataModels.ValidPaths import ValidPaths
from Pipeline.DataModels.DNAData import DNAData


class TestDataExtractor(unittest.TestCase):

    def setUp(self):
        self.extractor = DataExtractor()

    def test_extract_metadata_valid_json(self):
        test_metadata = {"participant_id": "123", "age": 45}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_metadata, f)
            metadata_path = f.name

        try:
            result = self.extractor._extract_metadata(metadata_path)
            self.assertEqual(result, test_metadata)
        finally:
            os.unlink(metadata_path)

    def test_extract_dna_valid_file(self):
        test_sequences = ["ATCG", "GCTA", "TTAA"]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            for seq in test_sequences:
                f.write(seq + '\n')
            dna_path = f.name

        try:
            result = self.extractor._extract_dna(dna_path)
            self.assertIsInstance(result, DNAData)
            self.assertEqual(result.sequences, test_sequences)
        finally:
            os.unlink(dna_path)

    def test_extract_dna_empty_lines(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("ATCG\n\nGCTA\n")
            dna_path = f.name

        try:
            result = self.extractor._extract_dna(dna_path)
            self.assertEqual(len(result.sequences), 2)
            self.assertEqual(result.sequences, ["ATCG", "GCTA"])
        finally:
            os.unlink(dna_path)

    def test_extract_full_pipeline(self):
        test_metadata = {"participant": "test123"}
        test_sequences = ["ATCG", "GCTA"]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as meta_f:
            json.dump(test_metadata, meta_f)
            metadata_path = meta_f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as dna_f:
            for seq in test_sequences:
                dna_f.write(seq + '\n')
            dna_path = dna_f.name

        try:
            paths = ValidPaths(dna_path, metadata_path, "/tmp", "/tmp/output")
            metadata, dna_data = self.extractor.extract(paths)

            self.assertEqual(metadata, test_metadata)
            self.assertIsInstance(dna_data, DNAData)
            self.assertEqual(dna_data.sequences, test_sequences)
        finally:
            os.unlink(metadata_path)
            os.unlink(dna_path)


if __name__ == '__main__':
    unittest.main()