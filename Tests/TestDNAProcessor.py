import unittest
from Pipeline.Transform.DNAProcessor import DNAProcessor
from Pipeline.DataModels.DNAData import DNAData


class TestDNAProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = DNAProcessor()

    def test_analyze_sequence_gc_content(self):
        sequence = "ATCG"
        result = self.processor._analyze_sequence(sequence)

        self.assertEqual(result["gc_content"], 50.0)
        self.assertIn("codons", result)

    def test_analyze_sequence_no_gc(self):
        sequence = "AAATTT"
        result = self.processor._analyze_sequence(sequence)

        self.assertEqual(result["gc_content"], 0.0)

    def test_analyze_sequence_all_gc(self):
        sequence = "GCGCGC"
        result = self.processor._analyze_sequence(sequence)

        self.assertEqual(result["gc_content"], 100.0)

    def test_analyze_sequence_codons(self):
        sequence = "ATCGCGATC"
        result = self.processor._analyze_sequence(sequence)

        expected_codons = {"ATC": 2, "GCG": 1}
        self.assertEqual(result["codons"], expected_codons)

    def test_longest_common_subsequence_found(self):
        seq1 = "ATCGCAT"
        seq2 = "AGCGTAT"

        result = self.processor._longest_common_subsequence(seq1, seq2)

        self.assertTrue(len(result) > 0)
        self.assertIn(result, seq1)
        self.assertIn(result, seq2)

    def test_longest_common_subsequence_none(self):
        seq1 = "AAAA"
        seq2 = "TTTT"

        result = self.processor._longest_common_subsequence(seq1, seq2)

        self.assertEqual(result, "")

    def test_find_lcs_multiple_sequences(self):
        dna_data = DNAData()
        dna_data.sequences = ["ATCGCAT", "AGCGTAT", "ATCGTAT"]

        result = self.processor._find_lcs(dna_data)

        self.assertIn("value", result)
        self.assertIn("sequences", result)
        self.assertIn("length", result)
        self.assertGreater(len(result["sequences"]), 1)

    def test_find_lcs_single_sequence(self):
        dna_data = DNAData()
        dna_data.sequences = ["ATCG"]

        result = self.processor._find_lcs(dna_data)

        self.assertEqual(result["value"], "")
        self.assertEqual(result["sequences"], [])
        self.assertEqual(result["length"], 0)

    def test_find_lcs_no_common(self):
        dna_data = DNAData()
        dna_data.sequences = ["AAAA", "TTTT", "CCCC"]

        result = self.processor._find_lcs(dna_data)

        self.assertEqual(result["value"], "")
        self.assertEqual(result["sequences"], [])
        self.assertEqual(result["length"], 0)

    def test_transform_dna_complete(self):
        dna_data = DNAData()
        dna_data.sequences = ["ATCGCG", "GCGATC"]

        result = self.processor.transform_dna(dna_data)

        self.assertIn("sequences", result)
        self.assertIn("most_common_codon", result)
        self.assertIn("lcs", result)
        self.assertEqual(len(result["sequences"]), 2)

        for seq_result in result["sequences"]:
            self.assertIn("gc_content", seq_result)
            self.assertIn("codons", seq_result)

    def test_global_codon_frequencies(self):
        processor = DNAProcessor()

        dna_data1 = DNAData()
        dna_data1.sequences = ["ATCATC"]
        processor.transform_dna(dna_data1)

        dna_data2 = DNAData()
        dna_data2.sequences = ["ATCGCG"]
        result = processor.transform_dna(dna_data2)

        self.assertEqual(result["most_common_codon"], "ATC")


if __name__ == '__main__':
    unittest.main()