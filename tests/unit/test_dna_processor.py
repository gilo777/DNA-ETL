import pytest
from Pipeline.Transform.DNAProcessor import DNAProcessor
from Pipeline.DataModels.DNAData import DNAData


class TestDNAProcessor:
    """Improved unit tests for the DNAProcessor component."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.processor = DNAProcessor()

    def test_analyze_sequence_gc_content_exact(self):
        """Test GC content calculation with exact percentages."""
        sequence = "ATCG"  # 2 GC out of 4 = 50%
        result = self.processor._analyze_sequence(sequence)

        assert (
            result["gc_content"] == 50.0
        )  # Implementation returns rounded to 2 decimal places
        assert "codons" in result
        assert isinstance(result["codons"], dict)

    def test_analyze_sequence_no_gc(self):
        """Test sequence with no GC content."""
        sequence = "AAATTT"
        result = self.processor._analyze_sequence(sequence)

        assert result["gc_content"] == 0.0
        assert isinstance(result["codons"], dict)
        # Should have codons: AAA and TTT
        expected_codons = {"AAA": 1, "TTT": 1}
        assert result["codons"] == expected_codons

    def test_analyze_sequence_all_gc(self):
        """Test sequence with 100% GC content."""
        sequence = "GCGCGC"
        result = self.processor._analyze_sequence(sequence)

        assert result["gc_content"] == 100.0
        assert isinstance(result["codons"], dict)
        # Should have codons: GCG and CGC
        expected_codons = {"GCG": 1, "CGC": 1}
        assert result["codons"] == expected_codons

    def test_analyze_sequence_codons_counting(self):
        """Test accurate codon counting in sequences."""
        sequence = "ATCGCGATC"  # Should have ATC:2, GCG:1
        result = self.processor._analyze_sequence(sequence)

        expected_codons = {"ATC": 2, "GCG": 1}
        assert result["codons"] == expected_codons
        assert len(result["codons"]) == 2

    def test_analyze_sequence_short_sequence(self):
        """Test handling of sequences shorter than 3 nucleotides."""
        sequence = "AT"
        result = self.processor._analyze_sequence(sequence)

        assert result["gc_content"] == 0.0  # No G or C
        assert result["codons"] == {}  # No complete codons

    def test_longest_common_subsequence_found(self):
        """Test LCS finding with common subsequences."""
        seq1 = "ATCGCAT"
        seq2 = "AGCGTAT"

        result = self.processor._longest_common_subsequence(seq1, seq2)

        assert isinstance(result, str)
        assert result == 'CG'
        # Verify it's actually a subsequence of both
        for char in result:
            assert char in seq1
            assert char in seq2

    def test_longest_common_subsequence_none(self):
        """Test LCS with no common subsequences."""
        seq1 = "AAAA"
        seq2 = "TTTT"

        result = self.processor._longest_common_subsequence(seq1, seq2)

        assert result == ""

    def test_longest_common_subsequence_identical(self):
        """Test LCS with identical sequences."""
        sequence = "ATCG"
        result = self.processor._longest_common_subsequence(sequence, sequence)

        assert result == sequence

    def test_find_lcs_multiple_sequences(self, sample_dna_sequences):
        """Test LCS finding across multiple sequences."""
        dna_data = DNAData()
        dna_data.sequences = sample_dna_sequences

        result = self.processor._find_lcs(dna_data)

        assert "value" in result
        assert "sequences" in result
        assert "length" in result
        assert isinstance(result["sequences"], list)
        assert isinstance(result["length"], int)
        assert result["length"] == len(result["value"])
        assert result["sequences"] == [1,4]
        assert result["value"] == 'CGCG'


    def test_find_lcs_single_sequence(self):
        """Test LCS with only one sequence."""
        dna_data = DNAData()
        dna_data.sequences = ["ATCG"]

        result = self.processor._find_lcs(dna_data)

        assert result["value"] == ""
        assert result["sequences"] == []
        assert result["length"] == 0

    def test_find_lcs_no_common(self):
        """Test LCS with no common subsequences."""
        dna_data = DNAData()
        dna_data.sequences = ["AAAA", "TTTT", "CCCC"]

        result = self.processor._find_lcs(dna_data)

        assert result["value"] == ""
        assert result["sequences"] == []
        assert result["length"] == 0

    def test_find_lcs_empty_sequences(self):
        """Test LCS with empty sequence list."""
        dna_data = DNAData()
        dna_data.sequences = []

        result = self.processor._find_lcs(dna_data)

        assert result["value"] == ""
        assert result["sequences"] == []
        assert result["length"] == 0

    def test_transform_dna_complete(self, sample_dna_sequences):
        """Test complete DNA transformation pipeline."""
        dna_data = DNAData()
        dna_data.sequences = sample_dna_sequences

        result = self.processor.transform_dna(dna_data)

        # Verify structure
        assert "sequences" in result
        assert "most_common_codon" in result
        assert "lcs" in result

        # Verify data types
        assert isinstance(result["sequences"], list)
        assert isinstance(result["most_common_codon"], str)
        assert isinstance(result["lcs"], dict)

        # Verify sequence processing
        assert len(result["sequences"]) == len(sample_dna_sequences)

        for seq_result in result["sequences"]:
            assert "gc_content" in seq_result
            assert "codons" in seq_result
            assert isinstance(seq_result["gc_content"], (int, float))
            assert isinstance(seq_result["codons"], dict)

    def test_global_codon_frequencies(self):
        """Test global codon frequency tracking across multiple calls."""
        processor = DNAProcessor()

        # First batch
        dna_data1 = DNAData()
        dna_data1.sequences = ["ATCATC"]  # ATC appears twice
        processor.transform_dna(dna_data1)

        # Second batch
        dna_data2 = DNAData()
        dna_data2.sequences = ["ATCGCG"]  # ATC appears once, GCG appears once
        result = processor.transform_dna(dna_data2)

        # ATC should be most common (3 total: 2 from first + 1 from second)
        # GCG appears only once
        assert result["most_common_codon"] == "ATC"

    def test_transform_dna_preserves_input(self, sample_dna_sequences):
        """Test that transformation doesn't modify input data."""
        dna_data = DNAData()
        original_sequences = sample_dna_sequences.copy()
        dna_data.sequences = sample_dna_sequences

        self.processor.transform_dna(dna_data)

        # Verify original data is unchanged
        assert dna_data.sequences == original_sequences

    def test_gc_content_edge_cases(self):
        """Test GC content calculation with various edge cases."""
        test_cases = [
            ("GCGC", 100.0),  # All GC: 4/4 = 100%
            ("ATAT", 0.0),  # No GC: 0/4 = 0%
            ("ATGC", 50.0),  # Half GC: 2/4 = 50%
            ("GGGCCCAAATTT", 50.0),  # Mixed sequence: 6/12 = 50%
        ]

        for sequence, expected_gc in test_cases:
            processor = DNAProcessor()  # Fresh processor for each test
            result = processor._analyze_sequence(sequence)
            assert (
                result["gc_content"] == expected_gc
            ), f"Failed for sequence {sequence}: got {result['gc_content']}, expected {expected_gc}"

    def test_codon_counting_edge_cases(self):
        """Test codon counting with various sequence lengths."""
        test_cases = [
            ("ATCATCATC", {"ATC": 3}),  # Perfect triplets
            ("ATCATCA", {"ATC": 2}),  # Remainder ignored (TCA incomplete)
            ("AT", {}),  # Too short
        ]

        for sequence, expected_codons in test_cases:
            # Reset processor to avoid global state contamination
            processor = DNAProcessor()
            result = processor._analyze_sequence(sequence)
            assert (
                result["codons"] == expected_codons
            ), f"Failed for sequence {sequence}"
