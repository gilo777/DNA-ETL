import pytest
import json
import tempfile
import os
from pathlib import Path
from Pipeline.DataExtractor import DataExtractor
from Pipeline.DataModels.ValidPaths import ValidPaths
from Pipeline.DataModels.DNAData import DNAData


class TestDataExtractor:
    """Unit tests for the DataExtractor component."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.extractor = DataExtractor()

    def test_extract_metadata_valid_json(self, cleanup_files):
        """Test extraction of valid JSON metadata."""
        test_metadata = {
            "participant_id": "test-123",
            "name": "John Doe",
            "age": 45,
            "date_of_birth": "1980-05-15",
            "location": "Boston",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_metadata, f)
            metadata_path = f.name

        cleanup_files(metadata_path)

        result = self.extractor._extract_metadata(metadata_path)

        assert result == test_metadata
        assert isinstance(result, dict)
        assert result["participant_id"] == "test-123"
        assert result["age"] == 45

    def test_extract_metadata_complex_nested_json(self, cleanup_files):
        """Test extraction of complex nested JSON metadata."""
        complex_metadata = {
            "participant": {
                "id": "complex-test-456",
                "demographics": {
                    "age": 35,
                    "gender": "F",
                    "location": {"city": "Seattle", "state": "WA", "zip": "98101"},
                },
                "medical_history": {
                    "allergies": ["peanuts", "shellfish"],
                    "medications": [],
                    "conditions": ["hypertension"],
                },
            },
            "study": {"id": "DNA-STUDY-001", "phase": 2, "enrolled_date": "2024-01-15"},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(complex_metadata, f)
            metadata_path = f.name

        cleanup_files(metadata_path)

        result = self.extractor._extract_metadata(metadata_path)

        assert result == complex_metadata
        assert result["participant"]["demographics"]["age"] == 35
        assert "peanuts" in result["participant"]["medical_history"]["allergies"]
        assert result["study"]["phase"] == 2

    def test_extract_metadata_empty_json(self, cleanup_files):
        """Test extraction of empty JSON metadata."""
        empty_metadata = {}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(empty_metadata, f)
            metadata_path = f.name

        cleanup_files(metadata_path)

        result = self.extractor._extract_metadata(metadata_path)

        assert result == {}
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_extract_dna_valid_sequences(self, cleanup_files):
        """Test extraction of valid DNA sequences from text file."""
        test_sequences = [
            "ATCGCGATCGTAGCTA",
            "GCTAGCTAGCTAGCTA",
            "TTAATTAATTAATTAA",
            "CGCGCGCGCGCGCGCG",
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            for seq in test_sequences:
                f.write(seq + "\n")
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert result.sequences == test_sequences
        assert len(result.sequences) == 4

        # Verify each sequence is properly extracted
        for i, expected_seq in enumerate(test_sequences):
            assert result.sequences[i] == expected_seq

    def test_extract_dna_with_empty_lines(self, cleanup_files):
        """Test DNA extraction handles empty lines correctly."""
        dna_content = "ATCGCGATCG\n\nGCTAGCTAGC\n\n\nTTAATTAAGG\n"
        expected_sequences = ["ATCGCGATCG", "GCTAGCTAGC", "TTAATTAAGG"]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(dna_content)
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert result.sequences == expected_sequences
        assert len(result.sequences) == 3

    def test_extract_dna_with_whitespace(self, cleanup_files):
        """Test DNA extraction handles whitespace correctly."""
        dna_content = "  ATCGCGATCG  \n\t\tGCTAGCTAGC\t\n   TTAATTAAGG   \n"
        expected_sequences = ["ATCGCGATCG", "GCTAGCTAGC", "TTAATTAAGG"]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(dna_content)
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert result.sequences == expected_sequences

        # Verify no whitespace remains
        for seq in result.sequences:
            assert seq == seq.strip()

    def test_extract_dna_empty_file(self, cleanup_files):
        """Test DNA extraction from empty file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            # Write nothing to file
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert result.sequences == []
        assert len(result.sequences) == 0

    def test_extract_dna_only_empty_lines(self, cleanup_files):
        """Test DNA extraction from file with only empty lines."""
        dna_content = "\n\n\n\t\t\n   \n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(dna_content)
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert result.sequences == []
        assert len(result.sequences) == 0

    def test_extract_dna_single_sequence(self, cleanup_files):
        """Test DNA extraction with single sequence."""
        single_sequence = "ATCGCGATCGTAGCTACGCGCGCG"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(single_sequence + "\n")
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert result.sequences == [single_sequence]
        assert len(result.sequences) == 1

    def test_extract_coordinated_extraction(
        self, temp_metadata_file, temp_dna_file, cleanup_files
    ):
        """Test coordinated extraction of both metadata and DNA data."""
        cleanup_files(temp_metadata_file)
        cleanup_files(temp_dna_file)

        paths = ValidPaths(
            dna_path=temp_dna_file,
            metadata_path=temp_metadata_file,
            context_path="/tmp/test",
            output_path="/tmp/test_output.json",
        )

        metadata, dna_data = self.extractor.extract(paths)

        # Verify metadata extraction
        assert isinstance(metadata, dict)
        assert "participant_id" in metadata
        assert metadata["name"] == "John Doe"

        # Verify DNA data extraction
        assert isinstance(dna_data, DNAData)
        assert len(dna_data.sequences) > 0
        assert all(isinstance(seq, str) for seq in dna_data.sequences)

    def test_extract_with_real_fixtures(self, cleanup_files):
        """Test extraction using real fixture files."""
        # Use actual fixture files
        fixtures_dir = Path(__file__).parent.parent / "fixtures"
        metadata_path = fixtures_dir / "sample_metadata.json"
        dna_path = fixtures_dir / "sample_dna.txt"

        paths = ValidPaths(
            dna_path=str(dna_path),
            metadata_path=str(metadata_path),
            context_path="/tmp/test",
            output_path="/tmp/test_output.json",
        )

        metadata, dna_data = self.extractor.extract(paths)

        # Verify metadata structure
        assert isinstance(metadata, dict)
        assert "participant_id" in metadata
        assert "name" in metadata
        assert "date_of_birth" in metadata

        # Verify DNA data structure
        assert isinstance(dna_data, DNAData)
        assert len(dna_data.sequences) > 0

        # Verify sequences contain valid DNA characters
        valid_chars = set("ATCG")
        for sequence in dna_data.sequences:
            assert all(
                char in valid_chars for char in sequence
            ), f"Invalid characters in sequence: {sequence}"

    def test_extract_preserves_data_types_in_metadata(self, cleanup_files):
        """Test that metadata extraction preserves various data types."""
        test_metadata = {
            "participant_id": "type-test-789",
            "age": 42,  # int
            "height": 5.75,  # float
            "is_active": True,  # bool
            "allergies": ["peanuts", "eggs"],  # list
            "scores": [85, 90, 78],  # list of ints
            "metadata": {"study_phase": 2, "enrolled": True},  # nested dict
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_metadata, f)
            metadata_path = f.name

        cleanup_files(metadata_path)

        result = self.extractor._extract_metadata(metadata_path)

        # Verify data types are preserved
        assert isinstance(result["age"], int)
        assert isinstance(result["height"], float)
        assert isinstance(result["is_active"], bool)
        assert isinstance(result["allergies"], list)
        assert isinstance(result["scores"], list)
        assert isinstance(result["metadata"], dict)
        assert isinstance(result["metadata"]["study_phase"], int)
        assert isinstance(result["metadata"]["enrolled"], bool)

    def test_extract_large_dna_file(self, cleanup_files):
        """Test extraction of larger DNA files."""
        # Generate many sequences
        large_sequences = [f"ATCG{'CGAT' * 25}{i:04d}" for i in range(100)]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            for seq in large_sequences:
                f.write(seq + "\n")
            dna_path = f.name

        cleanup_files(dna_path)

        result = self.extractor._extract_dna(dna_path)

        assert isinstance(result, DNAData)
        assert len(result.sequences) == 100
        assert result.sequences[0].endswith("0000")
        assert result.sequences[99].endswith("0099")

    def test_extract_metadata_with_unicode_characters(self, cleanup_files):
        """Test metadata extraction with unicode characters."""
        unicode_metadata = {
            "participant_name": "José María García",
            "location": "São Paulo",
            "notes": "Patient is très bien",
            "special_chars": "αβγδε",
            "emoji": "🧬 DNA study participant",
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            json.dump(unicode_metadata, f, ensure_ascii=False)
            metadata_path = f.name

        cleanup_files(metadata_path)

        result = self.extractor._extract_metadata(metadata_path)

        assert result == unicode_metadata
        assert result["participant_name"] == "José María García"
        assert result["location"] == "São Paulo"
        assert "🧬" in result["emoji"]
