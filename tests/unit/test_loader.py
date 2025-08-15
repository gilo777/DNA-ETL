import pytest
import json
import tempfile
import os
from datetime import datetime
from pathlib import Path
from Pipeline.Loader import Loader
from Pipeline.DataModels.ValidPaths import ValidPaths
from Exceptions.LoaderExceptions import LoaderException


class TestLoader:
    """Unit tests for the Loader component."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.loader = Loader()

    def test_create_output_success(self, sample_metadata, sample_dna_sequences, cleanup_files):
        """Test successful output file creation."""
        # Setup
        start_time = datetime.now()
        end_time = datetime.now()
        participant_id = "test-participant-123"
        
        # Create temporary output file path
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        cleanup_files(output_path)
        
        # Create sample DNA data structure (as returned by DNAProcessor)
        dna_data = {
            "sequences": [
                {"sequence": seq, "gc_content": 50.0, "codons": {"ATG": 1}}
                for seq in sample_dna_sequences
            ],
            "most_common_codon": "ATG",
            "lcs": {"value": "ATG", "length": 3, "sequences": ["seq1", "seq2"]}
        }
        
        paths = ValidPaths(
            dna_path="/tmp/test_dna.txt",
            metadata_path="/tmp/test_metadata.json",
            context_path="/tmp/test_context",
            output_path=output_path
        )
        
        # Execute
        self.loader.create_output(
            meta_data=sample_metadata,
            dna_data=dna_data,
            paths=paths,
            start_time=start_time,
            end_time=end_time,
            participant_id=participant_id
        )
        
        # Assert
        assert Path(output_path).exists()
        
        with open(output_path, 'r') as f:
            output_data = json.load(f)
        
        # Verify structure
        assert "metadata" in output_data
        assert "results" in output_data
        
        # Verify metadata section
        metadata = output_data["metadata"]
        assert "start_at" in metadata
        assert "end_at" in metadata
        assert "context_path" in metadata
        assert "results_path" in metadata
        assert metadata["context_path"] == str(paths.context_path)
        assert metadata["results_path"] == str(paths.output_path)
        
        # Verify results section
        results = output_data["results"]
        assert len(results) == 1
        
        result = results[0]
        assert "participant" in result
        assert "txt" in result
        assert "json" in result
        
        # Verify participant ID
        assert result["participant"]["_id"] == participant_id
        
        # Verify DNA data is preserved
        assert result["txt"] == dna_data
        
        # Verify metadata is preserved
        assert result["json"] == sample_metadata

    def test_create_output_json_formatting(self, sample_metadata, cleanup_files):
        """Test that output JSON is properly formatted with indentation."""
        start_time = datetime.now()
        end_time = datetime.now()
        participant_id = "test-format-123"
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        cleanup_files(output_path)
        
        dna_data = {"sequences": [], "most_common_codon": "", "lcs": {}}
        paths = ValidPaths("/tmp/dna", "/tmp/meta", "/tmp/context", output_path)
        
        self.loader.create_output(
            meta_data=sample_metadata,
            dna_data=dna_data,
            paths=paths,
            start_time=start_time,
            end_time=end_time,
            participant_id=participant_id
        )
        
        # Read raw file content to check formatting
        with open(output_path, 'r') as f:
            content = f.read()
        
        # Should have proper indentation (4 spaces)
        assert "    " in content  # Check for indentation
        assert content.count('\n') > 5  # Should have multiple lines due to formatting

    def test_create_output_invalid_path_raises_exception(self, sample_metadata):
        """Test that invalid output path raises LoaderException."""
        start_time = datetime.now()
        end_time = datetime.now()
        participant_id = "test-error-123"
        
        # Use invalid path (directory doesn't exist)
        invalid_output_path = "/nonexistent/directory/output.json"
        
        dna_data = {"sequences": [], "most_common_codon": "", "lcs": {}}
        paths = ValidPaths("/tmp/dna", "/tmp/meta", "/tmp/context", invalid_output_path)
        
        with pytest.raises(LoaderException):
            self.loader.create_output(
                meta_data=sample_metadata,
                dna_data=dna_data,
                paths=paths,
                start_time=start_time,
                end_time=end_time,
                participant_id=participant_id
            )

    def test_create_output_permission_error_raises_exception(self, sample_metadata):
        """Test that permission errors raise LoaderException."""
        start_time = datetime.now()
        end_time = datetime.now()
        participant_id = "test-permission-123"
        
        # Use read-only directory path
        readonly_path = "/test_readonly_output.json"  # This should fail on most systems
        
        dna_data = {"sequences": [], "most_common_codon": "", "lcs": {}}
        paths = ValidPaths("/tmp/dna", "/tmp/meta", "/tmp/context", readonly_path)
        
        with pytest.raises(LoaderException):
            self.loader.create_output(
                meta_data=sample_metadata,
                dna_data=dna_data,
                paths=paths,
                start_time=start_time,
                end_time=end_time,
                participant_id=participant_id
            )

    def test_create_output_timing_precision(self, sample_metadata, cleanup_files):
        """Test that timing information is captured accurately."""
        participant_id = "test-timing-123"
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        cleanup_files(output_path)
        
        # Use specific timestamps
        start_time = datetime(2024, 1, 15, 10, 30, 45)
        end_time = datetime(2024, 1, 15, 10, 35, 20)
        
        dna_data = {"sequences": [], "most_common_codon": "", "lcs": {}}
        paths = ValidPaths("/tmp/dna", "/tmp/meta", "/tmp/context", output_path)
        
        self.loader.create_output(
            meta_data=sample_metadata,
            dna_data=dna_data,
            paths=paths,
            start_time=start_time,
            end_time=end_time,
            participant_id=participant_id
        )
        
        with open(output_path, 'r') as f:
            output_data = json.load(f)
        
        # Verify timestamps are correctly formatted
        metadata = output_data["metadata"]
        assert str(start_time) in metadata["start_at"]
        assert str(end_time) in metadata["end_at"]

    def test_create_output_large_data_handling(self, cleanup_files):
        """Test handling of large datasets."""
        participant_id = "test-large-123"
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        cleanup_files(output_path)
        
        # Create large dataset
        large_metadata = {f"field_{i}": f"value_{i}" for i in range(1000)}
        large_dna_data = {
            "sequences": [
                {"sequence": "ATCG" * 100, "gc_content": 50.0, "codons": {"ATG": 50}}
                for _ in range(100)
            ],
            "most_common_codon": "ATG",
            "lcs": {"value": "ATCG", "length": 4, "sequences": [f"seq_{i}" for i in range(50)]}
        }
        
        start_time = datetime.now()
        end_time = datetime.now()
        paths = ValidPaths("/tmp/dna", "/tmp/meta", "/tmp/context", output_path)
        
        # Should not raise any exceptions
        self.loader.create_output(
            meta_data=large_metadata,
            dna_data=large_dna_data,
            paths=paths,
            start_time=start_time,
            end_time=end_time,
            participant_id=participant_id
        )
        
        # Verify file exists and is readable
        assert Path(output_path).exists()
        with open(output_path, 'r') as f:
            output_data = json.load(f)
        
        assert len(output_data["results"][0]["json"]) == 1000
        assert len(output_data["results"][0]["txt"]["sequences"]) == 100

    def test_create_output_empty_data_handling(self, cleanup_files):
        """Test handling of empty or minimal data."""
        participant_id = "test-empty-123"
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        cleanup_files(output_path)
        
        # Empty data structures
        empty_metadata = {}
        empty_dna_data = {}
        
        start_time = datetime.now()
        end_time = datetime.now()
        paths = ValidPaths("/tmp/dna", "/tmp/meta", "/tmp/context", output_path)
        
        self.loader.create_output(
            meta_data=empty_metadata,
            dna_data=empty_dna_data,
            paths=paths,
            start_time=start_time,
            end_time=end_time,
            participant_id=participant_id
        )
        
        with open(output_path, 'r') as f:
            output_data = json.load(f)
        
        # Should still have proper structure
        assert "metadata" in output_data
        assert "results" in output_data
        assert output_data["results"][0]["participant"]["_id"] == participant_id
        assert output_data["results"][0]["json"] == {}
        assert output_data["results"][0]["txt"] == {}