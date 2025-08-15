import pytest
import tempfile
import json
import os
from pathlib import Path
from Pipeline.ETLOrchestrator import ETLOrchestrator
from Constants import VALID_INPUT_KEYS


class TestETLOrchestrator:
    """Integration tests for the ETL Orchestrator."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.orchestrator = ETLOrchestrator()

    def test_orchestrate_valid_pipeline_success(self, temp_directory_with_files, temp_input_file, cleanup_files):
        """Test complete pipeline execution with valid data."""
        # Setup
        temp_data = temp_directory_with_files
        output_file = "/tmp/test_output.json"
        input_file = temp_input_file(temp_data["participant_dir"], output_file)
        cleanup_files(input_file)
        cleanup_files(output_file)
        
        # Execute
        status_code, message = self.orchestrator.orchestrate(input_file)
        
        # Assert
        assert status_code == 0
        assert temp_data["participant_id"] in message
        assert "Pipline completed for participant ID:" in message
        assert Path(output_file).exists()

    def test_orchestrate_missing_input_file(self):
        """Test pipeline with non-existent input file."""
        status_code, message = self.orchestrator.orchestrate("/nonexistent/path.json")
        
        assert status_code == 1
        assert "Input validation failed" in message or "does not exist" in message

    def test_orchestrate_invalid_json_format(self, cleanup_files):
        """Test pipeline with malformed JSON input."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            invalid_json_path = f.name
        
        cleanup_files(invalid_json_path)
        
        status_code, message = self.orchestrator.orchestrate(invalid_json_path)
        
        assert status_code == 1
        assert isinstance(message, str)

    def test_orchestrate_missing_required_keys(self, cleanup_files):
        """Test pipeline with input missing required keys."""
        input_data = {"wrong_key": "value"}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(input_data, f)
            input_path = f.name
        
        cleanup_files(input_path)
        
        status_code, message = self.orchestrator.orchestrate(input_path)
        
        assert status_code == 1
        assert "Input file don't match valid keys" in message or "Invalid input keys" in message or "required keys" in message

    def test_orchestrate_invalid_participant_directory(self, sample_uuid, temp_input_file, cleanup_files):
        """Test pipeline with non-existent participant directory."""
        fake_dir = f"/nonexistent/{sample_uuid}"
        output_file = "/tmp/test_output.json"
        input_file = temp_input_file(fake_dir, output_file)
        cleanup_files(input_file)
        
        status_code, message = self.orchestrator.orchestrate(input_file)
        
        assert status_code == 1
        assert "does not exist" in message

    def test_orchestrate_missing_dna_files(self, sample_uuid, temp_input_file, cleanup_files):
        """Test pipeline with missing DNA data files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            participant_dir = Path(temp_dir) / sample_uuid
            participant_dir.mkdir()
            
            output_file = "/tmp/test_output.json"
            input_file = temp_input_file(str(participant_dir), output_file)
            cleanup_files(input_file)
            cleanup_files(output_file)
            
            status_code, message = self.orchestrator.orchestrate(input_file)
            
            assert status_code == 1
            assert "does not exist" in message

    def test_orchestrate_invalid_metadata_age(self, temp_directory_with_files, temp_input_file, cleanup_files):
        """Test pipeline with metadata validation failure (age too young)."""
        temp_data = temp_directory_with_files
        
        # Modify metadata to have invalid age
        invalid_metadata = {
            "participant_id": "test-123",
            "name": "Too Young",
            "date_of_birth": "2010-01-01",  # Too young
            "age": 13,
            "location": "Boston"
        }
        
        metadata_file = Path(temp_data["metadata_file"])
        with open(metadata_file, 'w') as f:
            json.dump(invalid_metadata, f)
        
        output_file = "/tmp/test_output.json"
        input_file = temp_input_file(temp_data["participant_dir"], output_file)
        cleanup_files(input_file)
        cleanup_files(output_file)
        
        status_code, message = self.orchestrator.orchestrate(input_file)
        
        assert status_code == 1
        assert "age" in message.lower() or "young" in message.lower()

    def test_orchestrate_timing_capture(self, temp_directory_with_files, temp_input_file, cleanup_files):
        """Test that pipeline captures processing timing correctly."""
        temp_data = temp_directory_with_files
        output_file = "/tmp/test_output_timing.json"
        input_file = temp_input_file(temp_data["participant_dir"], output_file)
        cleanup_files(input_file)
        cleanup_files(output_file)
        
        status_code, message = self.orchestrator.orchestrate(input_file)
        
        assert status_code == 0
        
        # Check that output file contains timing information
        with open(output_file, 'r') as f:
            output_data = json.load(f)
        
        # Timing is in metadata section with different key names
        assert "start_at" in output_data["metadata"]
        assert "end_at" in output_data["metadata"]

    def test_orchestrate_metadata_privacy_removal(self, temp_directory_with_files, temp_input_file, cleanup_files, sample_metadata_with_private):
        """Test that private metadata keys are removed in output."""
        temp_data = temp_directory_with_files
        
        # Use metadata with private keys
        metadata_file = Path(temp_data["metadata_file"])
        with open(metadata_file, 'w') as f:
            json.dump(sample_metadata_with_private, f)
        
        output_file = "/tmp/test_output_privacy.json"
        input_file = temp_input_file(temp_data["participant_dir"], output_file)
        cleanup_files(input_file)
        cleanup_files(output_file)
        
        status_code, message = self.orchestrator.orchestrate(input_file)
        
        assert status_code == 0
        
        # Check that private keys are removed from output
        with open(output_file, 'r') as f:
            output_data = json.load(f)
        
        metadata_str = json.dumps(output_data)
        assert "_ssn" not in metadata_str
        assert "_internal_id" not in metadata_str
        assert "_phone" not in metadata_str