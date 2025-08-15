import pytest
import tempfile
import json
import os
import subprocess
import sys
from pathlib import Path


@pytest.mark.skip(reason="Integration tests require full system setup - run manually if needed")
class TestMainConcurrent:
    """Integration tests for concurrent processing in Main.py."""

    def create_test_directory_with_files(self, num_files=3):
        """Create a test directory with multiple valid input files."""
        temp_dir = tempfile.mkdtemp()
        files_created = []
        
        for i in range(num_files):
            participant_id = f"test-{i:03d}-concurrent"
            
            # Create participant directory
            participant_dir = Path(temp_dir) / "participants" / participant_id
            participant_dir.mkdir(parents=True)
            
            # Create DNA file
            dna_file = participant_dir / f"{participant_id}_dna.txt"
            with open(dna_file, 'w') as f:
                f.write(f"ATCGCGATCG{i:02d}\n")
                f.write(f"GCTAGCTACG{i:02d}\n")
            
            # Create metadata file
            metadata_file = participant_dir / f"{participant_id}_dna.json"
            metadata = {
                "participant_id": participant_id,
                "name": f"Test Participant {i}",
                "date_of_birth": "1980-05-15",
                "age": 43,
                "location": "Boston"
            }
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f)
            
            # Create input file
            input_file = Path(temp_dir) / f"input_{i:03d}.json"
            output_file = os.path.join(temp_dir, f"output_{i:03d}.json")
            input_data = {
                "context_path": str(participant_dir),
                "results_path": output_file
            }
            with open(input_file, 'w') as f:
                json.dump(input_data, f)
            
            files_created.append({
                "input_file": str(input_file),
                "output_file": output_file,
                "participant_id": participant_id
            })
        
        return temp_dir, files_created

    def test_concurrent_processing_success(self):
        """Test successful concurrent processing of multiple files."""
        temp_dir, files_created = self.create_test_directory_with_files(3)
        
        try:
            # Run concurrent processing
            result = subprocess.run([
                sys.executable, "Main.py", temp_dir, "concurrent"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            # Verify exit code
            assert result.returncode == 0, f"Process failed with stderr: {result.stderr}"
            
            # Verify all output files were created
            for file_info in files_created:
                output_path = Path(file_info["output_file"])
                assert output_path.exists(), f"Output file not created: {output_path}"
                
                # Verify output file content
                with open(output_path, 'r') as f:
                    output_data = json.load(f)
                
                assert "metadata" in output_data
                assert "results" in output_data
                assert len(output_data["results"]) == 1
                assert output_data["results"][0]["participant"]["_id"] == file_info["participant_id"]
            
            # Verify summary report contains correct information
            assert "PROCESSING SUMMARY - Concurrent Mode" in result.stdout
            assert "Total files processed: 3" in result.stdout
            assert "✓ Successful: 3" in result.stdout
            assert "✗ Failed: 0" in result.stdout
            assert "Success rate: 100.0%" in result.stdout
            
        finally:
            # Cleanup
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_vs_sequential_both_work(self):
        """Test that both concurrent and sequential processing work correctly."""
        temp_dir, files_created = self.create_test_directory_with_files(3)
        
        try:
            # Test sequential processing
            result_seq = subprocess.run([
                sys.executable, "Main.py", temp_dir, "sequential"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            # Clean up output files
            for file_info in files_created:
                output_path = Path(file_info["output_file"])
                if output_path.exists():
                    output_path.unlink()
            
            # Test concurrent processing
            result_conc = subprocess.run([
                sys.executable, "Main.py", temp_dir, "concurrent"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            # Both should succeed
            assert result_seq.returncode == 0, f"Sequential failed: {result_seq.stderr}"
            assert result_conc.returncode == 0, f"Concurrent failed: {result_conc.stderr}"
            
            # Both should process same number of files
            assert "Total files processed: 3" in result_seq.stdout
            assert "Total files processed: 3" in result_conc.stdout
            
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_processing_with_failures(self):
        """Test concurrent processing handles some failures gracefully."""
        temp_dir, files_created = self.create_test_directory_with_files(3)
        
        try:
            # Corrupt one of the input files to cause a failure
            corrupted_file = files_created[1]["input_file"]
            with open(corrupted_file, 'w') as f:
                f.write("invalid json content")
            
            result = subprocess.run([
                sys.executable, "Main.py", temp_dir, "concurrent"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            # Should exit with error code due to failures
            assert result.returncode == 1
            
            # Verify summary shows mixed results
            assert "PROCESSING SUMMARY - Concurrent Mode" in result.stdout
            assert "Total files processed: 3" in result.stdout
            assert "✓ Successful: 2" in result.stdout
            assert "✗ Failed: 1" in result.stdout
            assert "Success rate: 66.7%" in result.stdout
            
            # Should list failed files
            assert "Failed files:" in result.stdout
            
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_thread_pool_size(self):
        """Test that concurrent processing uses appropriate thread pool size."""
        temp_dir, files_created = self.create_test_directory_with_files(10)
        
        try:
            result = subprocess.run([
                sys.executable, "Main.py", temp_dir, "concurrent"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            assert result.returncode == 0
            
            # Verify all files were processed
            assert "Total files processed: 10" in result.stdout
            assert "✓ Successful: 10" in result.stdout
            
            # Verify all output files exist
            for file_info in files_created:
                assert Path(file_info["output_file"]).exists()
            
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_empty_directory(self):
        """Test concurrent processing with directory containing no JSON files."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create some non-JSON files
            (Path(temp_dir) / "not_json.txt").write_text("test")
            (Path(temp_dir) / "another.xml").write_text("<test/>")
            
            result = subprocess.run([
                sys.executable, "Main.py", temp_dir, "concurrent"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            # Should fail with no JSON files found
            assert result.returncode == 1
            assert "No JSON files found" in result.stdout
            
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_processing_error_isolation(self):
        """Test that errors in one file don't affect processing of other files."""
        temp_dir, files_created = self.create_test_directory_with_files(4)
        
        try:
            # Corrupt two files in different ways
            # File 1: Invalid JSON
            with open(files_created[1]["input_file"], 'w') as f:
                f.write("invalid json")
            
            # File 2: Missing required keys
            with open(files_created[2]["input_file"], 'w') as f:
                json.dump({"wrong_key": "value"}, f)
            
            result = subprocess.run([
                sys.executable, "Main.py", temp_dir, "concurrent"
            ], capture_output=True, text=True, cwd="/Users/gilamir/PycharmProjects/DNA-ETL")
            
            # Should exit with error due to failures
            assert result.returncode == 1
            
            # Verify that successful files were processed
            assert "Total files processed: 4" in result.stdout
            assert "✓ Successful: 2" in result.stdout
            assert "✗ Failed: 2" in result.stdout
            
            # Verify successful files have output
            for i in [0, 3]:  # Files 0 and 3 should succeed
                assert Path(files_created[i]["output_file"]).exists()
            
            # Verify failed files don't have output
            for i in [1, 2]:  # Files 1 and 2 should fail
                assert not Path(files_created[i]["output_file"]).exists()
            
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)