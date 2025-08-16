import pytest
import tempfile
import json
import uuid
from pathlib import Path
from Pipeline.DataModels.ValidPaths import ValidPaths
from Pipeline.DataModels.DNAData import DNAData


@pytest.fixture
def sample_uuid():
    """Generate a sample UUID for testing."""
    return str(uuid.uuid4())


@pytest.fixture
def sample_dna_sequences():
    """Provide sample DNA sequences for testing."""
    return ["ATCGCGATCG", "GCTAGCTAGC", "TTAATTAATT", "CGCGCGCGCG"]


@pytest.fixture
def sample_metadata():
    """Provide sample metadata for testing."""
    return {
        "participant_id": "test-123",
        "name": "John Doe",
        "date_of_birth": "1980-05-15",
        "age": 43,
        "location": "Boston",
        "study_id": "DNA-001",
    }


@pytest.fixture
def sample_metadata_with_private():
    """Provide metadata with private keys for testing removal."""
    return {
        "participant_id": "test-123",
        "name": "John Doe",
        "_ssn": "123-45-6789",
        "age": 43,
        "_internal_id": "secret123",
        "location": "Boston",
        "contact": {"email": "john@example.com", "_phone": "555-1234"},
    }


@pytest.fixture
def temp_dna_file(sample_dna_sequences):
    """Create a temporary DNA file with sample sequences."""
    with tempfile.NamedTemporaryFile(mode="w", suffix="_dna.txt", delete=False) as f:
        for seq in sample_dna_sequences:
            f.write(seq + "\n")
        return f.name


@pytest.fixture
def temp_metadata_file(sample_metadata):
    """Create a temporary metadata JSON file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix="_dna.json", delete=False) as f:
        json.dump(sample_metadata, f)
        return f.name


@pytest.fixture
def temp_input_file(sample_uuid):
    """Create a temporary input JSON file for pipeline testing."""

    def _create_input_file(context_path, results_path):
        input_data = {"context_path": context_path, "results_path": results_path}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(input_data, f)
            return f.name

    return _create_input_file


@pytest.fixture
def temp_directory_with_files(sample_uuid, sample_dna_sequences, sample_metadata):
    """Create a temporary directory with DNA and metadata files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        participant_dir = Path(temp_dir) / sample_uuid
        participant_dir.mkdir()

        # Create DNA file
        dna_file = participant_dir / f"{sample_uuid}_dna.txt"
        with open(dna_file, "w") as f:
            for seq in sample_dna_sequences:
                f.write(seq + "\n")

        # Create metadata file
        metadata_file = participant_dir / f"{sample_uuid}_dna.json"
        with open(metadata_file, "w") as f:
            json.dump(sample_metadata, f)

        yield {
            "temp_dir": temp_dir,
            "participant_dir": str(participant_dir),
            "dna_file": str(dna_file),
            "metadata_file": str(metadata_file),
            "participant_id": sample_uuid,
        }


@pytest.fixture
def valid_paths_fixture(temp_dna_file, temp_metadata_file):
    """Create a ValidPaths object for testing."""
    return ValidPaths(
        dna_path=temp_dna_file,
        metadata_path=temp_metadata_file,
        context_path="/tmp",
        results_path="/tmp/output.json",
    )


@pytest.fixture
def dna_data_fixture(sample_dna_sequences):
    """Create a DNAData object for testing."""
    dna_data = DNAData()
    dna_data.sequences = sample_dna_sequences
    return dna_data


@pytest.fixture
def cleanup_files():
    """Cleanup fixture to remove temporary files after tests."""
    files_to_cleanup = []

    def add_file(file_path):
        files_to_cleanup.append(file_path)

    yield add_file

    # Cleanup
    for file_path in files_to_cleanup:
        try:
            Path(file_path).unlink()
        except FileNotFoundError:
            pass
