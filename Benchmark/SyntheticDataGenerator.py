import json
import os
import random
import uuid
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List


class SyntheticDataGenerator:
    """
    Generates synthetic genetic data for testing the ETL pipeline at scale.
    Creates realistic participant directories with DNA sequences and metadata
    following the same structure as the original pipeline expects.

    This class helps test performance and functionality without using real genetic data.
    """

    def __init__(self, base_output_dir: str = "./SyntheticData"):
        """
        Initialize the synthetic data generator.

        :param base_output_dir: Directory where synthetic data will be created
        """
        self.base_output_dir = Path(base_output_dir)
        self.participants_dir = self.base_output_dir / "participants"
        self.input_configs_dir = self.base_output_dir / "input_configs"

        # DNA nucleotides for generating sequences
        self.nucleotides = ['A', 'T', 'G', 'C']

        # Sample data for realistic metadata generation
        self.sample_names = ["John Doe", "Jane Smith", "Mike Johnson", "Sarah Wilson", "David Brown"]
        self.ethnicities = ["Caucasian", "Hispanic", "Asian", "African American", "Mixed"]
        self.genders = ["Male", "Female"]
        self.diseases = [
            "Diabetes", "Hypertension", "Cancer", "Heart Disease",
            "Alzheimer's", "Parkinson's", "Arthritis"
        ]
        self.relations = ["Mother", "Father", "Sibling", "Grandparent"]

    def generate_dataset(self, num_participants: int) -> List[str]:
        """
        Generates a complete synthetic dataset with specified number of participants.

        :param num_participants: Number of participant datasets to create
        :return: List of paths to generated input configuration files
        """
        print(f"Generating synthetic dataset with {num_participants} participants...")

        # Create output directories
        self.participants_dir.mkdir(parents=True, exist_ok=True)
        self.input_configs_dir.mkdir(parents=True, exist_ok=True)

        input_config_paths = []

        for i in range(num_participants):
            # Generate unique participant ID
            participant_uuid = str(uuid.uuid4())

            # Create participant directory and files
            self._create_participant_data(participant_uuid)

            # Create input configuration file
            config_path = self._create_input_config(participant_uuid)
            input_config_paths.append(str(config_path))

            # Progress indicator
            if (i + 1) % 10 == 0:
                print(f"Generated {i + 1}/{num_participants} participants")

        print(f"✓ Successfully generated {num_participants} synthetic participants")
        print(f"Data location: {self.participants_dir}")
        print(f"Input configs: {self.input_configs_dir}")

        return input_config_paths

    def _create_participant_data(self, participant_uuid: str) -> None:
        """
        Creates DNA sequence file and metadata file for a single participant.

        :param participant_uuid: Unique identifier for the participant
        """
        # Create participant directory
        participant_dir = self.participants_dir / participant_uuid
        participant_dir.mkdir(exist_ok=True)

        # DON'T create output directory - the Loader writes to a file, not a directory
        # The results_path will be the output file path, not a directory

        # Generate DNA sequences file
        dna_file_path = participant_dir / f"{participant_uuid}_dna.txt"
        self._generate_dna_file(dna_file_path)

        # Generate metadata file
        metadata_file_path = participant_dir / f"{participant_uuid}_dna.json"
        self._generate_metadata_file(metadata_file_path, participant_uuid)

    def _generate_dna_file(self, file_path: Path) -> None:
        """
        Generates a DNA sequence file with multiple realistic sequences.

        :param file_path: Path where the DNA file will be saved
        """
        # Generate 2-5 DNA sequences per participant
        num_sequences = random.randint(2, 5)

        with open(file_path, 'w') as f:
            for i in range(num_sequences):
                # Generate sequence length between 200-500 nucleotides
                sequence_length = random.randint(200, 500)

                # Create random DNA sequence
                sequence = ''.join(random.choices(self.nucleotides, k=sequence_length))

                f.write(sequence)
                if i < num_sequences - 1:  # Don't add newline after last sequence
                    f.write('\n')

    def _generate_metadata_file(self, file_path: Path, participant_uuid: str) -> None:
        """
        Generates realistic metadata JSON file for a participant.

        :param file_path: Path where the metadata file will be saved
        :param participant_uuid: UUID of the participant
        """
        # Import constants to ensure compliance
        from Constants import YEAR_RANGE_LOWER, YEAR_RANGE_UPPER

        # Generate realistic dates within the valid year range
        # Use the full valid range for test dates
        test_date = self._random_date_in_range(YEAR_RANGE_LOWER, YEAR_RANGE_UPPER)
        completion_date = test_date + timedelta(days=random.randint(1, 14))
        collection_date = test_date - timedelta(days=random.randint(0, 7))

        # Ensure completion and collection dates don't go outside valid range
        if completion_date.year > YEAR_RANGE_UPPER:
            completion_date = datetime(YEAR_RANGE_UPPER, 12, 31)
        if collection_date.year < YEAR_RANGE_LOWER:
            collection_date = datetime(YEAR_RANGE_LOWER, 1, 1)

        # Birth date uses special logic to ensure age >= MIN_AGE
        birth_date = self._random_birth_date()

        metadata = {
            "test_metadata": {
                "test_id": f"DNA{random.randint(100000, 999999)}",
                "test_type": random.choice(["Genetic Profiling", "Exome Sequencing", "Whole Genome"]),
                "date_requested": test_date.strftime('%Y-%m-%d'),
                "date_completed": completion_date.strftime('%Y-%m-%d'),
                "status": "Completed",
                "laboratory_info": {
                    "name": random.choice(["BioGene Lab Co.", "GeneTech Solutions", "Advanced Genomics"]),
                    "certification": "CLIA Certified"
                }
            },
            "sample_metadata": {
                "sample_id": f"SAMP{random.randint(100000, 999999)}",
                "sample_type": random.choice(["Blood", "Saliva", "Tissue"]),
                "collection_date": collection_date.strftime('%Y-%m-%d'),
                "data_file": f"./data/participants/{participant_uuid}"
            },
            "analysis_metadata": {
                "platform": random.choice(["Illumina HiSeq 4000", "Illumina NovaSeq", "PacBio Sequel"]),
                "methodology": random.choice(["Exome Sequencing", "Whole Genome", "Targeted Panel"]),
                "coverage": f"{random.randint(30, 100)}x",
                "reference_genome": random.choice(["GRCh37", "GRCh38"]),
                "variants_detected": {
                    "total": random.randint(3000, 8000),
                    "pathogenic": random.randint(10, 50),
                    "likely_pathogenic": random.randint(5, 30),
                    "benign": random.randint(2500, 7500)
                }
            },
            "individual_metadata": {
                "_individual_id": f"IND{random.randint(100000, 999999)}",
                "_name": random.choice(self.sample_names),
                "date_of_birth": birth_date.strftime('%Y-%m-%d'),
                "gender": random.choice(self.genders),
                "ethnicity": random.choice(self.ethnicities),
                "family_history": {
                    "diseases": self._generate_family_history()
                }
            }
        }

        with open(file_path, 'w') as f:
            json.dump(metadata, f, indent=4)

    def _create_input_config(self, participant_uuid: str) -> Path:
        """
        Creates input configuration JSON file for the participant.

        :param participant_uuid: UUID of the participant
        :return: Path to the created input configuration file
        """
        # Use simple string paths that work relative to current directory
        base_dir_name = self.base_output_dir.name
        config_data = {
            "context_path": f"./{base_dir_name}/participants/{participant_uuid}",
            "results_path": f"./{base_dir_name}/participants/{participant_uuid}/out"
        }

        config_file_path = self.input_configs_dir / f"{participant_uuid}_input.json"

        with open(config_file_path, 'w') as f:
            json.dump(config_data, f, indent=4)

        return config_file_path

    def _random_date_in_range(self, start_year: int, end_year: int) -> datetime:
        """
        Generates a random date within the specified year range.

        :param start_year: Starting year
        :param end_year: Ending year
        :return: Random datetime object
        """
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)

        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randrange(days_between)

        return start_date + timedelta(days=random_days)

    def _random_birth_date(self) -> datetime:
        """
        Generates a random birth date ensuring the person is over 40 years old
        AND the birth date falls within the valid year range (2014-2024).

        Since birth dates must be in 2014-2024 but people must be 40+,
        we can't generate realistic birth dates. Instead, we'll generate
        dates in the valid range but adjust other metadata accordingly.

        :return: Random birth date datetime object
        """
        # Import constants to respect validation rules
        from Constants import YEAR_RANGE_LOWER, YEAR_RANGE_UPPER, MIN_AGE

        # For synthetic data, we need to balance two constraints:
        # 1. Birth dates must be 2014-2024 (YEAR_RANGE validation)
        # 2. People must be 40+ years old (MIN_AGE validation)
        #
        # Since these are contradictory for realistic data, we'll use
        # birth dates in the valid range but treat them as "synthetic timestamps"
        # rather than realistic birth dates.

        # Generate a date within the valid year range
        birth_year = random.randint(YEAR_RANGE_LOWER, YEAR_RANGE_UPPER)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Safe day that exists in all months

        birth_date = datetime(birth_year, birth_month, birth_day)

        # Verify this results in a valid age (it won't for realistic interpretation,
        # but the validation logic might expect it to)
        today = datetime.now()
        calculated_age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

        # If the calculated age is less than MIN_AGE, we have a fundamental
        # constraint conflict. For synthetic data, we'll use a workaround:
        # generate dates that will pass validation even if unrealistic.

        if calculated_age < MIN_AGE:
            # Fallback: create a birth date that ensures MIN_AGE
            # This will be outside the year range, but satisfies age constraint
            # We'll prioritize age constraint as it seems more critical for pipeline logic
            target_birth_year = today.year - MIN_AGE - random.randint(0, 40)  # 40-80 years old
            birth_date = datetime(target_birth_year, birth_month, birth_day)

            # Ensure we don't go too far back (keep within reasonable bounds)
            if birth_date.year < 1940:
                birth_date = datetime(1940 + random.randint(0, 30), birth_month, birth_day)

        return birth_date

    def _generate_family_history(self) -> List[dict]:
        """
        Generates realistic family medical history.

        :return: List of family medical history entries
        """
        num_conditions = random.randint(0, 3)
        history = []

        for _ in range(num_conditions):
            condition = {
                "name": random.choice(self.diseases),
                "relation": random.choice(self.relations),
                "age_at_diagnosis": random.randint(30, 70)
            }
            history.append(condition)

        return history

    def cleanup_generated_data(self) -> None:
        """
        Removes all generated synthetic data directories and files.
        Use this method to clean up after testing is complete.
        """
        if self.base_output_dir.exists():
            print(f"🗑️  Cleaning up synthetic data at {self.base_output_dir}")
            shutil.rmtree(self.base_output_dir)
            print("✓ Cleanup completed")
        else:
            print("No synthetic data found to clean up")

    def get_data_size_info(self) -> dict:
        """
        Returns information about the size of generated data.
        Useful for understanding disk space usage.

        :return: Dictionary with size information
        """
        if not self.base_output_dir.exists():
            return {"total_size_mb": 0, "file_count": 0, "directory_count": 0}

        total_size = 0
        file_count = 0
        directory_count = 0

        for root, dirs, files in os.walk(self.base_output_dir):
            directory_count += len(dirs)
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
                file_count += 1

        return {
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_count": file_count,
            "directory_count": directory_count,
            "base_path": str(self.base_output_dir)
        }


# Example usage for testing
if __name__ == "__main__":
    # Create generator instance
    generator = SyntheticDataGenerator()

    # Generate small test dataset
    input_configs = generator.generate_dataset(5)

    print("\nGenerated input configuration files:")
    for config in input_configs:
        print(f"  {config}")

    # Show data size information
    size_info = generator.get_data_size_info()
    print(f"\nData size: {size_info['total_size_mb']} MB")
    print(f"Files created: {size_info['file_count']}")

    # Cleanup (uncomment if you want to auto-cleanup)
    # generator.cleanup_generated_data()