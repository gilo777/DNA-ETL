from dataclasses import dataclass

@dataclass
class ValidPaths:
    """
    Object that holds relevant paths for easy access.
    """
    def __init__(self, dna_path, metadata_path, context_path, output_path):
        """
        Initialize the class with file paths for processing genetic data.

        :param dna_path: Path to the DNA sequence file containing genetic data
        :param metadata_path: Path to the metadata file with sample information
        :param context_path: Path to the context file containing additional data context
        :param output_path: Path where processed results will be saved
        """
        self.dna_path = dna_path
        self.metadata_path = metadata_path
        self.context_path = context_path
        self.output_path = output_path

