from dataclasses import dataclass

@dataclass
class DNAData:
    """
    Object that holds DNA sequences for easy access.

    Attributes:
        sequences (list): List of DNA sequences.
    """
    def __init__(self) -> None:
        self.sequences = []
