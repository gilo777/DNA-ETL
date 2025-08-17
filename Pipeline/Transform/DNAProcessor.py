from itertools import combinations

import Levenshtein
from Levenshtein import matching_blocks

from Pipeline.DataModels.DNAData import DNAData


class DNAProcessor:
    """
    DNAProcessor class processes DNA sequence data to calculate GC content, analyze codon
    frequencies, and find the longest common subsequence between all sequences. It maintains
    global codon frequency statistics across all processed sequences to identify
    the most common codon patterns.

    Attributes:
        codon_frequencies (dict): Global accumulator tracking codon occurrences
                                 across all analyzed sequences

    Methods:
        transform_dna(dna_data: DNAData) -> dict:
            Main processing method that coordinates all DNA analysis operations.

        _analyze_sequence(sequence: str) -> dict:
            Analyzes individual sequence for GC content and codon frequencies.

        _find_lcs(dna_data: DNAData) -> dict:
            Finds longest common subsequence across all sequences using pairwise comparison.

        _longest_common_subsequence(seq1: str, seq2: str) -> str:
            Computes LCS between two sequences using Levenshtein matching blocks algorithm.
    """

    def __init__(self):
        self.codon_frequencies = {}

    def transform_dna(self, dna_data: DNAData) -> dict:
        """
        Transforms DNA sequences into analyzed data including GC content, codon frequencies, and LCS.

        :param dna_data: DNAData object containing raw DNA sequences to analyze
        :return: Dictionary with sequences analysis, most common codon, and LCS information
        """
        sequences_data = []
        # Individual sequence analysis
        for sequence in dna_data.sequences:
            analysis_result = self._analyze_sequence(sequence)
            sequences_data.append(analysis_result)

        # Global pattern finding
        lcs_data = self._find_lcs(dna_data)

        # Global codon analysis
        most_common_codon = None
        if self.codon_frequencies:
            most_common_codon = max(
                self.codon_frequencies, key=self.codon_frequencies.get
            )

        return {
            "sequences": sequences_data,
            "most_common_codon": most_common_codon,
            "lcs": lcs_data,
        }

    def _analyze_sequence(self, sequence: str) -> dict:
        """
        Analyzes a single DNA sequence for GC content and codon frequencies.

        :param sequence: DNA sequence string to analyze
        :return: Dictionary containing GC content percentage and codon frequency map
        """
        # GC content calculating
        gc_count = 0
        for nuc in sequence:
            if nuc == "G" or nuc == "C":
                gc_count += 1
        gc_content = round((gc_count / len(sequence)) * 100, 2)

        # Counting codons in the sequence, updating the global codons map
        codons = {}
        for i in range(0, len(sequence) - (len(sequence) % 3) - 2, 3):
            codon = sequence[i : i + 3]
            codons[codon] = codons.get(codon, 0) + 1
            self.codon_frequencies[codon] = self.codon_frequencies.get(codon, 0) + 1

        return {"gc_content": gc_content, "codons": codons}

    def _find_lcs(self, dna_data: DNAData) -> dict:
        """
        Finds the longest common subsequence among all DNA sequences.
        Breaking ties by choosing the longest subsequence found in most sequences.
        The longest common subsequence between two DNA sequences- a single longest
        subsequence that appears in two (or more) sequences.


        :param dna_data: DNAData object containing sequences to compare
        :return: Dictionary with LCS value, participating sequences, and length
        """
        # Verify there are at least two sequences.
        if len(dna_data.sequences) < 2:
            return {"value": "", "sequences": [], "length": 0}

        # Iterating over all pairs of sequences.
        all_results = []
        for i, j in combinations(range(len(dna_data.sequences)), 2):
            # Find the LCS of the current pair
            lcs_value = self._longest_common_subsequence(
                dna_data.sequences[i], dna_data.sequences[j]
            )
            # If an LCS exists for the current pair, find all sequences containing it and save it.
            if lcs_value:
                participants = [
                    k + 1
                    for k in range(len(dna_data.sequences))
                    if lcs_value in dna_data.sequences[k]
                ]
                all_results.append(
                    {
                        "value": lcs_value,
                        "sequences": participants,
                        "length": len(lcs_value),
                    }
                )
        # Verify a LCS is found.
        if not all_results:
            return {"value": "", "sequences": [], "length": 0}
        # Retrieve the LCS with maximum length (breaking ties by most participating sequences).
        return max(all_results, key=lambda x: (x["length"], len(x["sequences"])))

    def _longest_common_subsequence(self, seq1: str, seq2: str) -> str:
        """
        Finds the longest common subsequence between two DNA sequences using Levenshtein algorithm.

        :param seq1: First DNA sequence string
        :param seq2: Second DNA sequence string
        :return: Longest common subsequence string, or empty string if none exists
        """
        # Use Levenshtein to find all common blocks in the two strings
        blocks = matching_blocks(Levenshtein.editops(seq1, seq2), seq1, seq2)
        # Get the longest one
        max_block = max(blocks, key=lambda x: x.size)
        # Slice seq1 from the start of 'max_block' to the end to return the subsequence itself, or "" if there is no common subsequence
        if max_block[2] > 0:
            return seq1[max_block.a : max_block.a + max_block.size]
        else:
            return ""
