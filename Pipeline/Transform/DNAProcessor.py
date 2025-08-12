from itertools import combinations

import Levenshtein
from Levenshtein import matching_blocks

from Pipeline.DataModelsAndConstants.DnaData import DnaData


class DNAProcessor:

    def __init__(self):
        self.codon_frequencies = {}

    # transforms the DNA sequences into a dict,
    # for each sequence:
    #  1.calculate GC content
    #  2.codons : frequencies
    #
    # find the most common codon,
    # find the LCS
    def transform_dna(self, dna_data : DnaData) -> dict:
        sequences_data = []

        # analyze DNA sequences and put them in a dict :
        for sequence in dna_data.sequences:
            analysis_result = self._analyze_sequence(sequence)
            sequences_data.append(analysis_result)

        # find the longest common subsequence :
        lcs_data = self._find_lcs(dna_data)

        # check if there are codons to check, and get the most frequent one :
        most_common_codon = None
        if self.codon_frequencies:
            most_common_codon = max(self.codon_frequencies, key=self.codon_frequencies.get)

        # return as a dict containing processed DNA as required :
        return {"sequences": sequences_data, "most_common_codon": most_common_codon,"lcs": lcs_data}


    # given a single DNA sequence, calculates the GC content, and the codons frequencies.
    # keeps count of the codon frequencies in a "global" map to later calculate the most frequent codon
    def _analyze_sequence(self, sequence : str) -> dict:
        # GC content calculation :
        gc_count = 0
        for nuc in sequence:
            if nuc == 'G' or nuc == 'C':
                gc_count += 1
        gc_content = round((gc_count / len(sequence)) * 100, 2)

        # as long as possible, check the codon, and move 3 characters to the right :
        codons = {}
        for i in range(0, len(sequence) - (len(sequence) % 3) - 2, 3):
            codon = sequence[i:i + 3]
            # put in 'codons' - for dna data
            codons[codon] = codons.get(codon, 0) + 1
            # put in 'codon frequencies' to later get the most frequent
            self.codon_frequencies[codon] = self.codon_frequencies.get(codon, 0) + 1

        return {
            "gc_content": gc_content,
            "codons": codons
        }

    # finds the LCS out of all given DNA sequences :
    def _find_lcs(self, dna_data : DnaData) -> dict:
        # when there's a single sequence or no sequences - no LCS
        if len(dna_data.sequences) < 2:
            return {"value": "", "sequences": [], "length": 0}

        all_results = []

        # for every pair possible from the DNA sequences,
        # ***(possibly could be improved, skipping repeating checks or subsequences)
        for i, j in combinations(range(len(dna_data.sequences)), 2):
            # get the LCS of the current pair
            lcs_value = self._longest_common_subsequence(dna_data.sequences[i], dna_data.sequences[j])
            # if there is one, find all sequences cotaining it and save it.
            if lcs_value:
                participants = [k + 1 for k in range(len(dna_data.sequences))
                                if lcs_value in dna_data.sequences[k]]

                all_results.append({
                    "value": lcs_value,
                    "sequences": participants,
                    "length": len(lcs_value)
                })
        if not all_results:
            return {"value": "", "sequences": [], "length": 0}
        # find the LCS of all - priority for length and then number of sequences containing it
        return max(all_results, key = lambda x: (x["length"], len(x["sequences"])))



    # Given two strings(DNA sequences) calculates and returns the longest common subsequence :
    def _longest_common_subsequence(self, seq1 : str, seq2: str) -> str:
        # use Levenshtein to find all common blocks in the two strings
        blocks = matching_blocks(Levenshtein.editops(seq1, seq2), seq1, seq2)
        # get the longest one
        max_block = max(blocks, key = lambda x : x.size)
        # block[2] = block.size
        if max_block[2] > 0:
            # slice deq1 from the start of 'max_block' to the end to return the subsequence itself :
            return seq1[max_block.a:max_block.a + max_block.size]
        else:
            return ""



