from itertools import combinations

from DataModels.DnaData import DnaData


class Transformer:

    def __init__(self):
        self.codon_frequencies = {}

    # Remove all related sensitive data
    def remove_private_keys(self, meta_data):
        cleaned_dict = {}

        for key, value in meta_data.items():
            if key.startswith("_"):
                continue

            if isinstance(value, dict):
                cleaned_dict[key] = self.remove_private_keys(value)

            else:
                cleaned_dict[key] = value

        return cleaned_dict


    # transforms the DNA sequences into a dict,
    # for each sequence:
    #  1.claculate GC content
    #  2.codons : frequencies
    #
    # find the most common codon,
    # find the LCS
    def transform_dna(self, dna_data) -> dict:
        sequences_data = []

        for sequence in dna_data.sequences:
            analysis = self.analyze_sequence(sequence)
            sequences_data.append(analysis)

        sequences_dict = {"sequences": sequences_data}

        lcs_data = self.findLCS(dna_data)

        most_common = None
        if self.codon_frequencies:
            most_common = max(self.codon_frequencies, key=self.codon_frequencies.get)

        return {"sequences": sequences_dict, "most_common_codon": most_common,"lcs": lcs_data}



    def analyze_sequence(self, sequence):
        gc_count = sequence.count('G') + sequence.count('C')
        gc_content = round((gc_count / len(sequence)) * 100, 2)

        codons = {}
        for i in range(0, len(sequence) - 2, 3):
            codon = sequence[i:i + 3]
            if len(codon) == 3:
                if codon in codons:
                    codons[codon] += 1
                else:
                    codons[codon] = 1
                if codon in self.codon_frequencies:
                    self.codon_frequencies[codon] += 1
                else:
                    self.codon_frequencies[codon] = 1

        return {
            "gc_content": gc_content,
            "codons": codons
        }

    def findLCS(self, dna_data):
        if len(dna_data.sequences) < 2:
            return {"value": "", "sequences": [], "length": 0}

        all_results = []

        for i, j in combinations(range(len(dna_data.sequences)), 2):
            lcs_value = self.lcs_sequence(dna_data.sequences[i], dna_data.sequences[j])
            if lcs_value:
                participants = [k for k in range(len(dna_data.sequences))
                                if self.is_subsequence(lcs_value, dna_data.sequences[k])]

                all_results.append({
                    "value": lcs_value,
                    "sequences": participants,
                    "length": len(lcs_value)
                })
        if not all_results:
            return {"value": "", "sequences": [], "length": 0}

        return max(all_results, key=lambda x: (x["length"], len(x["sequences"])))

    def is_subsequence(self, subseq, seq):
        i = j = 0
        while i < len(subseq) and j < len(seq):
            if subseq[i] == seq[j]:
                i += 1
            j += 1
        return i == len(subseq)

    def lcs_sequence(self, dna1, dna2):
        m, n = len(dna1), len(dna2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if dna1[i - 1] == dna2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                else:
                    dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])

        lcs = []
        i, j = m, n
        while i > 0 and j > 0:
            if dna1[i - 1] == dna2[j - 1]:
                lcs.append(dna1[i - 1])
                i -= 1
                j -= 1
            elif dp[i - 1][j] > dp[i][j - 1]:
                i -= 1
            else:
                j -= 1

        lcs.reverse()
        return "".join(lcs)

