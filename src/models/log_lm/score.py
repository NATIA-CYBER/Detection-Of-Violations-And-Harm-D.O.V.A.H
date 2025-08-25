import math
from collections import Counter, defaultdict
from typing import List, Iterable, Tuple, Dict, Any

class PerplexityScorer:
    """
    Calculates perplexity for a sequence of template IDs using an n-gram model.
    """

    def __init__(self, n: int = 3, smoothing_alpha: float = 1.0):
        """
        Initializes the n-gram model.

        Args:
            n (int): The order of the n-gram model (e.g., 3 for trigrams).
            smoothing_alpha (float): The Laplace smoothing parameter.
        """
        if n < 2:
            raise ValueError("n must be at least 2 for n-gram models.")
        self.n = n
        self.k = smoothing_alpha
        self.n_gram_counts = defaultdict(int)
        self.context_counts = defaultdict(int)
        self.vocab = set()

    def fit(self, sequences: Iterable[List[str]]):
        """
        Trains the n-gram model on template ID sequences.
        """
        for sequence in sequences:
            self.vocab.update(sequence)
            padded_sequence = ["<s>"] * (self.n - 1) + sequence
            for i in range(len(padded_sequence) - self.n + 1):
                ngram = tuple(padded_sequence[i:i+self.n])
                context = ngram[:-1]
                self.n_gram_counts[ngram] += 1
                self.context_counts[context] += 1

    def score(self, sequence: List[str]) -> float:
        """
        Calculates the perplexity of a single sequence.
        A higher perplexity score indicates a more anomalous sequence.
        """
        if not sequence:
            return 1.0

        log_prob = 0.0
        vocab_size = len(self.vocab)
        if vocab_size == 0:
            # Model hasn't been fit: define neutral perplexity
            return 1.0
        padded_sequence = ["<s>"] * (self.n - 1) + sequence

        for i in range(len(padded_sequence) - self.n + 1):
            ngram = tuple(padded_sequence[i:i+self.n])
            context = ngram[:-1]

            token_count = self.n_gram_counts.get(ngram, 0)
            prefix_count = self.context_counts.get(context, 0)

            prob = (token_count + self.k) / (prefix_count + self.k * vocab_size)

            if prob > 0:
                log_prob += math.log2(prob)
            else:
                # Handle zero probability with a large negative log-probability
                return float('inf')

        # Perplexity is 2 to the power of the cross-entropy
        cross_entropy = -log_prob / len(sequence)
        perplexity = math.pow(2, cross_entropy)
        return perplexity


if __name__ == '__main__':
    # --- Example Usage ---
    # Corpus of normal log sequences (template IDs)
    normal_sequences = [
        ['T01', 'T02', 'T03', 'T04'],
        ['T01', 'T02', 'T05'],
        ['T06', 'T07'],
        ['T01', 'T02', 'T03', 'T08', 'T09'],
        ['T06', 'T10', 'T11']
    ]

    # Create and train the scorer
    scorer = PerplexityScorer(n=3)
    scorer.fit(normal_sequences)

    # --- Test Sequences ---
    test_normal = ['T01', 'T02', 'T05']
    test_anomaly = ['T01', 'T99', 'T05'] # T99 is new
    test_unusual_pattern = ['T06', 'T02', 'T03']

    # Calculate and print perplexity scores
    score_normal = scorer.score(test_normal)
    score_anomaly = scorer.score(test_anomaly)
    score_unusual = scorer.score(test_unusual_pattern)

    print(f"Score for normal sequence '{' '.join(test_normal)}':")
    print(f"  Perplexity: {score_normal:.2f}\n")

    print(f"Score for anomalous sequence '{' '.join(test_anomaly)}':")
    print(f"  Perplexity: {score_anomaly:.2f}\n")

    print(f"Score for unusual pattern '{' '.join(test_unusual_pattern)}':")
    print(f"  Perplexity: {score_unusual:.2f}\n")
