import math
from collections import defaultdict
from typing import List, Iterable, Tuple, Dict


class PerplexityScorer:
    """
    Simple n-gram perplexity scorer for sequences of template IDs.
    Higher perplexity => more anomalous sequence.
    """

    def __init__(self, n: int = 3, smoothing_alpha: float = 1.0):
        if n < 2:
            raise ValueError("n must be at least 2 for n-gram models.")
        self.n = int(n)
        self.k = float(smoothing_alpha)
        self.n_gram_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
        self.context_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
        self.vocab: set[str] = set()
        self._fitted = False

    def fit(self, sequences: Iterable[List[str]]) -> None:
        for sequence in sequences:
            if not sequence:
                continue
            self.vocab.update(sequence)
            pad = ["<s>"] * (self.n - 1)
            padded = pad + sequence
            for i in range(len(padded) - self.n + 1):
                ngram = tuple(padded[i : i + self.n])
                ctx = ngram[:-1]
                self.n_gram_counts[ngram] += 1
                self.context_counts[ctx] += 1
        self._fitted = True

    def score(self, sequence: List[str]) -> float:
        """
        Perplexity of the sequence. If model not fitted or sequence empty, returns 1.0.
        """
        if not sequence:
            return 1.0
        if not self._fitted or len(self.vocab) == 0:
            return 1.0

        log_prob = 0.0
        V = float(len(self.vocab))
        pad = ["<s>"] * (self.n - 1)
        padded = pad + sequence

        for i in range(len(padded) - self.n + 1):
            ngram = tuple(padded[i : i + self.n])
            ctx = ngram[:-1]
            token_count = float(self.n_gram_counts.get(ngram, 0))
            ctx_count = float(self.context_counts.get(ctx, 0))
            prob = (token_count + self.k) / (ctx_count + self.k * V)
            if prob <= 0.0:
                # extremely rare; guard division-by-zero / log(0)
                return float("inf")
            log_prob += math.log2(prob)

        cross_entropy = -log_prob / max(1, len(sequence))
        perplexity = math.pow(2.0, cross_entropy)
        # cap to a reasonable range to avoid blowing up downstream normalization
        return float(min(perplexity, 1e6))
