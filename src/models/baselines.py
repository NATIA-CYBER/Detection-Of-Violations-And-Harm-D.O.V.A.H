"""Baseline models for anomaly detection."""
import numpy as np
from sklearn.ensemble import IsolationForest
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

class LogLM:
    """Simple n-gram language model for log template perplexity."""
    
    def __init__(self, n: int = 3):
        """Initialize n-gram model.
        
        Args:
            n: Size of n-grams to use
        """
        self.n = n
        self.ngram_counts: Dict[Tuple[str, ...], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.vocab: Dict[str, int] = defaultdict(int)
        
    def fit(self, sequences: List[List[str]]) -> None:
        """Fit n-gram model on sequences of log templates.
        
        Args:
            sequences: List of template ID sequences
        """
        for seq in sequences:
            # Count unigrams for vocabulary
            for token in seq:
                self.vocab[token] += 1
                
            # Count n-grams
            for i in range(len(seq) - self.n + 1):
                context = tuple(seq[i:i+self.n-1])
                next_token = seq[i+self.n-1]
                self.ngram_counts[context][next_token] += 1
                
    def perplexity(self, sequence: List[str], smoothing: float = 0.1) -> float:
        """Calculate perplexity of a sequence.
        
        Args:
            sequence: Template ID sequence
            smoothing: Laplace smoothing parameter
            
        Returns:
            Perplexity score (higher means more anomalous)
        """
        if len(sequence) < self.n:
            return 0.0
            
        log_prob = 0.0
        count = 0
        
        for i in range(len(sequence) - self.n + 1):
            context = tuple(sequence[i:i+self.n-1])
            next_token = sequence[i+self.n-1]
            
            # Get counts with smoothing
            context_total = sum(self.ngram_counts[context].values())
            count_next = self.ngram_counts[context][next_token]
            vocab_size = len(self.vocab)
            
            # Calculate probability with Laplace smoothing
            prob = (count_next + smoothing) / (context_total + smoothing * vocab_size)
            log_prob += np.log(prob)
            count += 1
            
        # Return perplexity
        return np.exp(-log_prob / count) if count > 0 else 0.0

class WindowIsolationForest:
    """Isolation Forest for window-level anomaly detection."""
    
    def __init__(
        self,
        n_estimators: int = 100,
        contamination: float = 0.1,
        random_state: Optional[int] = None
    ):
        """Initialize model.
        
        Args:
            n_estimators: Number of trees
            contamination: Expected proportion of anomalies
            random_state: Random seed
        """
        self.model = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=random_state
        )
        
    def fit(self, X: np.ndarray) -> None:
        """Fit model on window statistics.
        
        Args:
            X: Array of window features [n_samples, n_features]
        """
        self.model.fit(X)
        
    def predict_score(self, X: np.ndarray) -> np.ndarray:
        """Get anomaly scores for windows.
        
        Args:
            X: Array of window features [n_samples, n_features]
            
        Returns:
            Array of anomaly scores (higher is more anomalous)
        """
        # Convert decision function to probability-like score
        scores = -self.model.score_samples(X)
        return (scores - scores.min()) / (scores.max() - scores.min())
