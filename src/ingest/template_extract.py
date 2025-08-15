"""ML-based log template extraction using clustering."""
import numpy as np
import pandas as pd
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
from difflib import SequenceMatcher
import re

@dataclass
class Template:
    """Extracted log template."""
    template_id: str
    pattern: str
    regex: str
    variable_indices: List[int]
    sample_messages: List[str]
    cluster_size: int

class TemplateExtractor:
    def __init__(
        self,
        min_cluster_size: int = 3,
        max_dist: float = 0.3,
        max_templates: int = 1000,
        min_token_freq: float = 0.8
    ):
        """Initialize template extractor.
        
        Args:
            min_cluster_size: Minimum messages per template
            max_dist: Maximum distance for clustering
            max_templates: Maximum number of templates to maintain
            min_token_freq: Minimum frequency for constant tokens
        """
        self.min_cluster_size = min_cluster_size
        self.max_dist = max_dist
        self.max_templates = max_templates
        self.min_token_freq = min_token_freq
        
        self.vectorizer = TfidfVectorizer(
            analyzer='char_wb',
            ngram_range=(3, 3),
            max_features=1000
        )
        self.clustering = DBSCAN(
            eps=max_dist,
            min_samples=min_cluster_size,
            metric='cosine'
        )
        
        self.templates: Dict[str, Template] = {}
        self.template_counts: Dict[str, int] = {}
        
    def _preprocess(self, message: str) -> str:
        """Preprocess log message."""
        # Replace common variable patterns
        message = re.sub(r'\d+', '<NUM>', message)
        message = re.sub(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})', '<UUID>', message)
        message = re.sub(r'([0-9a-fA-F]{32})', '<HASH>', message)
        message = re.sub(r'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})', '<IP>', message)
        return message
        
    def _extract_pattern(self, messages: List[str]) -> Tuple[str, List[int]]:
        """Extract template pattern from cluster messages."""
        if not messages:
            return "", []
            
        # Tokenize messages
        tokenized = [msg.split() for msg in messages]
        
        # Find constant token positions
        n_msgs = len(messages)
        n_tokens = len(tokenized[0])
        
        constant_pos = []
        variable_pos = []
        
        for pos in range(n_tokens):
            # Get all tokens at this position
            pos_tokens = [msg[pos] for msg in tokenized if len(msg) > pos]
            
            # Check if position has consistent token
            unique_tokens = set(pos_tokens)
            most_common = max(unique_tokens, key=pos_tokens.count)
            freq = pos_tokens.count(most_common) / len(pos_tokens)
            
            if freq >= self.min_token_freq:
                constant_pos.append(pos)
            else:
                variable_pos.append(pos)
                
        # Build template pattern
        template_tokens = tokenized[0].copy()
        for pos in variable_pos:
            if pos < len(template_tokens):
                template_tokens[pos] = '<*>'
                
        return ' '.join(template_tokens), variable_pos
        
    def _build_regex(self, pattern: str) -> str:
        """Convert template pattern to regex."""
        regex = re.escape(pattern)
        regex = regex.replace(r'\<\*\>', r'(.*?)')
        return f'^{regex}$'
        
    def extract_templates(self, messages: List[str]) -> Dict[str, Template]:
        """Extract templates from log messages.
        
        Args:
            messages: List of log messages
            
        Returns:
            Dict mapping template IDs to templates
        """
        if not messages:
            return {}
            
        # Preprocess messages
        preprocessed = [self._preprocess(msg) for msg in messages]
        
        # Vectorize messages
        X = self.vectorizer.fit_transform(preprocessed)
        
        # Cluster messages
        labels = self.clustering.fit_predict(X)
        
        # Extract templates for each cluster
        new_templates = {}
        for label in set(labels):
            if label == -1:  # Noise cluster
                continue
                
            # Get messages in cluster
            cluster_msgs = [
                msg for msg, lbl in zip(messages, labels)
                if lbl == label
            ]
            
            # Extract template pattern
            pattern, var_pos = self._extract_pattern(cluster_msgs)
            if not pattern:
                continue
                
            # Generate template ID
            template_id = f"template_{len(self.templates)}"
            
            # Create template
            template = Template(
                template_id=template_id,
                pattern=pattern,
                regex=self._build_regex(pattern),
                variable_indices=var_pos,
                sample_messages=cluster_msgs[:5],
                cluster_size=len(cluster_msgs)
            )
            
            new_templates[template_id] = template
            
        # Merge with existing templates
        all_templates = {**self.templates, **new_templates}
        
        # Prune similar templates
        final_templates = {}
        for t1_id, t1 in all_templates.items():
            is_unique = True
            for t2_id, t2 in final_templates.items():
                similarity = SequenceMatcher(
                    None,
                    t1.pattern,
                    t2.pattern
                ).ratio()
                if similarity > 0.8:  # Very similar templates
                    is_unique = False
                    # Keep template with larger cluster
                    if t1.cluster_size > t2.cluster_size:
                        final_templates[t2_id] = t1
                    break
                    
            if is_unique:
                final_templates[t1_id] = t1
                
        # Limit number of templates
        if len(final_templates) > self.max_templates:
            sorted_templates = sorted(
                final_templates.items(),
                key=lambda x: x[1].cluster_size,
                reverse=True
            )
            final_templates = dict(sorted_templates[:self.max_templates])
            
        self.templates = final_templates
        return final_templates
        
    def match_template(self, message: str) -> Tuple[Optional[str], Dict[int, str]]:
        """Match message to existing template.
        
        Args:
            message: Log message
            
        Returns:
            Tuple of (template_id, variable_values)
        """
        preprocessed = self._preprocess(message)
        
        for template_id, template in self.templates.items():
            match = re.match(template.regex, preprocessed)
            if match:
                variables = {
                    i: val
                    for i, val in enumerate(match.groups())
                }
                return template_id, variables
                
        return None, {}
