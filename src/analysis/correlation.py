"""Event correlation using sliding windows and graph analysis."""
import networkx as nx
import pandas as pd
import numpy as np
from typing import Dict, List, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class EventNode:
    """Node in event correlation graph."""
    event_id: str
    timestamp: datetime
    host: str
    process: str
    severity: str
    template_id: str
    message: str

@dataclass
class EventEdge:
    """Edge between correlated events."""
    source_id: str
    target_id: str
    weight: float
    correlation_type: str
    time_delta: timedelta

class EventCorrelator:
    def __init__(
        self,
        window_size: timedelta = timedelta(minutes=5),
        min_weight: float = 0.5
    ):
        """Initialize event correlator.
        
        Args:
            window_size: Size of sliding window
            min_weight: Minimum edge weight to keep
        """
        self.window_size = window_size
        self.min_weight = min_weight
        self.graph = nx.DiGraph()
        self.template_pairs: Dict[Tuple[str, str], int] = defaultdict(int)
        self.host_pairs: Dict[Tuple[str, str], int] = defaultdict(int)
        
    def _calculate_temporal_weight(self, delta: timedelta) -> float:
        """Calculate temporal correlation weight."""
        # Weight decays exponentially with time difference
        decay_factor = 0.5
        return np.exp(-decay_factor * delta.total_seconds() / self.window_size.total_seconds())
        
    def _calculate_template_weight(self, source_id: str, target_id: str) -> float:
        """Calculate template correlation weight."""
        pair = (source_id, target_id)
        pair_count = self.template_pairs[pair]
        total_count = sum(self.template_pairs.values())
        return pair_count / (total_count + 1) if total_count > 0 else 0
        
    def _calculate_host_weight(self, source: str, target: str) -> float:
        """Calculate host correlation weight."""
        pair = (source, target)
        pair_count = self.host_pairs[pair]
        total_count = sum(self.host_pairs.values())
        return pair_count / (total_count + 1) if total_count > 0 else 0
        
    def update_graph(self, events_df: pd.DataFrame) -> None:
        """Update correlation graph with new events.
        
        Args:
            events_df: DataFrame with new events
        """
        # Convert events to nodes
        new_nodes = []
        for _, row in events_df.iterrows():
            node = EventNode(
                event_id=row["event_id"],
                timestamp=row["timestamp"],
                host=row["host"],
                process=row["process"],
                severity=row["severity"],
                template_id=row["template_id"],
                message=row["message"]
            )
            new_nodes.append(node)
            self.graph.add_node(node.event_id, **node.__dict__)
            
        # Find correlations between events in window
        window_start = min(node.timestamp for node in new_nodes)
        window_end = max(node.timestamp for node in new_nodes)
        
        # Get existing nodes in window
        window_nodes = [
            (node_id, data)
            for node_id, data in self.graph.nodes(data=True)
            if window_start - self.window_size <= data["timestamp"] <= window_end
        ]
        
        # Calculate correlations
        for source in new_nodes:
            for target_id, target_data in window_nodes:
                if source.event_id == target_id:
                    continue
                    
                # Calculate weights
                time_delta = abs(source.timestamp - target_data["timestamp"])
                temporal_weight = self._calculate_temporal_weight(time_delta)
                
                template_weight = self._calculate_template_weight(
                    source.template_id,
                    target_data["template_id"]
                )
                
                host_weight = self._calculate_host_weight(
                    source.host,
                    target_data["host"]
                )
                
                # Combine weights
                weight = (temporal_weight + template_weight + host_weight) / 3
                
                if weight >= self.min_weight:
                    edge = EventEdge(
                        source_id=source.event_id,
                        target_id=target_id,
                        weight=weight,
                        correlation_type="temporal",
                        time_delta=time_delta
                    )
                    self.graph.add_edge(
                        edge.source_id,
                        edge.target_id,
                        **edge.__dict__
                    )
                    
                    # Update pair frequencies
                    self.template_pairs[
                        (source.template_id, target_data["template_id"])
                    ] += 1
                    self.host_pairs[
                        (source.host, target_data["host"])
                    ] += 1
                    
        # Prune old nodes
        cutoff = window_end - self.window_size * 2
        old_nodes = [
            node_id
            for node_id, data in self.graph.nodes(data=True)
            if data["timestamp"] < cutoff
        ]
        self.graph.remove_nodes_from(old_nodes)
        
    def find_attack_paths(
        self,
        min_severity: str = "ERROR",
        min_path_length: int = 3
    ) -> List[List[str]]:
        """Find potential attack paths in correlation graph.
        
        Args:
            min_severity: Minimum severity to consider
            min_path_length: Minimum path length to return
            
        Returns:
            List of event ID paths
        """
        # Find high severity nodes
        severe_nodes = [
            node_id
            for node_id, data in self.graph.nodes(data=True)
            if data["severity"] >= min_severity
        ]
        
        # Find paths between severe nodes
        paths = []
        for source in severe_nodes:
            for target in severe_nodes:
                if source != target:
                    try:
                        path = nx.shortest_path(
                            self.graph,
                            source=source,
                            target=target,
                            weight="weight"
                        )
                        if len(path) >= min_path_length:
                            paths.append(path)
                    except nx.NetworkXNoPath:
                        continue
                        
        return paths
