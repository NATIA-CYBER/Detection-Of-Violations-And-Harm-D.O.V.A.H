"""Tests for event correlation module."""
import pytest
import pandas as pd
import networkx as nx
from datetime import datetime, timedelta
from src.analysis.correlation import EventCorrelator, EventNode, EventEdge

def test_correlation_graph():
    """Test building and updating correlation graph."""
    correlator = EventCorrelator(window_size=timedelta(minutes=5))
    
    # Create test events
    events = pd.DataFrame({
        "event_id": ["e1", "e2", "e3"],
        "timestamp": [
            datetime.now(),
            datetime.now() + timedelta(seconds=30),
            datetime.now() + timedelta(seconds=60)
        ],
        "host": ["host1", "host1", "host2"],
        "process": ["proc1", "proc1", "proc2"],
        "severity": ["INFO", "ERROR", "ERROR"],
        "template_id": ["t1", "t2", "t2"],
        "message": [
            "Connection attempt",
            "Authentication failed",
            "Access denied"
        ]
    })
    
    # Update graph
    correlator.update_graph(events)
    
    # Check graph structure
    assert isinstance(correlator.graph, nx.DiGraph)
    assert len(correlator.graph.nodes) == 3
    assert len(correlator.graph.edges) > 0
    
    # Check node attributes
    node = correlator.graph.nodes["e1"]
    assert isinstance(node["timestamp"], datetime)
    assert node["host"] == "host1"
    assert node["severity"] == "INFO"
    
    # Check edge attributes
    edges = list(correlator.graph.edges(data=True))
    assert len(edges) > 0
    edge = edges[0][2]
    assert 0 <= edge["weight"] <= 1
    assert isinstance(edge["time_delta"], timedelta)

def test_attack_path_detection():
    """Test detection of attack paths."""
    correlator = EventCorrelator(window_size=timedelta(minutes=5))
    
    # Create attack sequence
    events = pd.DataFrame({
        "event_id": ["e1", "e2", "e3", "e4"],
        "timestamp": [
            datetime.now(),
            datetime.now() + timedelta(seconds=30),
            datetime.now() + timedelta(seconds=60),
            datetime.now() + timedelta(seconds=90)
        ],
        "host": ["host1", "host1", "host1", "host2"],
        "process": ["proc1", "proc1", "proc2", "proc2"],
        "severity": ["WARNING", "ERROR", "ERROR", "CRITICAL"],
        "template_id": ["t1", "t2", "t3", "t4"],
        "message": [
            "Port scan detected",
            "Multiple login failures",
            "Privilege escalation attempt",
            "Data exfiltration detected"
        ]
    })
    
    correlator.update_graph(events)
    
    # Find attack paths
    paths = correlator.find_attack_paths(
        min_severity="ERROR",
        min_path_length=3
    )
    
    assert len(paths) > 0
    assert len(paths[0]) >= 3
    assert "e2" in paths[0]  # Login failures
    assert "e4" in paths[0]  # Data exfiltration

def test_temporal_correlation():
    """Test temporal correlation weights."""
    correlator = EventCorrelator(window_size=timedelta(minutes=5))
    
    # Create events with different time gaps
    events = pd.DataFrame({
        "event_id": ["e1", "e2", "e3"],
        "timestamp": [
            datetime.now(),
            datetime.now() + timedelta(seconds=10),
            datetime.now() + timedelta(minutes=4)
        ],
        "host": ["host1"] * 3,
        "process": ["proc1"] * 3,
        "severity": ["ERROR"] * 3,
        "template_id": ["t1"] * 3,
        "message": ["msg"] * 3
    })
    
    correlator.update_graph(events)
    
    # Check edge weights
    edges = list(correlator.graph.edges(data=True))
    close_edge = next(e for e in edges if e[0] == "e1" and e[1] == "e2")
    far_edge = next(e for e in edges if e[0] == "e1" and e[1] == "e3")
    
    assert close_edge[2]["weight"] > far_edge[2]["weight"]

def test_host_correlation():
    """Test host-based correlation."""
    correlator = EventCorrelator(window_size=timedelta(minutes=5))
    
    # Create events on same and different hosts
    events = pd.DataFrame({
        "event_id": ["e1", "e2", "e3"],
        "timestamp": [
            datetime.now(),
            datetime.now() + timedelta(seconds=30),
            datetime.now() + timedelta(seconds=30)
        ],
        "host": ["host1", "host1", "host2"],
        "process": ["proc1"] * 3,
        "severity": ["ERROR"] * 3,
        "template_id": ["t1"] * 3,
        "message": ["msg"] * 3
    })
    
    correlator.update_graph(events)
    
    # Check edge weights
    edges = list(correlator.graph.edges(data=True))
    same_host = next(e for e in edges if e[0] == "e1" and e[1] == "e2")
    diff_host = next(e for e in edges if e[0] == "e1" and e[1] == "e3")
    
    assert same_host[2]["weight"] > diff_host[2]["weight"]
