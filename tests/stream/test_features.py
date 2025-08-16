import json
import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from collections import deque
from datetime import datetime, timedelta, timezone

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.stream import features

class TestStreamFeatures(unittest.TestCase):

    def _run_stream_processor(self, events_data, schema_path='parsed_log.schema.json'):
        """Helper to run the stream processor with mocked stdin."""
        input_data = "\n".join(json.dumps(e) for e in events_data) + "\n"
        
        with patch('sys.stdin') as mock_stdin:
            mock_stdin.read.return_value = input_data
            mock_stdin.__iter__.return_value = iter(input_data.splitlines(True))
            
            with patch('sys.stdout', new_callable=MagicMock) as mock_stdout:
                features.stream_processor(
                    window_size_sec=60,
                    window_stride_sec=60,
                    schema_path=schema_path
                )
                # Capture what was written to stdout
                written_output = "".join(call.args[0] for call in mock_stdout.write.call_args_list)
        
        # Parse the output into a list of feature records
        if written_output:
            return [json.loads(line) for line in written_output.strip().split('\n')]
        return []

    def test_schema_validation(self):
        """Test that valid events are processed and invalid ones are skipped."""
        base_ts = datetime.now(timezone.utc)
        valid_event = {
            "timestamp": base_ts.isoformat(),
            "replay_ts": base_ts.isoformat(),
            "host": "host1",
            "level": "INFO",
            "component": "sshd",
            "message": "Accepted password for user",
            "template_id": "T001",
            "session_id": "S001"
        }
        invalid_event = {"message": "this is not a valid event"}

        events = [valid_event, invalid_event, valid_event]
        results = self._run_stream_processor(events, schema_path='parsed_log.schema.json')
        
        # We expect one window with features from the two valid events
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['event_count'], 2)

    def test_deterministic_windowing(self):
        """Test that events are correctly assigned to tumbling windows."""
        base_ts = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        def create_event(offset_sec):
            ts = base_ts + timedelta(seconds=offset_sec)
            return {
                "timestamp": ts.isoformat(), "replay_ts": ts.isoformat(), "host": "host1",
                "level": "INFO", "component": "sshd", "message": "msg",
                "template_id": "T001", "session_id": "S001"
            }

        events = [
            create_event(10),  # Window 1
            create_event(30),  # Window 1
            create_event(70),  # Window 2
            create_event(80),  # Window 2
            create_event(150)  # Window 3
        ]
        
        results = self._run_stream_processor(events)
        
        # We expect 3 windows to be processed
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]['event_count'], 2) # First window
        self.assertEqual(results[1]['event_count'], 2) # Second window
        self.assertEqual(results[2]['event_count'], 1) # Third window

    def test_cep_rules(self):
        """Test CEP rules: component churn, unseen templates, and bursts."""
        base_ts = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        def create_event(offset_sec, component, template_id):
            ts = base_ts + timedelta(seconds=offset_sec)
            return {
                "timestamp": ts.isoformat(), "replay_ts": ts.isoformat(), "host": "host1",
                "level": "INFO", "component": component, "message": "msg",
                "template_id": template_id, "session_id": "S001"
            }

        # Window 1: baseline
        events = [create_event(10, "sshd", "T001"), create_event(20, "kernel", "T002")]
        # Window 2: 1 new component, 1 disappeared. Churn = 2. New template T003.
        events.extend([create_event(70, "sshd", "T001"), create_event(80, "cron", "T003")])
        # Window 3: High volume burst
        events.extend([create_event(130, "sshd", "T001") for _ in range(30)])

        results = self._run_stream_processor(events)

        self.assertEqual(len(results), 3)

        # Test window 1 (baseline)
        self.assertEqual(results[0]['component_churn'], 2) # Initially, all components are new
        self.assertEqual(results[0]['is_unseen_template'], True)
        self.assertEqual(results[0]['is_burst'], False)

        # Test window 2 (churn and new template)
        self.assertEqual(results[1]['component_churn'], 2) # kernel disappeared, cron appeared
        self.assertEqual(results[1]['is_unseen_template'], True) # T003 is new
        self.assertEqual(results[1]['is_burst'], False)

        # Test window 3 (burst)
        self.assertEqual(results[2]['component_churn'], 1) # cron disappeared
        self.assertEqual(results[2]['is_unseen_template'], False) # T001 was seen before
        self.assertEqual(results[2]['is_burst'], True) # Event count (30) is a spike

if __name__ == '__main__':
    unittest.main()
