from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import os
from math import log2
from pathlib import Path

from pyflink.common import Time, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, KinesisStreamSource, JdbcSink
from pyflink.datastream.window import TumblingEventTimeWindows
from collections import Counter
from jsonschema import validate, ValidationError

from models.anomaly.iforest import IForestModel, IForestConfig

class SchemaValidator:
    """Validates log events against schema."""
    def __init__(self, schema_path: str = None):
        if schema_path is None:
            schema_path = os.path.join(
                Path(__file__).parent.parent,
                'schemas',
                'parsed_log.schema.json'
            )
        with open(schema_path) as f:
            self.schema = json.load(f)
    
    def validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate a single event against the schema."""
        try:
            validate(instance=event, schema=self.schema)
            return True
        except ValidationError:
            return False

class StreamingAdapter:
    """Cloud-agnostic streaming source adapter."""
    def __init__(self, source_type: str = 'kinesis'):
        self.source_type = source_type
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        self.env.set_parallelism(int(os.getenv('FLINK_PARALLELISM', '2')))
        self.schema_validator = SchemaValidator()

    def validate_and_deserialize(self, raw_data: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize and validate incoming data."""
        try:
            event = json.loads(raw_data.decode('utf-8'))
            if self.schema_validator.validate_event(event):
                return event
            return None
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def get_source(self, topic: str) -> object:
        """Get streaming source based on configuration."""
        if self.source_type == 'kinesis':
            return KinesisStreamSource(
                stream_name=topic,
                region=os.getenv('AWS_REGION', 'eu-west-1'),
                deserializer=self.validate_and_deserialize
            )
        elif self.source_type == 'kafka':
            return FlinkKafkaConsumer(
                topic,
                self.validate_and_deserialize,
                {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')}
            )
        else:
            raise ValueError(f'Unsupported source type: {self.source_type}')

    def get_sink(self) -> object:
        """Get database sink based on configuration."""
        jdbc_url = os.getenv('POSTGRES_URL', 'jdbc:postgresql://localhost:5432/dovah')
        properties = {
            'driver': 'org.postgresql.Driver',
            'user': os.getenv('POSTGRES_USER', 'dovah'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        insert_sql = """
        INSERT INTO window_features (
            ts, session_id, host, window_size, window_slide,
            event_count, unique_components, error_ratio,
            template_entropy, component_entropy, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        return JdbcSink.sink(
            insert_sql,
            lambda x: Row(
                x['ts'], x['session_id'], x['host'],
                x['window_size'], x['window_slide'],
                x['event_count'], x['unique_components'],
                x['error_ratio'], x['template_entropy'],
                x['component_entropy']
            ),
            properties
        )

def calculate_entropy(items: List[str]) -> float:
    """Calculate Shannon entropy of a list of items."""
    counts = Counter(items)
    total = len(items)
    return -sum((count/total) * log2(count/total) for count in counts.values())

class WindowFeatures:
    """Compute window-based features from log stream."""
    def __init__(self, window_size: int = 60, window_slide: int = 60):
        self.window_size = window_size  # seconds
        self.window_slide = window_slide  # seconds

    def compute_features(self, stream):
        """Compute windowed features from event stream."""
        return stream \
            .key_by(lambda x: (x['host'], x['session_id'])) \
            .window(TumblingEventTimeWindows.of(Time.seconds(self.window_size))) \
            .process(lambda events, ctx: {
                'ts': ctx.window.end,
                'session_id': events[0]['session_id'],
                'host': events[0]['host'],
                'window_size': self.window_size,
                'window_slide': self.window_slide,
                'event_count': len(events),
                'unique_components': len(set(e['component'] for e in events)),
                'error_ratio': len([e for e in events if e.get('level') == 'ERROR']) / len(events),
                'template_entropy': calculate_entropy([e['template_id'] for e in events]),
                'component_entropy': calculate_entropy([e['component'] for e in events])
            })

def main():
    """Main streaming job entry point."""
    # Initialize streaming adapter (AWS Kinesis default)
    adapter = StreamingAdapter(source_type=os.getenv('STREAM_SOURCE_TYPE', 'kinesis'))
    
    # Initialize anomaly detector
    model_path = os.getenv('IFOREST_MODEL_PATH')
    if model_path and os.path.exists(model_path):
        iforest = IForestModel.load(model_path)
    else:
        iforest = IForestModel()
    
    # Get source stream with schema validation
    source = adapter.get_source(
        topic=os.getenv('LOG_STREAM_NAME', 'logs_parsed_hdfs')
    )
    
    # Create base stream
    stream = adapter.env.add_source(source)
    
    # Filter out invalid events
    valid_stream = stream.filter(lambda x: x is not None)
    
    # Compute window features
    window_features = WindowFeatures()
    features_stream = window_features.compute_features(valid_stream)
    
    # Score windows with anomaly detection
    def score_window(events: List[Dict]) -> List[Dict]:
        scores = iforest.predict(events)
        for event in events:
            if event['session_id'] in scores:
                event['anomaly_score'] = scores[event['session_id']]['score']
        return events
    
    scored_stream = features_stream.map(score_window)
    
    # Add sink to window_features table
    features_stream.add_sink(adapter.get_sink())
    
    # Add sink for detections
    detections_sink = JdbcSink.sink(
        """INSERT INTO detections (
            ts, source, host, session_id, score, model_version,
            schema_ver, created_at
        ) VALUES (?, 'iforest', ?, ?, ?, ?, '1.0.0', CURRENT_TIMESTAMP)""",
        lambda x: Row(
            x['ts'], x['host'], x['session_id'],
            x['anomaly_score'], iforest.config.n_estimators
        ),
        {
            'driver': 'org.postgresql.Driver',
            'url': os.getenv('POSTGRES_URL', 'jdbc:postgresql://localhost:5432/dovah'),
            'user': os.getenv('POSTGRES_USER', 'dovah'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
    )
    scored_stream.add_sink(detections_sink)
    
    # Execute
    adapter.env.execute('DOVAH Window Features')

if __name__ == '__main__':
    main()