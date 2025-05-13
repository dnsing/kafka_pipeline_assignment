import pytest
from main import transform_event

def test_transform_event_valid_timestamp_input():
    input_event =     {"message_id": 0, "sender": "alice", "text": "Good morning!", 'timestamp': "2025-05-R-08T08:00:00Z"}
    expected_output = {"message_id": 0, "sender": "alice", "text": "Good morning!", "timestamp": "2025-05-08T08:00:00Z"}
    assert transform_event(input_event) == expected_output

def test_transform_event_missing_fields():
    input_event =     {"message_id": 0, "sender": "alice", 'timestamp': "2025-05-08T08:00:00Z"}
    expected_output = {"message_id": 0, "sender": "alice", "text": "Good morning!", "timestamp": "2025-05-08T08:00:00Z"}
    assert transform_event(input_event) == expected_output
