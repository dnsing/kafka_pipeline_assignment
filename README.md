# Setup steps
## Clone the repo
git clone https://github.com/your-username/kafka-data-pipeline.git
cd kafka-data-pipeline

## Install dependencies
pip install -r requirements.txt

## Start Kafka using Docker (for local setup)
docker-compose up -d

# Run components
## Run the Consumer
python consumer/main.py

## Run the Producer
python producer/main.py

# Examples, jsonl format
## Input
{"message_id": 0, "sender": "alice", "text": "Good morning!", "timestamp": "2025-05-08T08:00:00Z"}
{"message_id": 1, "sender": "bob", "text": "Morning, Alice!", "timestamp": "2025-05-08T08:01:00Z"}
{"message_id": 2, "sender": "carol", "text": "Hi everyone!", "timestamp": "2025-05-09T08:02:00Z"}
{"message_id": 3, "sender": "dave", "text": "Hey Carol!", "timestamp": "2025-05-09T08:03:00Z"}
{"message_id": 4, "sender": "alice", "text": "Ready for the meeting?", "timestamp": "2025-05-10T08:04:00Z"}
{"message_id": 5, "sender": "bob", "text": "Almost, just grabbing coffee.", "timestamp": "2025-05-10T08:05:00Z"}
{"message_id": 6, "sender": "carol", "text": "Same here!", "timestamp": "2025-05-10T08:06:00Z"}
{"message_id": 7, "sender": "dave", "text": "I'll join in 5 mins.", "timestamp": "2025-05-11T08:07:00Z"}
{"message_id": 8, "sender": "alice", "text": "Cool, see you all soon.", "timestamp": "2025-05-11T08:08:00Z"}
{"message_id": 9, "sender": "carol", "text": "See you!", "timestamp": "2025-05-12T08:09:00Z"}

## Output
{"message_id": 7, "sender": "dave", "text": "I'll join in 5 mins.", "timestamp": "2025-05-11T08:07:00Z"}
{"message_id": 8, "sender": "alice", "text": "Cool, see you all soon.", "timestamp": "2025-05-11T08:08:00Z"}
{"message_id": 9, "sender": "carol", "text": "See you!", "timestamp": "2025-05-12T08:09:00Z"}

# Result
The consumer console will print the matching condition of the transformed data (filter by 48 hours old by today 5/12/25)

# Considerations
- Please take into account the timestap in the data/input.jsonl, this directly affect the program output if there is not match no results will be store in the data/output.jsonl file. input.jsonl file can be easily modify to match the condition.
- Testing: run *pytest consumer*
