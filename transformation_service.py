
from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('metadata_topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', group_id='transformation_group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def transform_metadata(metadata):
    # Example transformation: Add a timestamp and enriched data
    print(metadata)
    metadata['transformed'] = True
    metadata['timestamp'] = '2024-08-12T12:00:00Z'
    metadata['enrichment'] = metadata['type']
    return metadata

for message in consumer:
    transformed_metadata = transform_metadata(message.value)
    producer.send('enriched_metadata_topic', transformed_metadata)
    producer.flush()