from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('enriched_metadata_topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', group_id='automation_group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def trigger_downstream_action(metadata):
    if metadata.get('enrichment') == 'PII':
        print(f"Triggering PII compliance actions for entity {metadata['entity_id']}")
    else:
        print(f"No action required for entity {metadata['entity_id']}")

for message in consumer:
    trigger_downstream_action(message.value)