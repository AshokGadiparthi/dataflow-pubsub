from google.cloud import pubsub_v1
from google.cloud import storage
import fastavro
from io import BytesIO
import json

def get_pubsub_schema(project_id, topic_name, subscription_name, output_bucket, output_schema_file):
    # Create Pub/Sub client
    subscriber = pubsub_v1.SubscriberClient()

    # Create Storage client
    storage_client = storage.Client()

    # Create subscription if not exists
    subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"
    try:
        subscriber.create_subscription(subscription_path, topic=topic_name)
    except:
        pass

    # Pull messages from subscription
    response = subscriber.pull(subscription_path, max_messages=100)
    messages = response.received_messages

    # Extract schema from Avro messages
    avro_schema = extract_avro_schema(messages)

    # Save Avro schema to Google Cloud Storage
    bucket_name, object_name = output_bucket, output_schema_file
    save_avro_schema_to_storage(avro_schema, bucket_name, object_name, storage_client)

def extract_avro_schema(messages):
    schema_fields = {}

    for message in messages:
        data = message.message.data
        avro_message = fastavro.reader(BytesIO(data))
        
        # Merge Avro schema fields
        for field in avro_message.schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]
            if field_name not in schema_fields:
                schema_fields[field_name] = field_type

    # Create Avro schema from merged fields
    avro_schema = {"type": "record", "name": "Message", "fields": [{"name": k, "type": v} for k, v in schema_fields.items()]}

    return avro_schema

def save_avro_schema_to_storage(avro_schema, bucket_name, object_name, storage_client):
    schema_content = json.dumps(avro_schema)
    
    # Upload Avro schema to Google Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(schema_content, content_type="application/json")

    print(f"Avro schema saved to gs://{bucket_name}/{object_name}")

if __name__ == "__main__":
    # Replace with your Google Cloud project and Pub/Sub details
    project_id = "your-project-id"
    topic_name = "your-topic-name"
    subscription_name = "your-subscription-name"
    output_bucket = "your-output-bucket"
    output_schema_file = "avro-schema.avsc"

    get_pubsub_schema(project_id, topic_name, subscription_name, output_bucket, output_schema_file)
