from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="D:\Bootcamp IYKRA\Final Project\TESTING_KAFKA\kafka\key\keyfile.json"

dataset_name = 'test_onlinefraud'
table_name = 'v3_online_payment'

client = bigquery.Client()
client.create_dataset(dataset_name, exists_ok=True)
dataset = client.dataset(dataset_name)

schema = [
    bigquery.SchemaField('step', 'INT64'),
    bigquery.SchemaField('type', 'STRING'),
    bigquery.SchemaField('amount', 'FLOAT64'),
    bigquery.SchemaField('nameOrig', 'STRING'),
    bigquery.SchemaField('oldbalanceOrg', 'FLOAT64'),
    bigquery.SchemaField('newbalanceOrig', 'FLOAT64'),
    bigquery.SchemaField('nameDest', 'STRING'),
    bigquery.SchemaField('oldbalanceDest', 'FLOAT64'),
    bigquery.SchemaField('newbalanceDest', 'FLOAT64'),
    bigquery.SchemaField('isFraud', 'INT64'),
    bigquery.SchemaField('isFlaggedFraud', 'INT64'),
]

table_ref = bigquery.TableReference(dataset, table_name)
table = bigquery.Table(table_ref, schema=schema)
client.create_table(table, exists_ok=True)

def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "online_payment.avro.consumer.2",
                       "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["com.online.payment"])

    while True:
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message is not None:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                # INSERT STREAM TO BIGQUERY
                client.insert_rows(table, [message.value()])
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()