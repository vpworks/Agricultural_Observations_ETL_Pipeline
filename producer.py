import psycopg2
import json
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "agri-data"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="agribase",
    user="postgres",
    password="Carter.C9",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Query data
cursor.execute("SELECT * FROM agridata")

# Get column names
columns = [desc[0] for desc in cursor.description]

# Fetch rows and send to Kafka
for row in cursor.fetchall():
    record = dict(zip(columns, row))
    producer.send(TOPIC_NAME, value=record)
    print("Sent:", record)

producer.flush()
producer.close()
cursor.close()
conn.close()
