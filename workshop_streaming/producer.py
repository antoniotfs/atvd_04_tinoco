import time
import json
import requests
import csv
from io import StringIO
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'airports_raw'
DATASET_URL = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat'

CHUNK_SIZE = 25
CHUNK_INTERVAL_SECONDS = 5
RECORD_INTERVAL_SECONDS = 0.15

COLUMNS = [
    'airport_id', 'name', 'city', 'country', 'iata', 'icao', 'latitude',
    'longitude', 'altitude', 'timezone', 'dst', 'tz_database_time_zone',
    'type', 'source'
]

def get_data():
    print(f"Downloading dataset from {DATASET_URL}...")
    response = requests.get(DATASET_URL)
    response.raise_for_status()
    
    csv_data = StringIO(response.text)
    reader = csv.reader(csv_data)
    records = []
    for row in reader:
        # Create a dict from the row
        record = dict(zip(COLUMNS, row))
        records.append(record)
    return records

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    records = get_data()
    total_records = len(records)
    print(f"Dataset downloaded. Total records: {total_records}")

    chunk_number = 1
    for i in range(0, total_records, CHUNK_SIZE):
        chunk = records[i : i + CHUNK_SIZE]
        print(f"\n--- Sending Chunk {chunk_number} ({len(chunk)} records) ---")
        
        for pos, record in enumerate(chunk, start=1):
            # Inject metadata as requested
            record['chunk_number'] = chunk_number
            record['chunk_position'] = pos
            
            producer.send(KAFKA_TOPIC, value=record)
            
            # Very small pause to see records arriving "1 by 1" during presentation
            time.sleep(RECORD_INTERVAL_SECONDS)
        
        producer.flush()
        print(f"Chunk {chunk_number} completely sent. Waiting {CHUNK_INTERVAL_SECONDS} seconds...")
        time.sleep(CHUNK_INTERVAL_SECONDS)
        chunk_number += 1
        
    print("All chunks sent. Closing producer.")
    producer.close()

if __name__ == '__main__':
    run_producer()
