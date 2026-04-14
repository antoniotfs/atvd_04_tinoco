import json
import time
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'airports_raw'
BATCH_WINDOW_SECONDS = 30
BRONZE_DIR = 'data/bronze/airports'

def run_consumer_bronze():
    os.makedirs(BRONZE_DIR, exist_ok=True)
    
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        group_id='bronze_group',
        auto_offset_reset='earliest',
        enable_auto_commit=False, # We will commit manually
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Consuming Topic {KAFKA_TOPIC} in {BATCH_WINDOW_SECONDS}s micro-batches...")

    current_batch = []
    window_start = time.time()
    
    try:
        while True:
            # poll with timeout 1.0s to allow timer to tick even without messages
            msg_pack = consumer.poll(timeout_ms=1000)
            
            for tp, messages in msg_pack.items():
                for msg in messages:
                    current_batch.append(msg.value)
            
            # Check window time
            elapsed = time.time() - window_start
            
            if elapsed >= BATCH_WINDOW_SECONDS:
                if current_batch:
                    timestamp_str = int(time.time())
                    print(f"\n[Micro-batch {BATCH_WINDOW_SECONDS}s elapsed] Processing {len(current_batch)} records.")
                    
                    # Convert to PyArrow table
                    df = pd.DataFrame(current_batch)
                    table = pa.Table.from_pandas(df)
                    
                    file_path = os.path.join(BRONZE_DIR, f"batch_{timestamp_str}.parquet")
                    pq.write_table(table, file_path)
                    print(f"Saved: {file_path}")
                    
                    # Manual Commit
                    consumer.commit()
                    print("Offset committed.")
                    
                    # Reset batch
                    current_batch = []
                else:
                    print(f"[{elapsed:.1f}s] Window elapsed. No records received.")
                
                window_start = time.time()
                
    except KeyboardInterrupt:
        print("Stopping Bronze Consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    run_consumer_bronze()
