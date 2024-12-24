from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from hdfs import InsecureClient
import numpy as np
import time
import json
import os
import pandas as pd
import env_variable
from env_variable import *

print('Kafka Consumer is running...')

def create_hdfs_client():
    return InsecureClient(HDFS_URL)

def create_kafka_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id='test-group',
        auto_offset_reset='earliest',
        # enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
def write_to_hdfs_parquet(data, hdfs_path):
    # Save the DataFrame as a Parquet file locally
    local_path = f'temp/temp_{time.time()}.parquet'
    data.to_parquet(local_path, index=False, engine='pyarrow')  # or engine='fastparquet'
    
    # Upload the Parquet file to HDFS
    hdfs_client = create_hdfs_client()
    with open(local_path, 'rb') as f:
        hdfs_client.write(hdfs_path, f)
    
    # Clean up local file
    os.remove(local_path)


def has_reached_latest(consumer, topic_partitions):
    end_offsets = consumer.end_offsets(topic_partitions)  # Latest offsets for partitions
    for partition in topic_partitions:
        current_offset = consumer.position(partition)
        if current_offset < end_offsets[partition]:
            return False 
    return True
  
def consume_and_save_to_hdfs(batch_size= BATCH_SIZE):
    consumer = create_kafka_consumer()
    topic_partitions = [TopicPartition(KAFKA_TOPIC, p) for p in consumer.partitions_for_topic(KAFKA_TOPIC)]
    try:
        batch = []
        
        while True:
            message = consumer.poll(timeout_ms=20000)
            if not message:
                print('No messages fetched...')
                if has_reached_latest(consumer, topic_partitions):
                    print('Consumer has reached the latest record. Exiting...')
                    break
                continue
            
            for _, records in message.items():
                for record in records:
                    if record.value.get("end_of_stream"):
                        print("End of stream signal")
                        # consumer.commit()
                        return
                    
                    batch.append(record.value)
                    
                    if len(batch) >= batch_size:
                        df = pd.DataFrame(batch)
                        df.drop_duplicates(inplace=True, keep='first')
                        
                        # Write as Parquet to HDFS
                        parquet_hdfs_path = f'{HDFS_PATH}tripdata_{time.time()}.parquet'
                        write_to_hdfs_parquet(df, parquet_hdfs_path)
                        print(f'Batch of {batch_size} records written to HDFS in Parquet format: {parquet_hdfs_path}')
                        
                        # consumer.commit()
                        batch = []
        
        if batch:
            df = pd.DataFrame(batch)
            df.drop_duplicates(inplace=True, keep='first')
            
            parquet_hdfs_path = f'{HDFS_PATH}tripdata_{time.time()}.parquet'
            write_to_hdfs_parquet(df, parquet_hdfs_path)
            print(f'Batch of {len(batch)} records written to HDFS in Parquet format: {parquet_hdfs_path}')
            
            # consumer.commit()
    except Exception as e:
        print(f'An error occurred: {e}')
    finally:
        consumer.close()
        print('Consumer closed')

if __name__ == "__main__":
    consume_and_save_to_hdfs(batch_size= BATCH_SIZE * 10)
    print("end of consumer")
            