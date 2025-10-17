import pika
import time
from kafka import KafkaConsumer
from config.constants import DEFAULT_PAUSE, RABBITMQ_TARGET_HOST, RABBITMQ_TARGET_QUEUE, KAFKA_SOURCE_SERVERS, KAFKA_SOURCE_TOPIC, KAFKA_SOURCE_GROUP

class KafkaToRabbitConnector:
    # --- Kafka to RabbitMQ Sync Funnction ---
    def kafka_to_rabbitmq(self):
        while True:
            connection = None
            try:
                print("[RabbitMQ]: Producer -> Connecting...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_TARGET_HOST))
                channel = connection.channel()
                channel.queue_declare(queue=RABBITMQ_TARGET_QUEUE, durable=True)
                print("[RabbitMQ]: Producer -> Connection successful.")
                
                print("[Kafka]: Consumer -> Connecting...")
                consumer = KafkaConsumer(
                KAFKA_SOURCE_TOPIC,
                bootstrap_servers=KAFKA_SOURCE_SERVERS.split(','),
                auto_offset_reset='earliest', # Empieza a leer desde el principio si es un nuevo consumidor
                group_id=KAFKA_SOURCE_GROUP
                )
                print("[Kafka]: Consumer -> Connection successful. Waiting for messages...")
                for message in consumer:
                    print(f"[Kafka]: Consumer ->  Received: Topic: {message.topic}, "
                        f"Partition: {message.partition}, Offset: {message.offset}, "
                        f"Key: {message.key}")
                    message_to_send = message.value.decode('utf-8')
                    print(f"[RabbitMQ]: Producer -> Message: {message_to_send}")

                    channel.basic_publish(
                        exchange='',
                        routing_key=RABBITMQ_TARGET_QUEUE,
                        body=message_to_send,
                        properties=pika.BasicProperties(delivery_mode=2) # Mensaje persistente
                    )
                    print(f"[RabbitMQ]: Producer -> Message sent -> '{message}'")
                
                time.sleep(DEFAULT_PAUSE)

            except pika.exceptions.AMQPConnectionError as e:
                print(f"[RabbitMQ]: Producer -> Connection error - {e}. Retrying in 5 seconds...")
                if connection:
                    connection.close()
                time.sleep(DEFAULT_PAUSE)
            except Exception as e:
                print(f"K-R Unexpected Error - {e}")
                break