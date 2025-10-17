import pika
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from config.constants import DEFAULT_PAUSE, RABBITMQ_SOURCE_HOST, RABBITMQ_SOURCE_QUEUE, KAFKA_TARGET_SERVERS, KAFKA_TARGET_TOPIC

class RabbitToKafkaConnector:
    
    # --- Productor global de Kafka ---
    # Es más eficiente tener una sola instancia del productor
    producer = None
    
    def __init__(self):
        while self.producer is None:
            try:
                print("[Kafka]: Producer -> Connecting...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_TARGET_SERVERS.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("[Kafka]: Producer -> Connection successful.")
            except KafkaError as e:
                print(f"[Kafka]: Producer -> Connection error - {e}. Retrying in 5 seconds...")
                time.sleep(DEFAULT_PAUSE)

    # --- Callback para cuando se recibe un mensaje de RabbitMQ ---
    def callback(self, ch, method, properties, body):
        message_body = body.decode('utf-8')
        print(f"[RabbitMQ]: Consumer ->  Received: '{message_body}'")
        
        # Preparamos el mensaje para Kafka (ej. en formato JSON)
        message_to_send = {
            'source': 'rabbitmq',
            'queue': RABBITMQ_SOURCE_QUEUE,
            'payload': message_body,
            'timestamp': int(time.time())
        }
        
        try:
            # Enviamos el mensaje al tópico de Kafka
            future = self.producer.send(KAFKA_TARGET_TOPIC, value=message_to_send)
            # Espera síncrona para confirmar el envío (opcional, pero bueno para logs)
            record_metadata = future.get(timeout=10)
            print(f"[Kafka]: Producer -> Message {message_to_send} sent to topic '{record_metadata.topic}'")
            
            # Confirmamos a RabbitMQ que el mensaje fue procesado (ACK)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"[Kafka]: Producer -> Message was not sent, Error {e}")
            # Rechazamos el mensaje pero no lo volvemos a encolar para evitar bucles infinitos
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # --- Lógica principal del consumidor de RabbitMQ ---
    def rabbitmq_to_kafka(self):
        connection = None
        while True:
            try:
                print("[RabbitMQ]: Consumer -> Connecting...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_SOURCE_HOST))
                channel = connection.channel()
                channel.queue_declare(queue=RABBITMQ_SOURCE_QUEUE, durable=True)
                
                # Solo procesar un mensaje a la vez para un control de flujo simple
                channel.basic_qos(prefetch_count=1)
                
                # Registrar la función callback para consumir mensajes
                channel.basic_consume(queue=RABBITMQ_SOURCE_QUEUE, on_message_callback=self.callback)
                
                print("[RabbitMQ]: Consumer -> Connection successful. Waiting for messages...")
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as e:
                print(f"[RabbitMQ]: Consumer -> Connection error - {e}. Retrying in 5 seconds...")
                if connection and not connection.is_closed:
                    connection.close()
                time.sleep(DEFAULT_PAUSE)
            except Exception as e:
                print(f"[RabbitMQ]: Consumer -> Unexpected Error - {e}")
                break