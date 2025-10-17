import threading
from connector.kafka_to_rabbitmq import KafkaToRabbitConnector
from connector.rabbitmq_to_kafka import RabbitToKafkaConnector
from config.constants import DUAL_MODE, MODE

def start_kafka_to_rabbit():
	kr = KafkaToRabbitConnector()
	print("Starting K-R mode (Kafka to RabbitMQ)")
	kr.kafka_to_rabbitmq()

def start_rabbit_to_kafka():
	rk = RabbitToKafkaConnector()
	print("Starting R-K mode (RabbitMQ to Kafka)")
	rk.rabbitmq_to_kafka()

def start_dual_mode():
	print("Running in DUAL_MODE")
	kafka_rabbit_thread = threading.Thread(target=start_kafka_to_rabbit)
	rabbit_kafka_thread = threading.Thread(target=start_rabbit_to_kafka)

	kafka_rabbit_thread.start()
	rabbit_kafka_thread.start()

	kafka_rabbit_thread.join()
	rabbit_kafka_thread.join()

# --- Iniciar ambos procesos en hilos separados ---
if __name__ == "__main__":
	print("Starting application...")
	if DUAL_MODE:
		start_dual_mode()
	else:
		if MODE == 'K-R':
			print("Running in K-R mode (Kafka to RabbitMQ)")
			start_kafka_to_rabbit()
		elif MODE == 'R-K':
			print("Running in R-K mode (RabbitMQ to Kafka)")
			start_rabbit_to_kafka()
		else:
			print("Invalid MODE configuration. Please set MODE to 'K-R' or 'R-K'.")
			print("Exiting application.")
			exit(1)