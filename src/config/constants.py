import os

# --- Configuración obtenida de variables de entorno ---
DUAL_MODE = os.getenv('DUAL_MODE', 'true').lower() == 'true'
MODE = os.getenv('MODE', 'K-R')  # Valores posibles: K-R / R-K
DEFAULT_PAUSE = int(os.getenv('DEFAULT_PAUSE', '5'))  # Pausa por defecto en segundos
KAFKA_SOURCE_SERVERS = os.getenv('KAFKA_SOURCE_SERVERS', 'localhost:9092')
KAFKA_SOURCE_TOPIC = os.getenv('KAFKA_SOURCE_TOPIC', 'kafka-source-topic')
KAFKA_SOURCE_GROUP = os.getenv('KAFKA_SOURCE_GROUP', 'my-group')
RABBITMQ_TARGET_HOST = os.getenv('RABBITMQ_TARGET_HOST', 'localhost')
RABBITMQ_TARGET_QUEUE = os.getenv('RABBITMQ_TARGET_QUEUE', 'target-queue')
RABBITMQ_SOURCE_HOST = os.getenv('RABBITMQ_SOURCE_HOST', 'localhost')
RABBITMQ_SOURCE_QUEUE = os.getenv('RABBITMQ_SOURCE_QUEUE', 'source-queue')
KAFKA_TARGET_SERVERS = os.getenv('KAFKA_TARGET_SERVERS', 'localhost:9092')
KAFKA_TARGET_TOPIC = os.getenv('KAFKA_TARGET_TOPIC', 'kafka-target-topic')