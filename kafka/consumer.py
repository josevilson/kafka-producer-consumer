from kafka import KafkaConsumer
import json

# Configura o Kafka Consumer
consumer = KafkaConsumer(
    'pedidos',  # Nome do tópico a ser consumido
    bootstrap_servers='localhost:9092',  # Endereço do Kafka
    auto_offset_reset='earliest',  # Inicia a partir do início do tópico
    enable_auto_commit=True,  # Confirma automaticamente o deslocamento
    group_id='my-group',  # Identificação do grupo de consumidores
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Desserializa as mensagens JSON
)

print("Consumindo mensagens do tópico 'pedidos'...")

# Consome mensagens em loop
try:
    for message in consumer:
        print(f"Recebido: {message.value}")
except KeyboardInterrupt:
    print("Consumo interrompido.")
finally:
    # Fecha o consumidor ao finalizar
    consumer.close()
