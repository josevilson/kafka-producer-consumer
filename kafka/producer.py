import time
from faker import Faker
from kafka import KafkaProducer
import json

# Instancia o Faker para gerar dados aleatórios
fake = Faker()

# Função para gerar dados de pedido
def generate_order():
    return {
        "order_id": fake.uuid4(),
        "user_id": fake.random_int(min=1, max=1000),
        "product": fake.word(),
        "price": round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2),
        "quantity": fake.random_int(min=1, max=5),
        "timestamp": fake.iso8601()
    }

# Configura o Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Endereço do Kafka (usando porta exposta no docker-compose)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializa os dados como JSON
)

# Envia dados continuamente para o tópico "pedidos"
try:
    while True:
        # Gera um novo pedido
        order_data = generate_order()
        
        # Envia para o tópico "pedidos"
        producer.send('pedidos', value=order_data)
        
        print(f"Enviado: {order_data}")
        
        # Pausa de 1 segundo entre cada envio
        time.sleep(1)

except KeyboardInterrupt:
    print("Produção de dados interrompida.")

finally:
    # Fecha o producer quando o script é encerrado
    producer.close()
