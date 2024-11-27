from kafka import KafkaProducer
import time
import random
import json

def get_kafka_config():
    kafka_ip = input("Введите IP адрес сервера Kafka: ")
    kafka_port = input("Введите порт Kafka: ")
    topic = input("Введите название топика: ")
    return kafka_ip, kafka_port, topic

def create_kafka_producer(kafka_ip, kafka_port):
    try:
        producer = KafkaProducer(
            bootstrap_servers=f"{kafka_ip}:{kafka_port}",
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Успешно подключено к Kafka")
        return producer
    except Exception as e:
        print(f"Ошибка подключения к Kafka: {e}")
        exit(1)

def generate_random_message():
    return {
        "id": random.randint(1, 1000),
        "message": f"Random message {random.randint(1, 1000)}",
        "timestamp": time.time()
    }

def main():
    kafka_ip, kafka_port, topic = get_kafka_config()
    producer = create_kafka_producer(kafka_ip, kafka_port)

    print(f"Начало отправки сообщений в топик '{topic}'...")
    try:
        while True:
            message = generate_random_message()
            producer.send(topic, value=message)
            print(f"Сообщение отправлено: {message}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nОстановка клиента Kafka")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
