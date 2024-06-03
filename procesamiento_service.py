from sqlite3 import DataError
from confluent_kafka import Consumer, Producer, KafkaException
import json
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

producer = Producer({'bootstrap.servers': 'localhost:9092'})

consumer.subscribe(['solicitudes'])

def procesar_solicitud(solicitud):
    estados = ['recibido', 'preparando', 'entregando', 'finalizado']
    for estado in estados:
        solicitud['estado'] = estado
        producer.produce('notificaciones', value=json.dumps(solicitud).encode('utf-8'))
        producer.flush()
        time.sleep(5)  # Simulación del tiempo de procesamiento

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == DataError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    solicitud = json.loads(msg.value().decode('utf-8'))
    procesar_solicitud(solicitud)

consumer.close()
