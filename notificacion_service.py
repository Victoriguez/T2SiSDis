from sqlite3 import DataError
from flask import Flask, request, jsonify
from confluent_kafka import Consumer
import json
import smtplib

app = Flask(__name__)
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['notificaciones'])
notificaciones = {}

@app.route('/notificacion/<int:id>', methods=['GET'])
def notificacion(id):
    if id in notificaciones:
        return jsonify(notificaciones[id]), 200
    else:
        return jsonify({'error': 'Solicitud no encontrada'}), 404

def enviar_correo(notificacion):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('tu_email@gmail.com', 'tu_contraseña')
    message = f"Subject: Actualización de Pedido\n\nEstado de su pedido: {notificacion['estado']}"
    server.sendmail('tu_email@gmail.com', notificacion['correo'], message)
    server.quit()

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
    notificacion = json.loads(msg.value().decode('utf-8'))
    notificaciones[notificacion['id']] = notificacion
    enviar_correo(notificacion)

if __name__ == '__main__':
    app.run(debug=True, port=5003)
