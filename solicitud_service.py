from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json

app = Flask(__name__)
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

@app.route('/solicitud', methods=['POST'])
def solicitud():
    data = request.json
    data['id'] = 1  # Ejemplo: generar un ID único
    producer.produce('solicitudes', value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    producer.flush()
    return jsonify({'status': 'Solicitud recibida', 'data': data}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5001)
