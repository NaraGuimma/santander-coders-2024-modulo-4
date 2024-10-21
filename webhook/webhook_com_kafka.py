# Databricks notebook source
pip install flask

# COMMAND ----------

pip install kafka-python

# COMMAND ----------

from flask import Flask, request
from kafka import KafkaProducer
import json

# COMMAND ----------

# Função de serialização para converter em bytes
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Criando o produtor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Endereço do Kafka
    value_serializer=serializer  # Serializando mensagens
)


app = Flask("app")

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    print(data)
    producer.send('webhook', value=data)
    return 'Webhook OK', 200
  

app.run()

# COMMAND ----------


