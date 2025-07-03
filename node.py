import os
import threading
import time
import pika
import json
import requests
from flask import Flask, request, send_file

# Configurações básicas do nó
LOCAL_IP = "127.0.0.1"
PORTA_PADRAO = 5001
NODE_ID = f"node_{PORTA_PADRAO}"
NODE_URL = f"http://{LOCAL_IP}:{PORTA_PADRAO}"

RABBIT_HOST = "localhost"

# Cria a pasta de armazenamento se não existir
STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

app = Flask(__name__)

# Conexão inicial com o RabbitMQ para enviar heartbeats
heartbeat_connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
heartbeat_channel = heartbeat_connection.channel()
heartbeat_channel.queue_declare(queue='manager_queue')

@app.route("/upload", methods=["POST"])
def upload():
    # Recebe o arquivo e salva no storage
    file = request.files["file"]
    filename = request.form["filename"]
    chunk_index = int(request.form["chunk_index"])

    chunk_filename = f"{filename}.chunk{chunk_index}"
    file.save(os.path.join(STORAGE_DIR, chunk_filename))

    # Registra o chunk no manager
    local_connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    local_channel = local_connection.channel()
    local_channel.queue_declare(queue='manager_queue')

    data = {
        "type": "register_file",
        "filename": filename,
        "chunk_index": chunk_index,
        "node_url": NODE_URL
    }

    local_channel.basic_publish(exchange='', routing_key='manager_queue', body=json.dumps(data))
    local_connection.close()

    return "Chunk recebido", 200

@app.route("/delete/<chunk_filename>", methods=["DELETE"])
def delete_chunk(chunk_filename):
    # Exclui o chunk do storage
    file_path = os.path.join(STORAGE_DIR, chunk_filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Arquivo {chunk_filename} removido do storage.")
        return f"{chunk_filename} removido com sucesso.", 200
    else:
        print(f"Arquivo {chunk_filename} não encontrado no storage.")
        return f"{chunk_filename} não encontrado.", 404

@app.route("/download/<chunk_filename>")
def download(chunk_filename):
    # Faz o download do chunk
    return send_file(os.path.join(STORAGE_DIR, chunk_filename))

@app.route("/replicate", methods=["POST"])
def replicate():
    # Replica um chunk de outro nó
    filename = request.form["filename"]
    chunk_index = int(request.form["chunk_index"])
    source_node = request.form["source_node"]

    chunk_filename = f"{filename}.chunk{chunk_index}"
    r = requests.get(f"{source_node}/download/{chunk_filename}")
    with open(os.path.join(STORAGE_DIR, chunk_filename), 'wb') as f:
        f.write(r.content)

    # Registra a réplica no manager
    local_connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    local_channel = local_connection.channel()
    local_channel.queue_declare(queue='manager_queue')

    data = {
        "type": "register_file",
        "filename": filename,
        "chunk_index": chunk_index,
        "node_url": NODE_URL
    }

    local_channel.basic_publish(exchange='', routing_key='manager_queue', body=json.dumps(data))
    local_connection.close()

    return "Réplica criada", 200

def send_heartbeat():
    # Envia heartbeat para o manager periodicamente
    while True:
        try:
            data = {
                "type": "heartbeat",
                "node_id": NODE_ID,
                "node_url": NODE_URL
            }
            heartbeat_channel.basic_publish(exchange='', routing_key='manager_queue', body=json.dumps(data))
        except Exception as e:
            print(f"Erro ao enviar heartbeat: {e}")
        time.sleep(5)

def consume_replication_queue():
    # Escuta a fila de replicação para receber ordens de replicar chunks
    replication_connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    replication_channel = replication_connection.channel()
    replication_channel.queue_declare(queue='replication_queue')

    def replication_callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            if data["type"] == "replicate":
                # Faz a replicação local
                requests.post(f"{NODE_URL}/replicate", data={
                    "filename": data["filename"],
                    "chunk_index": data["chunk_index"],
                    "source_node": data["source_node_url"]
                })
        except Exception as e:
            print(f"Erro ao processar replicacao: {e}")

    replication_channel.basic_consume(queue='replication_queue', on_message_callback=replication_callback, auto_ack=True)
    print(f"Nó {NODE_ID} escutando fila de replicacao...")
    replication_channel.start_consuming()

# Inicia as threads de heartbeat e de consumo da fila de replicação
threading.Thread(target=send_heartbeat, daemon=True).start()
threading.Thread(target=consume_replication_queue, daemon=True).start()

if __name__ == "__main__":
    print(f"Nó iniciado: {NODE_ID} @ {NODE_URL} → Enviando heartbeats para {RABBIT_HOST}")
    app.run(host="0.0.0.0", port=PORTA_PADRAO)
