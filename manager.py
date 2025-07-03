import pika
import json
import threading
import time
import os
import requests
from flask import Flask, jsonify, request

# Configurações iniciais
RABBIT_HOST = "localhost"

nodes = {}  # Armazena informações dos nós conectados
files = {}  # Armazena informações dos arquivos e suas localizações

TIMEOUT = 15  # Tempo máximo para considerar um nó como ativo
REPLICATION_QUEUE = 'replication_queue'
REPLICATION_FACTOR = 2  # Quantidade mínima de réplicas por chunk
CHUNK_SIZE = 128 * 1024 * 1024  # Tamanho padrão de chunk
LOG_FILE = 'audit_log.txt'

app = Flask(__name__)

# Conexão com o servidor RabbitMQ e criação das filas
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
channel = connection.channel()
channel.queue_declare(queue='manager_queue')
channel.queue_declare(queue=REPLICATION_QUEUE)

def clear_terminal():
    # Limpa o terminal 
    os.system('cls' if os.name == 'nt' else 'clear')

def log_operation(operation, details):
    # Registra operações no arquivo de log
    with open(LOG_FILE, 'a') as log:
        log.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {operation} - {details}\n")

def print_dashboard():
    # painel de controle no terminal atualizando a cada 3 segundos
    while True:
        clear_terminal()
        print("=" * 50)
        print("DASHBOARD - MANAGER (Atualiza a cada 3 segundos)")
        print("=" * 50)
        now = time.time()

        if not nodes:
            print("Nenhum nó conectado ainda.")
        else:
            print(f"{'Node ID':<20} {'Node URL':<30} {'Status':<10} {'Último Heartbeat'}")
            print("-" * 80)
            for node_id, info in nodes.items():
                elapsed = now - info["last_heartbeat"]
                status = "Ativo" if elapsed < TIMEOUT else "Inativo"
                print(f"{node_id:<20} {info['node_url']:<30} {status:<10} {elapsed:.1f} s atrás")

        print("\nArquivos Registrados:")
        if not files:
            print("Nenhum arquivo registrado ainda.")
        else:
            for filename, chunks in files.items():
                print(f"- {filename}:")
                for chunk_index, node_urls in chunks.items():
                    print(f"    Chunk {chunk_index}: {node_urls}")

        time.sleep(3)

def callback(ch, method, properties, body):
    # Função de tratamento de mensagens recebidas pelo RabbitMQ
    try:
        data = json.loads(body)
        if data["type"] == "heartbeat":
            node_id = data["node_id"]
            node_url = data["node_url"]
            nodes[node_id] = {"node_url": node_url, "last_heartbeat": time.time()}
        elif data["type"] == "register_file":
            filename = data["filename"]
            node_url = data["node_url"]
            chunk_index = data["chunk_index"]

            if filename not in files:
                files[filename] = {}

            if chunk_index not in files[filename]:
                files[filename][chunk_index] = []

            if node_url not in files[filename][chunk_index]:
                files[filename][chunk_index].append(node_url)
                log_operation("REGISTER", f"{filename} - Chunk {chunk_index} registrado em {node_url}")

            if len(files[filename][chunk_index]) < REPLICATION_FACTOR:
                replicate_file(filename, chunk_index)
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

def replicate_file(filename, chunk_index):
    # Função para replicar chunks que não atingiram o fator de replicação
    available_nodes = [info['node_url'] for node_id, info in nodes.items() if time.time() - info['last_heartbeat'] < TIMEOUT]
    current_nodes = files[filename][chunk_index]

    # Seleciona candidatos para replicação
    candidates = [node for node in available_nodes if node not in current_nodes]
    replicas_needed = REPLICATION_FACTOR - len(current_nodes)

    if replicas_needed <= 0 or not candidates:
        return

    for node_url in candidates[:replicas_needed]:
        replication_data = {
            "type": "replicate",
            "filename": filename,
            "chunk_index": chunk_index,
            "source_node_url": current_nodes[0],
            "target_node_url": node_url
        }
        channel.basic_publish(exchange='', routing_key=REPLICATION_QUEUE, body=json.dumps(replication_data))
        files[filename][chunk_index].append(node_url)
        log_operation("REPLICATE", f"{filename} - Chunk {chunk_index} replicado para {node_url}")

def consume_queue():
    # Consome mensagens da fila manager_queue
    channel.basic_consume(queue='manager_queue', on_message_callback=callback, auto_ack=True)
    print("Manager escutando a fila manager_queue...")
    channel.start_consuming()

def verify_integrity():
    # Verifica periodicamente a integridade dos arquivos e substitui réplicas de nós inativos
    while True:
        available_nodes = {info['node_url'] for node_id, info in nodes.items() if time.time() - info['last_heartbeat'] < TIMEOUT}
        for filename, chunks in files.items():
            for chunk_index, node_urls in chunks.items():
                for node_url in node_urls:
                    if node_url not in available_nodes:
                        print(f"Nó {node_url} falhou. Verificando necessidade de ressincronização de {filename} - Chunk {chunk_index}.")
                        log_operation("NODE FAILURE", f"{node_url} falhou. Verificando {filename} - Chunk {chunk_index}")
                        replicate_file(filename, chunk_index)
        time.sleep(10)

@app.route('/list', methods=['GET'])
def list_files():
    # Retorna todos os arquivos e a distribuição de seus chunks
    return jsonify(files)

@app.route('/upload_request', methods=['POST'])
def upload_request():
    # Retorna os nós ativos disponíveis para upload
    data = request.get_json()
    filename = data.get('filename')

    active_nodes = [info['node_url'] for node_id, info in nodes.items() if time.time() - info['last_heartbeat'] < TIMEOUT]
    if active_nodes:
        return jsonify({"node_urls": active_nodes})
    return "Nenhum nó disponível no momento.", 503

@app.route('/download_location/<filename>', methods=['GET'])
def download_location(filename):
    # Retorna a localização dos chunks disponíveis de um arquivo
    if filename in files and files[filename]:
        response = {}
        for chunk_index, node_urls in files[filename].items():
            for node_url in node_urls:
                if any(node['node_url'] == node_url and time.time() - node['last_heartbeat'] < TIMEOUT for node in nodes.values()):
                    response[chunk_index] = node_url
                    break  # Garante que retornamos apenas um nó ativo por chunk
        if response:
            return jsonify(response)
    return "Arquivo não encontrado.", 404

@app.route('/remove/<filename>', methods=['DELETE'])
def remove_file(filename):
    # Remove um arquivo do sistema (de todos os nós e do registro)
    if filename in files:
        for chunk_index, node_urls in files[filename].items():
            chunk_filename = f"{filename}.chunk{chunk_index}"
            for node_url in node_urls:
                try:
                    requests.delete(f"{node_url}/delete/{chunk_filename}")
                except Exception as e:
                    print(f"Falha ao remover {chunk_filename} de {node_url}: {e}")

        del files[filename]
        log_operation("REMOVE", f"{filename} removido do sistema.")
        return f"Arquivo '{filename}' removido do sistema."
    return "Arquivo não encontrado.", 404

if __name__ == "__main__":
    # Inicializa as threads do sistema
    threading.Thread(target=consume_queue, daemon=True).start()
    threading.Thread(target=print_dashboard, daemon=True).start()
    threading.Thread(target=verify_integrity, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
