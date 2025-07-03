import requests
import os
import math
import json
import hashlib
import concurrent.futures

# Tamanho de cada pedaço do arquivo (chunk)
CHUNK_SIZE = 128 * 1024 * 1024  # 128MB

# Calcula o hash MD5 dos dados
def calcular_md5(data):
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()

# Cria o cabeçalho com informações sobre o chunk
def criar_cabecalho(chunk_index, filename, total_chunks, md5_hash):
    header = json.dumps({
        "chunk_index": chunk_index,
        "filename": filename,
        "total_chunks": total_chunks,
        "md5": md5_hash
    }).encode('utf-8') + b'\n'
    return header

# Separa o cabeçalho dos dados do chunk
def separar_cabecalho(chunk_data):
    header_end = chunk_data.find(b'\n')
    header = json.loads(chunk_data[:header_end].decode('utf-8'))
    body = chunk_data[header_end + 1:]
    return header, body

# Faz upload de um arquivo dividido em chunks
def upload_file(file_path):
    filename = os.path.basename(file_path)
    
    # Solicita os nós disponíveis para upload
    response = requests.post("http://localhost:5000/upload_request", json={"filename": filename})
    if response.status_code == 200:
        node_urls = response.json()["node_urls"]
        num_nodes = len(node_urls)

        file_size = os.path.getsize(file_path)
        num_chunks = math.ceil(file_size / CHUNK_SIZE)

        with open(file_path, 'rb') as f:
            for chunk_index in range(num_chunks):
                chunk_data = f.read(CHUNK_SIZE)
                md5_hash = calcular_md5(chunk_data)
                header = criar_cabecalho(chunk_index, filename, num_chunks, md5_hash)
                chunk_completo = header + chunk_data

                # Alterna entre os nós disponíveis
                target_node = node_urls[chunk_index % num_nodes]

                files = {"file": (f"{filename}.chunk{chunk_index}", chunk_completo)}
                data = {"filename": filename, "chunk_index": chunk_index}
                
                # Faz upload do chunk
                requests.post(f"{target_node}/upload", files=files, data=data)

        print(f"Arquivo '{filename}' enviado em {num_chunks} chunks para os nós com sucesso.")
    else:
        print("Erro ao obter nós do manager.")

# FDownload do arquivo
def download_file(filename, destino):
    response = requests.get(f"http://localhost:5000/download_location/{filename}")
    if response.status_code == 200:
        chunk_locations = response.json()

        def baixar_chunk(chunk_index, node_url):
            r = requests.get(f"{node_url}/download/{filename}.chunk{chunk_index}")
            return r.content

        # Baixa os chunks 
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_chunk = {executor.submit(baixar_chunk, idx, url): idx for idx, url in chunk_locations.items()}
            chunks_recebidos = {}

            for future in concurrent.futures.as_completed(future_to_chunk):
                idx = future_to_chunk[future]
                chunk_data = future.result()
                header, body = separar_cabecalho(chunk_data)
                
                # Verifica se o chunk está íntegro
                hash_local = calcular_md5(body)
                if header['md5'] != hash_local:
                    print(f"Erro de integridade no chunk {header['chunk_index']} do arquivo {filename}")
                    return
                chunks_recebidos[header['chunk_index']] = body

        # Junta todos os chunks e salva o arquivo completo
        with open(destino, 'wb') as f:
            for idx in sorted(chunks_recebidos.keys()):
                f.write(chunks_recebidos[idx])

        print(f"Arquivo '{filename}' baixado com sucesso para '{destino}'.")
    else:
        print("Arquivo não encontrado no manager.")

# Lista os arquivos disponíveis no sistema
def list_files():
    response = requests.get("http://localhost:5000/list")
    arquivos = response.json()
    print("Arquivos disponíveis:")
    for nome, chunks in arquivos.items():
        print(f"- {nome} (Chunks: {len(chunks)})")

# Remove um arquivo do sistema
def remove_file(filename):
    response = requests.delete(f"http://localhost:5000/remove/{filename}")
    print(response.text)

# Menu
if __name__ == "__main__":
    while True:
        comando = input("Comando (ls | rm <arquivo> | cp <origem> <destino> | sair): ").strip()
        if comando == "sair":
            break
        elif comando == "ls":
            list_files()
        elif comando.startswith("rm "):
            _, filename = comando.split(maxsplit=1)
            remove_file(filename)
        elif comando.startswith("cp "):
            partes = comando.split()
            if len(partes) == 3:
                origem, destino = partes[1], partes[2]

                # Se for copiar do sistema remoto para local
                if origem.startswith("remote:"):
                    nome_arquivo = origem.replace("remote:", "")
                    download_file(nome_arquivo, destino)

                # Se for copiar do local para o sistema remoto
                elif destino.startswith("remote:"):
                    if os.path.exists(origem):
                        upload_file(origem)
                    else:
                        print(f"Arquivo local '{origem}' não encontrado para upload.")
                else:
                    print("Para copiar de/para o sistema, use o prefixo remote: em origem ou destino.")
            else:
                print("Uso incorreto de cp. Exemplo: cp origem destino")
        else:
            print("Comando inválido.")
