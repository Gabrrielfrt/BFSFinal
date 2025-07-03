import concurrent.futures
import os
import time
import hashlib
from cliente import upload_file, download_file

# Calcula o checksum de um arquivo
def calcular_checksum(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

# Faz upload de vários arquivos usando multithread
def upload_multithread(directory, max_workers):
    arquivos = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    checksums = {arquivo: calcular_checksum(arquivo) for arquivo in arquivos}

    total_upload_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(upload_file, arquivo): arquivo for arquivo in arquivos}

        for future in concurrent.futures.as_completed(future_to_file):
            arquivo = future_to_file[future]
            try:
                start_time = time.time()
                future.result()
                elapsed_time = time.time() - start_time
                print(f"Upload de {os.path.basename(arquivo)} concluído em {elapsed_time:.2f} segundos.")
            except Exception as e:
                print(f"Erro durante upload: {e}")

    total_upload_time = time.time() - total_upload_start
    print(f"Upload de {len(arquivos)} arquivos concluído em {total_upload_time:.2f} segundos.")

    return checksums, total_upload_time

# Faz upload sequencial de arquivos
def upload_sequencial(directory):
    arquivos = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    checksums = {arquivo: calcular_checksum(arquivo) for arquivo in arquivos}

    total_upload_start = time.time()

    for arquivo in arquivos:
        try:
            start_time = time.time()
            upload_file(arquivo)
            elapsed_time = time.time() - start_time
            print(f"Upload de {os.path.basename(arquivo)} concluído em {elapsed_time:.2f} segundos.")
        except Exception as e:
            print(f"Erro durante upload: {e}")

    total_upload_time = time.time() - total_upload_start
    print(f"Upload de {len(arquivos)} arquivos concluído em {total_upload_time:.2f} segundos.")

    return checksums, total_upload_time

# Faz upload de um único arquivo
def upload_unico(file_path):
    total_upload_start = time.time()

    try:
        start_time = time.time()
        upload_file(file_path)
        elapsed_time = time.time() - start_time
        print(f"Upload de {os.path.basename(file_path)} concluído em {elapsed_time:.2f} segundos.")
    except Exception as e:
        print(f"Erro durante upload: {e}")

    total_upload_time = time.time() - total_upload_start
    print(f"Upload concluído em {total_upload_time:.2f} segundos.")

    return {file_path: calcular_checksum(file_path)}, total_upload_time

# Faz download de vários arquivos usando multithread
def download_multithread(filenames, destino_dir, max_workers):
    os.makedirs(destino_dir, exist_ok=True)

    total_download_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {}
        for filename in filenames:
            destino = os.path.join(destino_dir, filename)
            future = executor.submit(download_file, filename, destino)
            future_to_file[future] = destino

        for future in concurrent.futures.as_completed(future_to_file):
            destino = future_to_file[future]
            try:
                start_time = time.time()
                future.result()
                elapsed_time = time.time() - start_time
                print(f"Download de {os.path.basename(destino)} concluído em {elapsed_time:.2f} segundos.")
            except Exception as e:
                print(f"Erro durante download: {e}")

    total_download_time = time.time() - total_download_start
    print(f"Download de {len(filenames)} arquivos concluído em {total_download_time:.2f} segundos.")

    return total_download_time

# Verifica a integridade dos arquivos baixados
def verificar_integridade(original_checksums, destino_dir):
    erros = 0
    for original_file, original_checksum in original_checksums.items():
        filename = os.path.basename(original_file)
        downloaded_file = os.path.join(destino_dir, filename)
        downloaded_checksum = calcular_checksum(downloaded_file)

        if original_checksum != downloaded_checksum:
            print(f"Integridade falhou para {filename}!")
            erros += 1
    if erros == 0:
        print("Todos os arquivos foram transferidos com integridade.")
    else:
        print(f"{erros} arquivos falharam na verificação de integridade.")

# Menu para escolher o tipo de benchmark
if __name__ == "__main__":
    print("Benchmark")
    print("Escolha o cenário:")
    print("1 - Arquivos Pequenos em Série (100 arquivos de 512KB)")
    print("2 - Arquivos Pequenos Concorrentes (1000 arquivos de 256KB, 10 threads)")
    print("3 - Upload Único de Arquivo Grande (5GB)")
    print("4 - Arquivos Grandes em Série (10 arquivos de 2GB)")
    print("5 - Arquivos Grandes Concorrentes (5 arquivos de 4GB, 5 threads)")
    print("6 - Mistura Realista (100 pequenos + 200 grandes em paralelo)")

    opcao = input("Digite a opção: ")

    if opcao == "1":
        diretorio = 'arquivos_benchmark/pequenos'
        pasta_download = 'downloads_pequenos_serie'
        checksums, tempo_upload = upload_sequencial(diretorio)
        max_workers = 10

    elif opcao == "2":
        diretorio = 'arquivos_benchmark/concorrentes_pequenos'
        max_workers = 10
        pasta_download = 'downloads_pequenos_concorrente'
        checksums, tempo_upload = upload_multithread(diretorio, max_workers)

    elif opcao == "3":
        arquivo_unico = 'arquivos_benchmark/grandes/arquivo_grande_5GB.bin'
        pasta_download = 'downloads_unico_grande'
        checksums, tempo_upload = upload_unico(arquivo_unico)
        max_workers = 1

    elif opcao == "4":
        diretorio = 'arquivos_benchmark/sequenciais_grandes'
        pasta_download = 'downloads_grandes_serie'
        checksums, tempo_upload = upload_sequencial(diretorio)
        max_workers = 5

    elif opcao == "5":
        diretorio = 'arquivos_benchmark/concorrentes_grandes'
        max_workers = 5
        pasta_download = 'downloads_grandes_concorrente'
        checksums, tempo_upload = upload_multithread(diretorio, max_workers)

    elif opcao == "6":
        diretorio = 'arquivos_benchmark/mistura_realista'
        max_workers = 10
        pasta_download = 'downloads_misto'
        checksums, tempo_upload = upload_multithread(diretorio, max_workers)

    else:
        print("Opção inválida.")
        exit()

    nomes_arquivos = [os.path.basename(arquivo) for arquivo in checksums.keys()]
    tempo_download = download_multithread(nomes_arquivos, pasta_download, max_workers)

    verificar_integridade(checksums, pasta_download)

    print(f"Tempo total de upload: {tempo_upload:.2f} segundos.")
    print(f"Tempo total de download: {tempo_download:.2f} segundos.")
