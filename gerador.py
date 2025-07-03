import os
import random

def gerar_arquivo(destino, tamanho_bytes):
    """Gera um arquivo com conteúdo aleatório de tamanho especificado."""
    with open(destino, 'wb') as f:
        f.write(os.urandom(tamanho_bytes))

def criar_arquivos_benchmark(base_dir, opcoes):
    os.makedirs(base_dir, exist_ok=True)

    if '1' in opcoes:
        pequenos_dir = os.path.join(base_dir, 'pequenos')
        os.makedirs(pequenos_dir, exist_ok=True)
        for i in range(100):
            gerar_arquivo(os.path.join(pequenos_dir, f'arquivo_pequeno_{i+1}.bin'), 512 * 1024)

    if '2' in opcoes:
        concorrentes_pequenos_dir = os.path.join(base_dir, 'concorrentes_pequenos')
        os.makedirs(concorrentes_pequenos_dir, exist_ok=True)
        for i in range(1000):
            gerar_arquivo(os.path.join(concorrentes_pequenos_dir, f'arquivo_concorrente_{i+1}.bin'), 256 * 1024)

    if '3' in opcoes:
        grandes_dir = os.path.join(base_dir, 'grandes')
        os.makedirs(grandes_dir, exist_ok=True)
        gerar_arquivo(os.path.join(grandes_dir, 'arquivo_grande_5GB.bin'), 5 * 1024 * 1024 * 1024)

    if '4' in opcoes:
        sequenciais_dir = os.path.join(base_dir, 'sequenciais_grandes')
        os.makedirs(sequenciais_dir, exist_ok=True)
        for i in range(10):
            gerar_arquivo(os.path.join(sequenciais_dir, f'arquivo_grande_{i+1}.bin'), 2 * 1024 * 1024 * 1024)

    if '5' in opcoes:
        concorrentes_grandes_dir = os.path.join(base_dir, 'concorrentes_grandes')
        os.makedirs(concorrentes_grandes_dir, exist_ok=True)
        for i in range(5):
            gerar_arquivo(os.path.join(concorrentes_grandes_dir, f'arquivo_concorrente_grande_{i+1}.bin'), 4 * 1024 * 1024 * 1024)

    if '6' in opcoes:
        mistura_dir = os.path.join(base_dir, 'mistura_realista')
        os.makedirs(mistura_dir, exist_ok=True)

        for i in range(100):
            gerar_arquivo(os.path.join(mistura_dir, f'misto_pequeno_{i+1}.bin'), random.randint(100 * 1024, 1024 * 1024))

        for i in range(200):
            gerar_arquivo(os.path.join(mistura_dir, f'misto_grande_{i+1}.bin'), random.randint(1 * 1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024))

    print(f"Arquivos gerados com sucesso na pasta: {base_dir}")

if __name__ == "__main__":
    print("Escolha os cenários de arquivos que deseja gerar (separados por vírgula):")
    print("1 - 100 arquivos pequenos de 512 KB")
    print("2 - 1000 arquivos pequenos de 256 KB")
    print("3 - 1 arquivo grande de 5 GB")
    print("4 - 10 arquivos grandes de 2 GB")
    print("5 - 5 arquivos grandes de 4 GB")
    print("6 - Mistura realista (100 pequenos e 200 grandes)")
    opcoes = input("Digite as opções desejadas: ").split(',')
    opcoes = [opcao.strip() for opcao in opcoes]

    criar_arquivos_benchmark('arquivos_benchmark', opcoes)
