import os
import requests
from bs4 import BeautifulSoup

# URL base da Receita Federal
URL_BASE = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/2024-11/"
DIRETORIO_DOWNLOAD = "./downloads_cnpj"

# Configuração de sessão com User-Agent
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
})

def listar_arquivos(url):
    """Obtém a lista de arquivos disponíveis para download."""
    try:
        print(f"Acessando {url}...")
        response = session.get(url, timeout=10)
        response.raise_for_status()

        # Parse do HTML
        soup = BeautifulSoup(response.text, "html.parser")
        arquivos = []
        
        # Procura links de arquivos
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.endswith(".zip"):
                arquivos.append(href)
        
        return arquivos

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL base: {e}")
        return []

def baixar_arquivo(url_arquivo, caminho_destino):
    """Faz o download de um arquivo."""
    try:
        print(f"Baixando {url_arquivo}...")
        response = session.get(url_arquivo, stream=True, timeout=30)
        response.raise_for_status()

        with open(caminho_destino, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Arquivo salvo em {caminho_destino}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar {url_arquivo}: {e}")

def main():
    """Executa o fluxo principal."""
    print("Iniciando o processo de download dos arquivos do CNPJ...")

    # Garantir que o diretório de downloads exista
    os.makedirs(DIRETORIO_DOWNLOAD, exist_ok=True)

    # Listar arquivos disponíveis
    arquivos = listar_arquivos(URL_BASE)

    if not arquivos:
        print("Não foi possível listar os arquivos.")
        return

    print(f"Arquivos encontrados: {arquivos}")

    # Baixar cada arquivo
    for arquivo in arquivos:
        url_arquivo = os.path.join(URL_BASE, arquivo)
        caminho_destino = os.path.join(DIRETORIO_DOWNLOAD, arquivo)

        if not os.path.exists(caminho_destino):  # Evita baixar novamente
            baixar_arquivo(url_arquivo, caminho_destino)
        else:
            print(f"Arquivo já existe: {caminho_destino}")

if __name__ == "__main__":
    main()
