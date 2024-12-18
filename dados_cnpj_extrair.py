from pathlib import Path
import zipfile
from tqdm import tqdm

# Caminhos das pastas
PASTA_ZIPS = Path("dados-publicos-zip")
PASTA_DESTINO = Path("dados-publicos")

def excluir_arquivos_existentes(pasta_destino):
    """Exclui todos os arquivos existentes na pasta de destino."""
    if pasta_destino.exists():
        for arquivo in pasta_destino.rglob("*"):
            if arquivo.is_file():
                arquivo.unlink()

def extrair_arquivos_zip():
    """Extrai arquivos ZIP da pasta origem para a pasta destino com barra de progresso."""
    if not PASTA_ZIPS.exists():
        print(f"A pasta {PASTA_ZIPS} não existe. Verifique o caminho.")
        return

    # Excluir arquivos existentes na pasta de destino
    excluir_arquivos_existentes(PASTA_DESTINO)

    # Criar a pasta de destino, se não existir
    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)

    # Obter lista de arquivos ZIP
    arquivos_zip = list(PASTA_ZIPS.glob("*.zip"))

    if not arquivos_zip:
        print("Nenhum arquivo ZIP encontrado na pasta origem.")
        return

    total_arquivos = len(arquivos_zip)
    sucesso = 0

    # Barra de progresso
    print("Extraindo arquivos...")
    with tqdm(total=total_arquivos, unit="arquivo", desc="Progresso") as barra:
        for arquivo_zip in arquivos_zip:
            try:
                with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                    zip_ref.extractall(PASTA_DESTINO)
                    sucesso += 1
            except Exception as e:
                print(f"Erro ao extrair {arquivo_zip.name}: {e}")
            finally:
                barra.update(1)

    print(f"Extração concluída: {sucesso}/{total_arquivos} arquivos extraídos com sucesso.")

if __name__ == "__main__":
    extrair_arquivos_zip()
