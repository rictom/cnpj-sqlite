import pandas as pd
import glob
import time
import pymongo
import gc
import os
import sys
import zipfile
from dotenv import load_dotenv

load_dotenv() #carrega o .env

APAGA_DESCOMPACTADOS = False # True - Os arquivos descompatados serão apagados após uso
PASTA_COMPACTADOS = r"dados-publicos-zip" # Caminho arquivos zipados da Receita
PASTA_SAIDA = r"dados-publicos" # Caminho pasta temporária para descompatação

COLUNAS_EMPRESAS = [
    'cnpj_basico', 
    'razao_social',
    'natureza_juridica',
    'qualificacao_responsavel',
    'capital_social_str',
    'porte_empresa',
    'ente_federativo_responsavel'
]
           
COLUNAS_ESTABELECIMENTOS = [
    'cnpj_basico',
    'cnpj_ordem', 
    'cnpj_dv',
    'matriz_filial', 
    'nome_fantasia',
    'situacao_cadastral','data_situacao_cadastral', 
    'motivo_situacao_cadastral',
    'nome_cidade_exterior',
    'pais',
    'data_inicio_atividades',
    'cnae_fiscal',
    'cnae_fiscal_secundaria',
    'tipo_logradouro',
    'logradouro', 
    'numero',
    'complemento',
    'bairro',
    'cep',
    'uf',
    'municipio',
    'ddd1', 
    'telefone1',
    'ddd2', 
    'telefone2',
    'ddd_fax', 
    'fax',
    'correio_eletronico',
    'situacao_especial',
    'data_situacao_especial'
]    

COLUNAS_SOCIOS = [
    'cnpj_basico',
    'identificador_de_socio',
    'nome_socio',
    'cnpj_cpf_socio',
    'qualificacao_socio',
    'data_entrada_sociedade',
    'pais',
    'representante_legal',
    'nome_representante',
    'qualificacao_representante_legal',
    'faixa_etaria'
]

COLUNAS_SIMPLES = [
    'cnpj_basico',
    'opcao_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei'
]

# Conexão com o banco de dados
def connecta_mongodb():

    # Verifica a nescessidade de autenticação no MongoDB
    if(os.getenv('MONGODB_AUTH') == 'True'):
        mongodb_uri = f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASS')}@{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}"

    else:
        mongodb_uri = f"mongodb://{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}"

    client = pymongo.MongoClient(mongodb_uri)
    db     = client[os.getenv('MONGODB_DATABASE')]
    
    return db

# Verifica se todos os arquivos estão presentes
def check_aquvios(caminho_zips):
    arquivos_zip = list(glob.glob(os.path.join(caminho_zips,r'*.zip')))

    if len(arquivos_zip) != 37:
        r = input(f'A pasta {caminho_zips} deveria conter 37 arquivos zip, mas tem {len(arquivos_zip)}. É recomendável prosseguir apenas com todos os arquivos, senão a base ficará incompleta. Deseja prosseguir assim mesmo? (y/n) ')    
        if not r or r.lower()!='y':
            print('Para baixar os arquivos, use um gerenciador de downloads ou use o comando python dados_cnpj_baixa.py')	
            sys.exit()

    return arquivos_zip

# Descompata todos os aquivos na PASTA_COMPCTADOS
def descompacta_zips(arquivos_zip, caminho_saida):
    print('Início:', time.asctime())
    for arq in arquivos_zip:
        print(time.asctime(), 'descompactando ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(caminho_saida)


# Carrega as coleções menores (códigos)
def carrega_codigos_collection(extensaoArquivo, nome_collection):
    arquivo = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + extensaoArquivo)))[0]

    print('Carregando arquivo ' + arquivo + ' na collection ' + nome_collection)

    # Realiza a leitua do CSV e conversão para dict
    dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['_id','descricao'])
    dtdict = dtab.to_dict(orient='records')

    collection = mongodb[nome_collection]
    collection.insert_many(dtdict)

    if APAGA_DESCOMPACTADOS:
        print('Apagando arquivo '+arquivo)
        os.remove(arquivo)

    #Coleta de lixo (liberar RAM)
    del dtab
    del dtdict
    gc.collect()


def carrega_geral_collections(tipo, colunas, nome_collection):

    arquivos = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + tipo)))

    for arq in arquivos:
        print(f'Carregando: {arq=} em {nome_collection}')
        print('Lendo csv ...', time.asctime())

        # Realiza a leitua do CSV e conversão para dict
        dtab   = pd.read_csv(arq, sep=';', header=None, names=colunas, encoding='latin1', dtype=str, na_filter=None)
        dtdict = dtab.to_dict(orient='records')

        collection = mongodb[nome_collection]
        collection.insert_many(dtdict)

        if APAGA_DESCOMPACTADOS:
            print(f'Apagando o arquivo {arq=}')
            os.remove(arq)
        
        #Coleta de lixo (liberar RAM)
        del dtab
        del dtdict
        gc.collect()

        print('Fim parcial...', time.asctime())
    

if __name__ == '__main__':
    arquivos_zip = check_aquvios(PASTA_COMPACTADOS)
    descompacta_zips(arquivos_zip, PASTA_SAIDA)
    mongodb = connecta_mongodb()

    # Carrega as coleções menores (códigos)
    carrega_codigos_collection('.CNAECSV',  os.getenv('MONGODB_COLLECTION_CNAE'))
    carrega_codigos_collection('.MOTICSV',  os.getenv('MONGODB_COLLECTION_MOTIVO'))
    carrega_codigos_collection('.MUNICCSV', os.getenv('MONGODB_COLLECTION_MUNICIPIO'))
    carrega_codigos_collection('.NATJUCSV', os.getenv('MONGODB_COLLECTION_NATUREZA'))
    carrega_codigos_collection('.PAISCSV',  os.getenv('MONGODB_COLLECTION_PAIS'))
    carrega_codigos_collection('.QUALSCSV', os.getenv('MONGODB_COLLECTION_QUAILIFICACAO'))

    # Carrega as colções maiores (info geral)
    carrega_geral_collections('.EMPRECSV',      COLUNAS_EMPRESAS,        os.getenv('MONGODB_COLLECTION_EMPRESAS'))
    carrega_geral_collections('.ESTABELE',      COLUNAS_ESTABELECIMENTOS, os.getenv('MONGODB_COLLECTION_ESTABALECIMENTOS'))
    carrega_geral_collections('.SOCIOCSV',      COLUNAS_SOCIOS,          os.getenv('MONGODB_COLLECTION_SOCIOS'))
    carrega_geral_collections('.SIMPLES.CSV.*', COLUNAS_SIMPLES,         os.getenv('MONGODB_COLLECTION_SIMPLES'))
    
    print('Fim!', time.asctime())
