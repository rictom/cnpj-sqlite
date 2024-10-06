import sqlite3
import csv
import glob
import os
import time
import zipfile

dataReferencia = 'xx/xx/2024'
pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos"

cam = os.path.join(pasta_saida, 'cnpj.db')
if os.path.exists(cam):
    input(f'O arquivo {cam} já existe. Apague-o primeiro e rode este script novamente.')

bApagaDescompactadosAposUso = True

arquivos_zip = list(glob.glob(os.path.join(pasta_compactados, r'*.zip')))

if len(arquivos_zip) != 37:
    r = input(f'A pasta {pasta_compactados} deveria conter 37 arquivos zip, mas tem {len(arquivos_zip)}. Deseja prosseguir assim mesmo? (y/n) ')
    if not r or r.lower() != 'y':
        print('Para baixar os arquivos, use um gerenciador de downloads.')

print('Início:', time.asctime())
for arq in arquivos_zip:
    print(time.asctime(), 'descompactando ' + arq)
    with zipfile.ZipFile(arq, 'r') as zip_ref:
        zip_ref.extractall(pasta_saida)

def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def carrega_tabela(arquivo_csv, nome_tabela, colunas):
    print(f'Carregando {arquivo_csv} na tabela {nome_tabela}...')

    with create_connection(cam) as conn:
        cursor = conn.cursor()

        cursor.execute(f'CREATE TABLE IF NOT EXISTS {nome_tabela} ({", ".join(colunas)});')

        with open(arquivo_csv, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)

            next(reader, None)  

            cursor.executemany(f'INSERT INTO {nome_tabela} VALUES ({", ".join("?" for _ in colunas)});', reader)

        colunas_com_indice = ['codigo', 'cnpj_basico', 'razao_social', 'nome_fantasia', 'cep', 'nome_socio', 'cnpj_cpf_socio']

        for coluna in colunas:
            if coluna in colunas_com_indice:
                cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_tabela}_{coluna} ON {nome_tabela}({coluna});')

    
    if bApagaDescompactadosAposUso:
        print('apagando arquivo ' + arquivo_csv)
        os.remove(arquivo_csv)

def carrega_dados():
    colunas_cnae = ['codigo', 'descricao']
    colunas_empresas = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 
                        'capital_social_str', 'porte_empresa', 'ente_federativo_responsavel']
    colunas_estabelecimento = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia',
                                'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                                'nome_cidade_exterior', 'pais', 'data_inicio_atividades', 'cnae_fiscal',
                                'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero',
                                'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1',
                                'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico',
                                'situacao_especial', 'data_situacao_especial']
    colunas_socios = ['cnpj_basico', 'identificador_de_socio', 'nome_socio', 'cnpj_cpf_socio',
                      'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
                      'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria']
    colunas_simples = ['cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
                       'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
    colunas_motivo = ['codigo', 'descricao']
    colunas_municipio = ['codigo', 'descricao']
    colunas_pais = ['codigo', 'descricao']
    colunas_qualificacao_socio = ['codigo', 'descricao']
    colunas_natureza = ['codigo', 'descricao']
    for arquivo in glob.glob(os.path.join(pasta_saida, '*CNAECSV')):
        carrega_tabela(arquivo, 'cnae', colunas_cnae)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*EMPRECSV')):
        carrega_tabela(arquivo, 'empresas', colunas_empresas)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*ESTABELE')): 
        carrega_tabela(arquivo, 'estabelecimento', colunas_estabelecimento)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*SOCIOCSV')):
        carrega_tabela(arquivo, 'socios', colunas_socios)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*SIMPLES.CSV.*')):
        carrega_tabela(arquivo, 'simples', colunas_simples)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*MOTI.CSV.*')):
        carrega_tabela(arquivo, 'motivo', colunas_motivo)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*MUNIC.CSV.*')):
        carrega_tabela(arquivo, 'municipio', colunas_municipio)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*NATJU.CSV.*')):
        carrega_tabela(arquivo, 'natureza_juridica', colunas_natureza)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*PAIS.CSV.*')):
        carrega_tabela(arquivo, 'pais', colunas_pais)

    for arquivo in glob.glob(os.path.join(pasta_saida, '*QUALS.CSV.*')):
        carrega_tabela(arquivo, 'qualificacao_socio', colunas_qualificacao_socio)

print('Iniciando carga de dados...')
carrega_dados()
print('Carga de dados concluída:', time.asctime())
