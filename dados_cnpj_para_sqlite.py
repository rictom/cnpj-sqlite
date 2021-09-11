# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 02:54:50 2021

@author: ricar
"""

import pandas as pd, sqlalchemy, glob, time, dask.dataframe as dd
import os

dataReferencia = '17/07/2021' #input('Data de referência da base dd/mm/aaaa: ')
pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos" #esta pasta deve estar vazia. 
#pasta_tabelas = r"tabelas"

cam = os.path.join(pasta_saida, 'cnpj.db') 
if os.path.exists(cam):
    print('o arquivo ' + cam + ' já existe. Apague primeiro e rode este script novamente.')
    1/0

#engine = sqlalchemy.create_engine('sqlite:///cnpj.db')
engine = sqlalchemy.create_engine(f'sqlite:///{cam}')

arquivos_a_zipar = list(glob.glob(os.path.join(pasta_compactados,r'*.zip')))
import zipfile

for arq in arquivos_a_zipar:
    print('descompactando ' + arq)
    with zipfile.ZipFile(arq, 'r') as zip_ref:
        zip_ref.extractall(pasta_saida)

tipos = ['.EMPRECSV', '.ESTABELE', '.SOCIOCSV']

arquivos_emprescsv = list(glob.glob(os.path.join(pasta_saida, '*' + tipos[0])))

# # opção, juntar os csvs usando o cmd do windows:
# import subprocess
# #result = subprocess.check_output('dir *.*', shell = True)
# #print(result.decode('latin1'))
# #list_files = subprocess.run(["ls", "-l"])
# '''
# import subprocess
# result = subprocess.check_output('dir *.*', shell = True)
# '''
# '''
# juntar csv no dos
# >copy *.EMPRECSV EMPRECSV.csv
# >copy *.ESTABELE ESTABELE.csv
# copy *.SOCIOCSV SOCIOCSV.csv
# '''

# #juntar os arquivos csv por comando
# result = subprocess.check_output(r'copy dados-publicos\*.EMPRECSV dados-publicos\EMPRECSV.csv', shell = True)
# result = subprocess.check_output(r'copy dados-publicos\*.ESTABELE dados-publicos\ESTABELE.csv', shell = True)
# result = subprocess.check_output(r'copy dados-publicos\*.SOCIOCSV dados-publicos\SOCIOCSV.csv', shell = True)


def sqlCriaTabela(nomeTabela, colunas):
    sql = 'CREATE TABLE ' + nomeTabela + ' ('
    for k, coluna in enumerate(colunas):
        sql += '\n' + coluna + ' TEXT'
        if k+1<len(colunas):
            sql+= ',' #'\n'
    sql += ')\n'
    return sql

colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
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
              'complemento','bairro',
              'cep','uf','municipio',
              'ddd1', 'telefone1',
              'ddd2', 'telefone2',
              'ddd_fax', 'fax',
              'correio_eletronico',
              'situacao_especial',
              'data_situacao_especial']    

colunas_empresas = ['cnpj_basico', 'razao_social',
           'natureza_juridica',
           'qualificacao_responsavel',
           'capital_social',
           'porte_empresa',
           'ente_federativo_responsavel']

colunas_socios = [
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

colunas_simples = [
    'cnpj_basico',
    'opcao_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei']


sql = sqlCriaTabela('estabelecimento', colunas_estabelecimento)
engine.execute(sql)
sql = sqlCriaTabela('empresas', colunas_empresas)
engine.execute(sql)
sql = sqlCriaTabela('socios', colunas_socios)
engine.execute(sql)
sql = sqlCriaTabela('simples', colunas_simples)
engine.execute(sql)



def carregaTipo(nome_tabela, tipo, colunas):
    #usando dask, bem mais rápido que pandas
    arquivos = list(glob.glob(os.path.join(pasta_saida, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas, #nrows=1000,
                         encoding='latin1', dtype=str,
                         na_filter=None)
        #df.columns = colunas.copy()
        #engine.execute('Drop table if exists estabelecimento')
        print('to_sql...', time.asctime())
        ddf.to_sql(nome_tabela, str(engine.url), index=None, if_exists='append', #method='multi', chunksize=1000, 
                  dtype=sqlalchemy.sql.sqltypes.TEXT)
        print('fim parcial...', time.asctime())

# def carregaTipoDaskAlternativo(nome_tabela, tipo, colunas):
#     #usando dask, bem mais rápido que pandas
#     print(f'carregando: {tipo=}')
#     print('lendo csv ...', time.asctime())
#     #dask possibilita usar curinga no nome de arquivo
#     ddf = dd.read_csv(pasta_saida+r'\*' + tipo, 
#                       sep=';', header=None, names=colunas, 
#                       encoding='latin1', dtype=str,
#                       na_filter=None)
#     print('to_sql...', time.asctime())
#     ddf.to_sql(nome_tabela, str(engine.url), index=None, if_exists='append', #method='multi', chunksize=1000, 
#               dtype=sqlalchemy.sql.sqltypes.TEXT)
#     print('fim parcial...', time.asctime())

# def carregaTipoPandas(nome_tabela, tipo, colunas):
#     #usando pandas
#     arquivos = list(glob.glob(pasta_saida+r'\*' + tipo))
#     for arq in arquivos:
#         print(f'carregando: {arq=}')
#         print('lendo csv ...', time.asctime())
#         df = pd.read_csv(arq, sep=';', header=None, names=colunas, #nrows=1000,
#                          encoding='latin1', dtype=str,
#                          na_filter=None)
#         #df.columns = colunas.copy()
#         #engine.execute('Drop table if exists estabelecimento')
#         print('to_sql...', time.asctime())
#         df.to_sql(nome_tabela, engine, index=None, if_exists='append',method='multi',
#                   chunksize=1000, dtype=sqlalchemy.sql.sqltypes.TEXT)
#         print('fim parcial...', time.asctime())

carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)
carregaTipo('socios', '.SOCIOCSV', colunas_socios)
carregaTipo('empresas', '.EMPRECSV', colunas_empresas)
carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples)
#converter para utf8
#powershell -command "Get-Content .\test.txt" > test-utf8.txt
#GEt-Content .\ESTABELE.csv > estabeleutf8.csv
#rodar powershell a partir do cmd:
#powershell -command "Get-Content .\*.SIMPLES.*" > simplesutf8.csv

# '''
# #carregar usando .import do sqlite está dando erro no campo cnae_secundario, onde está "cod2,cod3,cod4"- o .import
# #quebra os codigos separados por vírgula dentro das aspas para colunas

# .mode csv
# .separator ";"
# .import EMPRECSVutf8.csv empresas
# .import estabeleutf8.csv estabelecimento
# .import sociosutf8.csv socios
# '''
# #sqlite3.exe cnpj.db < cria01.sql

# '''
# sqlite> .mode csv
# sqlite> .separator ";"
# sqlite> CREATE TABLE empresas (
#    ...> cnpj_basico TEXT,
#    ...> razao_social TEXT,
#    ...> natureza_juridica TEXT,
#    ...> qualificacao_responsavel TEXT,
#    ...> capital_social_str TEXT,
#    ...> porte_empresa TEXT,
#    ...> ente_federativo_responsavel TEXT)
#    ...> ;
# sqlite> .import EMPRECSVutf8.csv empresas
# '''

sqls = '''
ALTER TABLE empresas RENAME COLUMN capital_social TO capital_social_str;
ALTER TABLE empresas ADD COLUMN capital_social real;
update  empresas
set capital_social = cast( replace(capital_social_str,',', '.') as real);

ALTER TABLE estabelecimento ADD COLUMN cnpj text;
Update estabelecimento
set cnpj = cnpj_basico||cnpj_ordem||cnpj_dv;

CREATE  INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
CREATE  INDEX idx_empresas_razao_social ON empresas (razao_social);
CREATE  INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);
CREATE  INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);

ALTER TABLE socios RENAME TO socios_original;
CREATE INDEX idx_socios_original_cnpj_basico
ON socios_original(cnpj_basico);

CREATE TABLE socios AS 
SELECT te.cnpj as cnpj, ts.*
from socios_original ts
left join estabelecimento te on te.cnpj_basico = ts.cnpj_basico
where te.matriz_filial="1";

--DROP INDEX [IF EXISTS] index_name;
DROP TABLE IF EXISTS socios_original;

CREATE INDEX idx_socios_cnpj ON socios(cnpj);
CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);

CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);

CREATE TABLE "_referencia" (
	"referencia"	TEXT,
	"valor"	TEXT
);
'''


# '''
# -- ajuste de nomes de socios vazios para tabela de abril/2021
# create table socios2 AS
# select ts.cnpj, ts.cnpj_basico, 
# 		ts.identificador_de_socio, 
# 		case when ts.nome_socio<>"" then ts.nome_socio when t.nome_socio is not null then t.nome_socio else "" end as nome_socio,
# 		ts.cnpj_cpf_socio,
#             ts.qualificacao_socio,
#             ts.data_entrada_sociedade,
#             ts.pais,
#             ts.representante_legal,
#             ts.nome_representante,
#             ts.qualificacao_representante_legal,
#             ts.faixa_etaria
# from cnpj.socios ts
# left join socios t on t.cnpj=ts.cnpj and t.cnpj_cpf_socio=ts.cnpj_cpf_socio
# -- where ts.nome_socio=""
# --limit 1000
# '''

print('Inicio sqls:', time.asctime())
for k, sql in enumerate(sqls.split(';')):
    print('-'*20 + f'\nexecutando parte {k}:\n', sql)
    engine.execute(sql)
    print('fim parcial...', time.asctime())
print('fim sqls...', time.asctime())
                
#https://database.guide/5-ways-to-run-sql-script-from-file-sqlite/
#sqlite3.exe Test.db -init insert_data.sql
#sqlite3.exe Test.db ".read insert_data.sql"


def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    arquivo = list(glob.glob(os.path.join(pasta_saida, '*' + extensaoArquivo)))[0]
    print('carregando tabela '+arquivo)
    dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo','descricao'])
    #dqualificacao_socio['codigo'] = dqualificacao_socio['codigo'].apply(lambda x: str(int(x)))
    dtab.to_sql(nomeTabela, engine, if_exists='replace', index=None)
    engine.execute(f'CREATE INDEX idx_{nomeTabela} ON {nomeTabela}(codigo);')

carregaTabelaCodigo('.CNAECSV','cnae')
carregaTabelaCodigo('.MOTICSV', 'motivo')
carregaTabelaCodigo('.MUNICCSV', 'municipio')
carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
carregaTabelaCodigo('.PAISCSV', 'pais')
carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

#inserir na tabela referencia_

qtde_cnpjs = engine.execute('select count(*) as contagem from empresas;').fetchone()[0]

engine.execute(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')")
engine.execute(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')")

print('FIM!!!', time.asctime())