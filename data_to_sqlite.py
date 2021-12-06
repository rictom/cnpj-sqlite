#!/usr/bin/env python
"""
    
    Criado por ricar em "Sex Mar 26 02:54:50 2021", refatorado para melhor compreensão do código.
    Refatoração realizada por Jean Landim - <jewanbb@gmail.com>

    Considerações:

    Os arquivos já devem se encontrar deszipados no caminho setado na variável "OUTPUT_DIR".
    Para baixar os arquivos poderá utilizar o Bash Script "download_data.sh" que está na raiz
    do repositório (Para ambientes Linux Like).
   
     Abaixo as legendas para os campos como serão gerados de acordo com a natureza dos CSVs.

     EMPRESAS:
     
     cnpj_b -> CNPJ Básico
     razao_s -> Razão Social
     nat_jur -> Natureza Jurídica 
     qual_dr -> Qualificação do Responsável
     cap_soc_emp -> Capital Social da Empresa 
     porte -> Porte da Empresa
     ente_f -> Ente Federativo Responsável 
     cap_soc -> Capital Social 
     
     ESTABELECIMENTO:
       
     cnpj_b -> CNPJ Básico
     cnpj_o -> CNPJ Ordem
     cnpj_dv -> CNPJ DV
     id_mat_fil -> Identificador Matriz Filial
     nom_fan -> Nome Fantasia
     sit_cad -> Situação Cadastral
     dt_sit_cad -> Data Situação Cadastral
     mot_sit_cad -> Motivo Situação Cadastral
     nom_cid_ext -> Nome da Cidade no Exterior
     pais -> País
     dt_ini_ativ -> Data de Início de Atividade
     cnae_fp -> Cnae Fiscal Principal
     cnae_fs -> Cnae Fiscal Secundaria
     tipo_log -> Tipo de Logradouro
     log -> Logradouro
     num -> Número
     comp -> Complemento
     bairro -> Bairro
     cep -> CEP
     uf -> UF
     municipio -> Municipio
     ddd1 -> DDD 1
     tel1 -> Telefone 1
     ddd2 -> DDD 2
     tel2 -> Telefone 2
     ddd_fax -> DDD do Fax
     fax -> Fax
     email -> Correio Eletrônico
     sit_especial -> Situação Especial
     dt_sit_especial -> Data da Situação Cadastral
     
     SOCIOS:
     
     cnpj_b -> CNPJ Básico
     id_socio -> Identificador de Sócio
     nom_socio -> Nome do Sócio
     cpf_socio -> CNPJ/CPF do Sócio
     qual_socio -> Qualificação do Sócio
     dt_ent_soc -> Data de Entrada da Sociedade
     pais -> País
     rep_legal -> Representante Legal
     nome_rep -> Nome do Representante
     qual_rep_legal -> Qualificação do Representante Legal
     faixa_etaria -> Faixa Etária

     SIMPLES:

     cnpj_b -> CNPJ Básico
     opcao_simples -> Opção Simples
     dt_opcao_simples -> Data Opção Simples 
     dt_exclusao_simples -> Data Exclusão Simples
     opcao_mei -> Opção MEI
     dt_opcao_mei -> Data Opção MEI
     dt_exclusao_mei -> Data Exclusão MEI

"""

import dask.dataframe as dd
import sqlalchemy, glob, time, os
from loguru import logger

dataReferencia = '17/07/2021' #input('Data de referência da base dd/mm/aaaa: ')

# Pasta de saída para o arquivo SQLITE de banco de dados
OUTPUT_DIR = "dados-publicos"
# Nome do arquivo de banco de dados
SQLITE_FILENAME = "cnpj.db"
# Nome do arquivo com o caminho em que ele se encontra
SQLITE_FULL_PATH = os.path.join(OUTPUT_DIR, SQLITE_FILENAME)  
# Tabelas, colunas e suas extensões
TYPE_TABLES_COLUMNS_EXT={
    "empresas":(('cnpj_b', 'razao_s', 'nat_jur', 'qual_dr', 'cap_soc_emp', 'porte', 'ente_f', 'cap_soc'),'.EMPRECSV'),
    "estabelecimento":(('cnpj_b', 'cnpj_o', 'cnpj_dv', 'id_mat_fil', 'nom_fan', 'sit_cad', 'dt_sit_cad', 'mot_sit_cad',
            'nom_cid_ext','pais','dt_ini_ativ','cnae_fp','cnae_fs', 'tipo_log', 'log', 'num', 'comp', 'bairro',
            'cep','uf','municipio','ddd1','tel1','ddd2','tel2','ddd_fax','fax','email','sit_especial','dt_sit_especial'),'.ESTABELE'),
    "socios":(('cnpj_b', 'id_socio', 'nom_socio', 'cpf_socio', 'qual_socio', 'dt_ent_soc', 'pais', 'rep_legal', 'nome_rep', 'qual_rep_legal', 'faixa_etaria'),'.SOCIOCSV'),
    "simples":(('cnpj_b', 'opcao_simples', 'dt_opcao_simples', 'dt_exclusao_simples', 'opcao_mei', 'dt_opcao_mei', 'dt_exclusao_mei'), '.SIMPLES.CSV*')
}

# Tabelas que contém somente código e descrição
COD_TABLES_COLUMNS_EXT={
        "cnae": ".CNAECSV",
        "motivo": ".MOTICSV",
        "municipio": ".MUNICCSV",
        "natureza_juridica": ".NATJUCSV",
        "pais": ".PAISCSV",
        "qualificacao_socio": ".QUALSCSV"
}

class FileExists(Exception):
    def __str__(self):
        return f"O arquivo de banco de dados já existe. Apague-o primeiro e tente novamente."

class FilesNotFound(Exception):
    def __str__(self):
        return f"Arquivos CSVs não encontrados!!!"

def _load_table(table: str, file_extension: str, columns: list):
    engine_url = SQLHelper().engine.url
    files = list(glob.glob(os.path.join(OUTPUT_DIR)+'/*'+file_extension))

    if files == []:
        raise FilesNotFound

    for file in files:
        logger.info(f"Carregando arquivo: {file}")
        ddf = dd.read_csv(file, sep=';', header=None, names=columns,
                encoding='latin1', dtype=str, na_filter=None)
        ddf.to_sql(table, str(engine_url), index=None, if_exists='append',
                dtype=sqlalchemy.sql.sqltypes.TEXT)

class SQLHelper:
    """ Classe que irá concatenar as funcionalidades e manipular SQL. """

    def __init__(self):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{SQLITE_FULL_PATH}")

    def execute(self, sql: str):
        """ Executa a instrução SQL. """ 
        return self.engine.execute(sql)

    def create_table_sql(self, table_name: str, columns: list) -> str:
        """ Cria a instrução SQL. """
        sql_query = f"CREATE TABLE {table_name} ("
        sql_query+=' '.join(f"{c} TEXT," for c in columns)
        return sql_query[:-1] + ");"

def create_tables():
    """ Crias as tabelas e suas respectivas colunas. """
    sql_helper = SQLHelper()
     
    logger.info(f"Criando as tabelas.")
    for table, columns in TYPE_TABLES_COLUMNS_EXT.items():
        columns = columns[0]
        logger.info(f"Criando a tabela: {table} -> colunas: {columns}")
        sql = sql_helper.create_table_sql(table, columns)
        sql_helper.execute(sql)
    
    columns = ['codigo','descricao']
    for table, columns in zip(COD_TABLES_COLUMNS_EXT.keys(), [columns]): 
        logger.info(f"Criando a tabela: {table} -> colunas: {columns}")
        sql = sql_helper.create_table_sql(table, columns)
        sql_helper.execute(sql)

def load_table_code():
    """ Joga para a base de dados as tabelas que tem somente código e descrição. """
    columns = ["codigo", "descricao"]
    for table, file_extension in COD_TABLES_COLUMNS_EXT.items(): 
        _load_table(table, file_extension, columns)
    return True

def load_table_types():
    """ Joga para a base de dados as tabelas que tem tipos defindos além de código e descrição.
        Essas tabelas são EMPRESAS, ESTABELECIMENTO, SOCIOS e SIMPLES.
    """
    for table, columns_file_extension in TYPE_TABLES_COLUMNS_EXT.items():
        columns, file_extension = columns_file_extension
        _load_table(table, file_extension, columns)
    return True

def post_sql():
     """Executa instruções SQL de finais."""
     
     sqls = '''
     ALTER TABLE empresas RENAME COLUMN cap_soc TO cap_soc_str;
     ALTER TABLE empresas ADD COLUMN capital_social real;
     UPDATE empresas SET cap_soc_str = cast(replace(cap_soc_str,',', '.') as real);
     ALTER TABLE estabelecimento ADD COLUMN cnpj text;
     UPDATE estabelecimento SET cnpj = cnpj_b||cnpj_o||cnpj_dv;
     CREATE  INDEX idx_empresas_cnpj_b ON empresas (cnpj_b);
     CREATE  INDEX idx_empresas_razao_s ON empresas (razao_s);
     CREATE  INDEX idx_estabelecimento_cnpj_b ON estabelecimento (cnpj_b);
     CREATE  INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);

     ALTER TABLE socios RENAME TO socios_original;
     CREATE INDEX idx_socios_original_cnpj_b
     ON socios_original(cnpj_b);

     CREATE TABLE socios AS 
     SELECT te.cnpj as cnpj, ts.*
     FROM socios_original ts left join estabelecimento te on te.cnpj_b = ts.cnpj_b
     WHERE te.id_mat_fil="1";

     --DROP INDEX [IF EXISTS] index_name;
     DROP TABLE IF EXISTS socios_original;

     CREATE INDEX idx_socios_cnpj ON socios(cnpj);
     CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cpf_socio);
     CREATE INDEX idx_socios_nome_socio ON socios(nom_socio);

     CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_b);

     CREATE TABLE "_referencia" (
            "referencia"	TEXT,
            "valor"	TEXT
     );
     '''
     sql_helper = SQLHelper() 
     logger.info("Aplicando instruções SQL finais.")
     for instr_n, sql in enumerate(sqls.split(';')):
         logger.info(f"Executando query de número {instr_n}")
         sql_helper.execute(sql)

     qtde_cnpjs = sql_helper.execute("SELECT count(*) as contagem FROM empresas;").fetchone()[0]
     sql_helper.execute(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')")
     sql_helper.execute(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')")

def cnpj_to_sqlite():
    """ Carrega dos dados CSV para o formato SQLITE. """

    if os.path.exists(SQLITE_FULL_PATH):
        raise FileExists

    logger.info("CNPJ -> SQLITE")
    logger.info("Leia o código fonte para mais informações.")
    for function in (create_tables, load_table_code, load_table_types, post_sql):
        logger.info(f"Chamando: {function.__name__}")
        function()

if __name__ == "__main__":
    cnpj_to_sqlite()
