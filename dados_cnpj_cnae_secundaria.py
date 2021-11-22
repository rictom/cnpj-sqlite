# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 10:15:17 2021

@author: rictom
https://github.com/rictom/cnpj-sqlite

https://stackoverflow.com/questions/17116814/pandas-how-do-i-split-text-in-a-column-into-multiple-rows
"""
'''
script para criar tabela com cnae_secundaria, com um cnae por registro. 
Há duas versões:
a) uma em pandas que carrega toda a coluna cnae_secundária na memória, por isso pode dar erro dependendo do 
computador. Quando executei chegou a ocupar 10GB. Use bUsaPandas = True
b) usando DASK, salvando primeiro os cnaes_secundários em um arquivo temporário e depois carregando
na base sqlite. Teoricamente isso pode funcionar com menos memória. Use bUsaPandas = False
O desempenho das rotinas é semelhante, mas leva cerca de 1 hora em um notebook com i7 8a geração.
'''

#%%
import pandas as pd, sqlalchemy, glob, time
import os, sys

pasta_saida = r"dados-publicos"
cam = os.path.join(pasta_saida, 'cnpj.db')
bUsaPandas = True

#%% usando pandas, funciona, leva 1 hora. Ocupa bastante memória RAM, mais de 10GB
if bUsaPandas:
    print('inicio...', time.asctime())
    conn = sqlalchemy.create_engine(f'sqlite:///{cam}')
    conn.execute('drop table if exists cnae_secundaria')
    
    df = pd.read_sql('Select cnpj, cnae_fiscal_secundaria from estabelecimento', conn)
    
    df = df[ df['cnae_fiscal_secundaria']!='']
    df['cnae_fiscal_secundaria'] = df['cnae_fiscal_secundaria'].str.split(',')
    
    de = df.explode('cnae_fiscal_secundaria')
    de.to_sql('cnae_secundaria', conn, index=None, if_exists='append',method='multi',
              chunksize=1000, dtype=sqlalchemy.sql.sqltypes.TEXT)
    print('fim...', time.asctime())

# #%% fazendo em pedaços, dá database locked

# print('inicio...', time.asctime())

# conn = sqlalchemy.create_engine(f'sqlite:///{cam}')
# conn.execute('drop table if exists tmp_cnae')
# #conn.execute('drop table if exists tmp_cnae_secundaria')
# k = 1
# for df in pd.read_sql('Select cnpj, cnae_fiscal_secundaria from estabelecimento', conn, chunksize=1000000):
#     print('chunk:', k)  
#     k += 1
#     df1 = df[ df['cnae_fiscal_secundaria']!='']
#     df1['cnae_fiscal_secundaria'] = df1['cnae_fiscal_secundaria'].str.split(',')
    
#     de = df1.explode('cnae_fiscal_secundaria')
#     de.to_sql('cnae_secundaria', conn, index=None, if_exists='append',method='multi',
#               chunksize=10000, dtype=sqlalchemy.sql.sqltypes.TEXT)
# print('fim...', time.asctime())

#%% usando dask 
#para a tabela inteirá dá database locked, tentando salvar primeiro em parquet, parece OK. 
#leva quase 1 hora
import sqlalchemy, glob, time
import os, sys
if not bUsaPandas:
    import dask.dataframe as dd
    
    print('inicio...', time.asctime())
    conn = sqlalchemy.create_engine(f'sqlite:///{cam}')
    conn.execute('drop table if exists tmp_cnae')
    conn.execute('drop table if exists cnae_secundaria')
    conn.execute('''create table tmp_cnae as select cnpj, cast(cnpj as integer) as cnpj_int,  
                  cnae_fiscal_secundaria from estabelecimento''')
    
    #conn.execute('''ALTER TABLE tmp_cnae ADD COLUMN iq INTEGER ''')
    conn = None
    ddf = dd.read_sql_table('tmp_cnae', f'sqlite:///{cam}', index_col='cnpj_int')
    ddf = ddf[ ddf['cnae_fiscal_secundaria']!='']
    ddf['cnae_fiscal_secundaria'] = ddf['cnae_fiscal_secundaria'].str.split(',')
    ddf.explode('cnae_fiscal_secundaria')[['cnpj','cnae_fiscal_secundaria']].to_parquet('tmp_cnae.pq')
    ddf = None
    ddf = dd.read_parquet('tmp_cnae.pq')
    #ddf.explode('cnae_fiscal_secundaria').to_sql('tmp_cnae_secundaria', f'sqlite:///{cam}', index=None, if_exists='replace') #, #method='multi', chunksize=1000, 
                #dtype=sqlalchemy.sql.sqltypes.TEXT)
    ddf.to_sql('cnae_fiscal_secundaria', f'sqlite:///{cam}', index=None,  if_exists='replace') #, dtype=sqlalchemy.sql.sqltypes.TEXT)
    conn.execute('drop table if exists tmp_cnae')
    print('fim...', time.asctime())
    #dde.compute()
