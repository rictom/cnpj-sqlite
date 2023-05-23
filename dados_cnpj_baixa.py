# -*- coding: utf-8 -*-
"""
Spyder Editor

lista relação de arquivos na página de dados públicos da receita federal
e faz o download
"""
from bs4 import BeautifulSoup
import requests, wget, os, sys, time, glob

url = 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj'
url = 'http://200.152.38.155/CNPJ/'


pasta_compactados = r"dados-publicos-zip" #local dos arquivos zipados da Receita

if len(glob.glob(os.path.join(pasta_compactados,'*.zip'))):
    print(f'Há arquivos zip na pasta {pasta_compactados}. Apague ou mova esses arquivos zip e tente novamente')
    sys.exit()
       
page = requests.get(url)    
data = page.text
soup = BeautifulSoup(data)
lista = []
print('Relação de Arquivos em ' + url)
for link in soup.find_all('a'):
    if str(link.get('href')).endswith('.zip'): 
        cam = link.get('href')
        # if cam.startswith('http://http'):
        #     cam = 'http://' + cam[len('http://http//'):] 
        if not cam.startswith('http'):
            print(url+cam)
            lista.append(url+cam)
        else:
            print(cam)
            lista.append(cam)
            
resp = input(f'Deseja baixar os arquivos acima para a pasta {pasta_compactados} (y/n)?')
if resp.lower()!='y' and resp.lower()!='s':
    sys.exit()
    
def bar_progress(current, total, width=80):
    if total>=2**20:
        tbytes='Megabytes'
        unidade = 2**20
    else:
        tbytes='kbytes'
        unidade = 2**10
    progress_message = f"Baixando: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
    # Don't use print() as it will print in new line every time.
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()
  
for k, url in enumerate(lista):
    print('\n' + time.asctime() + f' - item {k}: ' + url)
    wget.download(url, out=os.path.join(pasta_compactados, os.path.split(url)[1]), bar=bar_progress)
    
print('\n\n'+ time.asctime() + f' Finalizou!!! Baixou {len(lista)} arquivos.')

#lista dos arquivos
'''
http://200.152.38.155/CNPJ/Cnaes.zip
http://200.152.38.155/CNPJ/Empresas0.zip
http://200.152.38.155/CNPJ/Empresas1.zip
http://200.152.38.155/CNPJ/Empresas2.zip
http://200.152.38.155/CNPJ/Empresas3.zip
http://200.152.38.155/CNPJ/Empresas4.zip
http://200.152.38.155/CNPJ/Empresas5.zip
http://200.152.38.155/CNPJ/Empresas6.zip
http://200.152.38.155/CNPJ/Empresas7.zip
http://200.152.38.155/CNPJ/Empresas8.zip
http://200.152.38.155/CNPJ/Empresas9.zip
http://200.152.38.155/CNPJ/Estabelecimentos0.zip
http://200.152.38.155/CNPJ/Estabelecimentos1.zip
http://200.152.38.155/CNPJ/Estabelecimentos2.zip
http://200.152.38.155/CNPJ/Estabelecimentos3.zip
http://200.152.38.155/CNPJ/Estabelecimentos4.zip
http://200.152.38.155/CNPJ/Estabelecimentos5.zip
http://200.152.38.155/CNPJ/Estabelecimentos6.zip
http://200.152.38.155/CNPJ/Estabelecimentos7.zip
http://200.152.38.155/CNPJ/Estabelecimentos8.zip
http://200.152.38.155/CNPJ/Estabelecimentos9.zip
http://200.152.38.155/CNPJ/Motivos.zip
http://200.152.38.155/CNPJ/Municipios.zip
http://200.152.38.155/CNPJ/Naturezas.zip
http://200.152.38.155/CNPJ/Paises.zip
http://200.152.38.155/CNPJ/Qualificacoes.zip
http://200.152.38.155/CNPJ/Simples.zip
http://200.152.38.155/CNPJ/Socios0.zip
http://200.152.38.155/CNPJ/Socios1.zip
http://200.152.38.155/CNPJ/Socios2.zip
http://200.152.38.155/CNPJ/Socios3.zip
http://200.152.38.155/CNPJ/Socios4.zip
http://200.152.38.155/CNPJ/Socios5.zip
http://200.152.38.155/CNPJ/Socios6.zip
http://200.152.38.155/CNPJ/Socios7.zip
http://200.152.38.155/CNPJ/Socios8.zip
http://200.152.38.155/CNPJ/Socios9.zip
'''