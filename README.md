# CNPJ-SQLITE
Script em python para converter os arquivos de dados públicos de CNPJs para o formato SQLITE. O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.

## Dados públicos de CNPJs no site da Receita:
Os arquivos csv zipados com os dados de CNPJs estão disponíveis em https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj ou http://200.152.38.155/CNPJ/ (aqui os arquivos aparecem primeiro). 

## Pré-requisitos:
Python 3.8;<br>
Bibliotecas pandas, dask e sqlalchemy.<br>

## Utilizando o script:
Baixe todos os arquivos zipados do site da Receita e salve na pasta "dados-publicos-zip".<br>
O download no site da Receita é lento, pode demorar várias horas. Sugiro utilizar um gerenciador de downloads.<br><br>
Crie uma pasta com o nome "dados-publicos".<br>

Para iniciar esse script, em um console DOS digite<br>
python dados_cnpj_para_sqlite.py<br>

O processamento durou cerca de 2hs em um notebook i7 de oitava geração.

Ao final, será gerado um arquivo cnpj.db, no formato sqlite, que pode ser aberto no DB Browser for SQLITE (https://sqlitebrowser.org/).<br>

O arquivo cnpj.db poderá ser usado no meu projeto rede-cnpj (https://github.com/rictom/rede-cnpj), que permite visualização gráfica de relacionamentos entre empresas e sócios. O projeto está rodando online em https://www.redecnpj.com.br.<br>

## Arquivo sqlite já tratado:
O banco de dados no formato sqlite, referência 8/1/2022 (.D20108.), está disponível em  https://www.mediafire.com/folder/1vdqoa2mk0fu9/cnpj-sqlite.
Baixe o arquivo cnpj.7z e utilize o 7-zip (https://www.7-zip.org/download.html) para descompactar.<br>

## Conversão para mysql:
O script em https://github.com/rictom/cnpj-mysql faz o carregamento dos dados para o banco de dados em MYSQL.<br>

## Histórico de versões

versão 0.1 (julho/2021)
- primeira versão

