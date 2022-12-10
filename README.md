# CNPJ-SQLITE
Script em python para converter os arquivos de dados públicos de CNPJs para o formato [SQLITE](https://pt.wikipedia.org/wiki/SQLite). O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.

## Dados públicos de CNPJs no site da Receita:
Os arquivos csv zipados com os dados de CNPJs estão disponíveis em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj ou https://dadosabertos.rfb.gov.br/CNPJ/ (http://200.152.38.155/CNPJ/).<br><br>

## Pré-requisitos:
Python 3.8 ou posterior;<br>
Bibliotecas pandas, dask e sqlalchemy.<br>
55GB de disco livre, 30GB para a base em sqlite e 25GB para os arquivos do site da Receita zip ou descompactados. Os 25GB poderão ser liberados depois de rodar o script.<br>

## Utilizando o script:
Este projeto não baixa os arquivos do site da Receita.  Obtenha uma relação dos arquivos disponíveis pelo comando<br>
python dados_cnpj_lista_url.py<br>

Baixe todos os arquivos zipados do site da Receita e salve na pasta "dados-publicos-zip".<br>
O download no site da Receita é lento, pode demorar várias horas (a última vez levou 8 horas)<br>Sugiro utilizar um gerenciador de downloads, como o https://portableapps.com/apps/internet/free-download-manager-portable.<br><br>
Crie uma pasta com o nome "dados-publicos".<br>

Para iniciar a conversão dos arquivos para o formato sqlite, digite em um console:<br>
python dados_cnpj_para_sqlite.py<br>

O processamento leva cerca de 2hs em um notebook i7 de oitava geração.

Ao final, será gerado um arquivo cnpj.db, no formato sqlite, com cerca de 30GB, que poderá ser aberto no DB Browser for SQLITE (https://sqlitebrowser.org/).<br>

O arquivo cnpj.db poderá ser usado no meu projeto rede-cnpj (https://github.com/rictom/rede-cnpj), que permite visualização gráfica de relacionamentos entre empresas e sócios. Este projeto está rodando online em https://www.redecnpj.com.br.<br>
O projeto https://github.com/rictom/cnpj_consulta também utiliza o arquivo cnpj.db para visualizar os dados de cnpj em formato de tabela.<br>

## Arquivo sqlite já tratado com a base CNPJ:<a id="arquivo_sqlite"></a>
O banco de dados no formato sqlite, referência 19/11/2022 (.D21008.), está disponível em  https://www.mediafire.com/folder/1vdqoa2mk0fu9/cnpj-sqlite.
Baixe o arquivo cnpj.db.AAAA-MM-DD.7z e utilize o 7-zip (https://www.7-zip.org/download.html) para descompactar.<br>

![image](https://user-images.githubusercontent.com/71139693/154585662-8c38c206-cb80-492e-8413-47699c79b4fd.png)<br>
Lista das tabelas do arquivo cnpj.db no [DBBrowser](https://sqlitebrowser.org/) for SQLITE.

## Conversão para mysql:
O script em https://github.com/rictom/cnpj-mysql faz o carregamento dos dados para o banco de dados em MYSQL.<br>

## Histórico de versões
versão 0.4 (setembro/2022)
- inclusão de índice na coluna "nome fantasia" da tabela estabelecimento.

versão 0.3 (maio/2022)
- inclusão de índice na coluna representante_legal da tabela sócios.

versão 0.2 (janeiro/2022)
- removido código não utilizado

versão 0.1 (julho/2021)
- primeira versão

