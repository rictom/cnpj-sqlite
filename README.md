# CNPJ-SQLITE
Script em python para baixar e converter os arquivos de dados públicos de CNPJs para o formato [SQLITE](https://pt.wikipedia.org/wiki/SQLite). O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.<br><br>
**AVISO IMPORTANTE: Ao final de janeiro/2026, a RFB alterou o layout da página, nome e caminho dos arquivos, por isso a parte do script que baixa os arquivos parou de funcionar!!! Aguarde uma atualização.**<br>

## Dados públicos de CNPJs no site da Receita:
Os arquivos csv zipados com os dados de CNPJs são disponibilizados mensalmente em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj e <s>[https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/)</s>. Esse diretório contém 37 arquivos. As tabelas maiores, como empresas, estabelecimentos e sócios, estão divididas em dez partes. A tabela empresas tem dados das matrizes dos cnpjs, como razão social, natureza jurídica e capital social. A tabela estabelecimentos contém dados cadastrais de cada matriz/filial, como nome fantasia, cnae, endereço, etc. A tabela sócios tem informações como nome de sócio, data de entrada e tipo de vínculo. O dicionário de dados está disponível [aqui](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf). <br>
Depois de deszipadas, cada arquivo é um csv, isto é, arquivo de texto com as colunas separadas pelo caractere ponto e vírgula (;). Devido ao grande tamanho das tabelas (são mais de 60 milhões de registros) fica inviável trabalhar diretamente os csvs no Excel ou outro aplicativo de planilha. Este projeto gera um arquivo em formato sqlite .<br>

## Pré-requisitos:
Python 3.9 ou posterior;<br>
Bibliotecas pandas, dask, sqlalchemy, wget.<br><br>
Recomenda-se instalar o [Anaconda](https://www.anaconda.com/download#downloads), que já configura o Python e ferramentas de programação;<br>
Verifique se você tem mais de <b>60GB de disco livre</b>, cerca de 30GB para a base em sqlite e 25GB para os arquivos do site da Receita zip ou descompactados. Os 25GB poderão ser liberados depois de rodar o script.<br>
Baixe o código do projeto pelo botão "Download ZIP" no menu "Code": <br>
![image](https://github.com/rictom/cnpj-sqlite/assets/71139693/e35ca678-7c52-45cc-ad61-32bfb4490fb9)
<br>
Isso fará baixar um arquivo zip com o código deste repositório. Descompacte em alguma pasta, por exemplo, na raiz do disco C:<br>

## Usando o Anaconda prompt:
<b>Atenção:</b> Você poderia abrir um console do DOS pela lupa do Windows e digitar "cmd", ou pelo menu contextual, mas assim o console não iria estar no "ambiente" correto e as instruções para rodar os scripts não iriam funcionar.<br><br>
Se você instalou o Anaconda no Windows, todas as operações de linha de comando devem ser no "ambiente" correto utilizando o Anaconda prompt.<br>
![image](https://github.com/rictom/cnpj-sqlite/assets/71139693/c13faf05-36ff-436c-bd09-8cdb46f835ad)

Quando se está em um ambiente padrão do Anaconda prompt, o console começa com (base) C:\Users\\<apelido do usuário>\:<br>
![image](https://github.com/rictom/cnpj-sqlite/assets/71139693/3a5b0bb6-42f1-4fa2-9916-02484ffefde5)

Se não for utilizar o Anaconda, sugere-se criar um environment "ambiente" próprio para rodar o projeto. Siga as orientações para criar um ambiente no python, sem Anaconda:<br>
https://docs.python.org/pt-br/3/library/venv.html
## Utilizando o script:
Use o comando <b>cd</b> para "navegar" o prompt até a pasta com o código deste projeto, por exemplo, <b>cd C:\cnpj-sqlite-main</b>. Isto dependerá do lugar que você descompactou a cópia deste projeto no seu HD.<br><br>
Instale as bibliotecas necessárias neste projeto usando o comando no Anaconda prompt:<br>
<b>pip install -r requirements.txt</b><br><br>
<s>Se desejar apenas uma relação dos arquivos disponíveis no site da Receita Federal ou baixar os arquivos, faça o seguinte comando no Anaconda prompt na pasta deste projeto:<br>
<b>python dados_cnpj_baixa.py</b><br>
Isto irá baixar os arquivos zipados do site da Receita na pasta "dados-publicos-zip".<br>
<b>ATENÇÃO: Em 14/8/2024 a página de dados abertos foi modificada, o script dados_cnpj_baixa.py foi atualizado para pegar a pasta do mês mais recente.</b><br>
<s>O download no site da Receita é lento, pode demorar várias horas (a última vez levou 8 horas)<br></s>Se o download estiver muito lento, outra forma de baixar os arquivos é usar um gerenciador de downloads, como o https://portableapps.com/apps/internet/free-download-manager-portable.<br><br>
</s>
Em janeiro/2026 a RFB alterou o local dos arquivos, por isso o script dados_cnpj_baixa.py parou de funcionar.<br>
Crie uma pasta com o nome <b>"dados-publicos"</b>. Se houver arquivos antigos nesta pasta, apague ou mova-os de lugar.<br>
Para baixar os arquivos csv, vá até a página [https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) , procure a seção "Recursos", clique no botão "Acessar Recurso" em "Inscrições no CNPJ". <br>
Isto levará para uma página de Download. Selecione o mês desejado e baixe os (37) arquivos: Cnaes.zip, Motivos.zip, Municipios.zip, Naturezas.zip, Paises.zip, Qualificacoes.zip, Simples.zip, Empresas0.zip, Empresas1.zip, Empresas2.zip, Empresas3.zip, Empresas4.zip, Empresas5.zip, Empresas6.zip, Empresas7.zip, Empresas8.zip, Empresas9.zip, Estabelecimentos0.zip, Estabelecimentos1.zip, Estabelecimentos2.zip, Estabelecimentos3.zip, Estabelecimentos4.zip, Estabelecimentos5.zip, Estabelecimentos6.zip, Estabelecimentos7.zip, Estabelecimentos8.zip, Estabelecimentos9.zip, Socios0.zip, Socios1.zip, Socios2.zip, Socios3.zip, Socios4.zip, Socios5.zip, Socios6.zip, Socios7.zip, Socios8.zip, Socios9.zip.<br>
Copie os arquivos zip para a pasta "dados-publicos-zip".<br>
O site da RFB agora permite baixar uma pasta zipada correspondente ao ANO-MÊS. Se você baixar por esta opção, descompacte o arquivo ANO-MÊS.zip, e copie os 37 arquivos zip para a pasta "dados-publicos-zip".<br>

Para iniciar a conversão dos arquivos para o formato sqlite, digite em um console do Anaconda prompt:<br>
<b>python dados_cnpj_para_sqlite.py</b><br>

Ao final, será gerado um arquivo cnpj.db, no formato sqlite, com cerca de 30GB, que poderá ser aberto no DB Browser for SQLITE (https://sqlitebrowser.org/).<br>

O arquivo cnpj.db poderá ser usado no meu projeto rede-cnpj (https://github.com/rictom/rede-cnpj), que permite visualização gráfica de relacionamentos entre empresas e sócios. Este projeto está rodando online em https://www.redecnpj.com.br.<br>
O projeto https://github.com/rictom/cnpj_consulta também utiliza o arquivo cnpj.db para visualizar os dados de cnpj em formato de tabela.<br>

<s>## Problema recorrente:
Se por acaso ocorrer um erro do tipo "Engine object has no attribute execute", altere a versão da biblioteca sqlalchemy pelo comando:
<b>pip install sqlalchemy==1.4.47</b><br></s>

## Tempo de execução:
Após baixar os arquivos, o processamento levou cerca de 2hs no Windows 10 em um notebook i7 com de oitava geração, 1h30 no Ubuntu no notebook i7 8th e 1h no MacOS com processador M1.

## Versão aplicativo Windows 10/MacOS do script:
Se você não tem familiaridade com python, pode utilizar o aplicativo em https://www.redecnpj.com.br/rede/pag/aplicativo.html#rede_programa_baixar que serve para baixar a base do site de dados abertos da Receita Federal e faz a conversão para sqlite. Baixe e rode APENAS as partes 1 e 2 do programa. Leia o manual antes de executar.

## Arquivo sqlite com a base CNPJ:<a id="arquivo_sqlite"></a>
O arquivo final poderá ser aberto no  [DBBrowser](https://sqlitebrowser.org/) for SQLITE.<br>

![image](https://user-images.githubusercontent.com/71139693/154585662-8c38c206-cb80-492e-8413-47699c79b4fd.png)<br>
Lista das tabelas do arquivo cnpj.db no DBBrowser.

Na pasta [exemplos](https://github.com/rictom/cnpj-sqlite/tree/main/exemplos) há amostras de consultas em SQL para exibir dados de empresas. Cole os exemplos na aba "Executar SQL" do DBBrowser para testar.<br>
![image](https://github.com/user-attachments/assets/f2833aa3-4227-40dd-8c96-6f5f5fcce9a0)


Isso irá apresentar o resultado:
![image](https://github.com/user-attachments/assets/c3714849-a9dc-453f-930d-3cce37cacda2)


## Como gerar listas de forma alternativa:
A forma mais direta de gerar listas é abrindo a base cnpj.db com o DBBrowser. Contudo, para quem não tem familiaridade com SQL, pode-se utilizar o script em <br>
https://github.com/rictom/cnpj_consulta <br>
que gera listas a partir de parâmetros como UF, Município ou CNAE. Existe a versão em python ou em aplicativo Windows. 

## Consultas por API:
O projeto seguinte é uma api em python (fastAPI) para consultar a base em sqlite cnpj.db:<br> 
https://github.com/rictom/cnpj_api

## Conversão para mysql:
O script em https://github.com/rictom/cnpj-mysql faz o carregamento dos dados para MYSQL ou POSTGRES.<br>

## Histórico de versões

versão 0.6 (julho/2024)
- biblioteca parfive para download de arquivos em paralelo no script dados_cnpj_baixa.py

versão 0.5 (janeiro/2024)
- utilizando sqlite3 ao invés de sqlalchemy para gerar conexão, de forma que não há mais dependência de sqlalchemy<2.x
  
versão 0.4 (setembro/2022)
- inclusão de índice na coluna "nome fantasia" da tabela estabelecimento.

versão 0.3 (maio/2022)
- inclusão de índice na coluna representante_legal da tabela sócios.

versão 0.2 (janeiro/2022)
- removido código não utilizado

versão 0.1 (julho/2021)
- primeira versão

