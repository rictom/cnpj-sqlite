
# CNPJ-SQLITE

Fork do [CNPJ-SQLITE](https://github.com/rictom/cnpj-sqlite/).

Script em python para carregar os arquivos de cnpj dos dados públicos da Receita Federal em formato SQLite.

A partir de 2021 os dados da Receita Federal estão disponíveis nesse [link](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj) (ou direto [aqui](http://200.152.38.155/CNPJ/) no servidor de arquivos) em formato .CSV separados em arquivos .ZIP. 

## Pré-requisitos:

### Software
**Linguagem:** _Python 3.8 (ou versão maior)_
**Gerenciador de pacotes:** _Poetry_
**Bibliotecas:** _Loguru, SQLAlchemy, Dask e Pandas_
**Sistema Operacional**: GNU/Linux e/ou Mac OS para executar o arquivo _download_data.sh_ (apesar que acredito que possa funcionar no WSL e/ou Cygwin do Windows).
**Utilitários**: _Axel_ (irá fazer o download mais rápido dos arquivos .ZIP do site da receita) e _SQLite_.
### Hardware
**Processador**: _Mid-End (i3, i5, i7 ou i9)_
**Armazenamento**: Espaço livre de no mínimo de 60 GB.
**RAM**: 6 GB (ou superior)


## Utilizando o script:

### Download dos arquivos:
Baixe todos os arquivos utilizando o script _download_data.sh_.

No Terminal Linux com o Bash deverá ser o comando:
  ``` $ ./download_data.sh ```
 
 
### Observação
Vale ressaltar que apesar de estar sendo utilizado um gerenciador de downloads, o download total do arquivo ainda poderá demorar, e no momento da descompreensão dos arquivos a máquina pode ficar irresponsiva, devido a quantidade de dados.

### Geração de arquivo para o formato SQLite.

Primeiro ative o ambiente virtual do _Poetry_:

```$ poetry shell ```

Caso não tenha eexecutado ainda o install das dependências, chame:

```$ poetry install ```

E depois no terminal chame o script:
```$ ./data_to_sqlite.py```

## Arquivo SQLite já tratado:

Arquivo SQLite já extraído, referência 13/11/2021 (.D11113.), está disponível no google drive, zipado em 4 pedaços: no [link](https://drive.google.com/drive/folders/1Gkeq27aHv6UgT8m30fc4hZWMPqdhEHWr?usp=sharing).

Utilize o 7-zip para descompacta-los.

Para pegar o arquivo SQLite, mais atualizado, sugiro ver o README do repositório original [CNPJ-SQLITE](https://github.com/rictom/cnpj-sqlite).

## Histórico de versões

versão 0.2 (dezembro/2021)
- Refatoramento e melhorias do código,  por Jean Landim.

versão 0.1 (julho/2021)
- primeira versão

## Créditos

A versão original desse repositório encontra-se [CNPJ-SQLITE](https://github.com/rictom/cnpj-sqlite/) e sinceros agradecimentos ao usuário [rictom](https://github.com/rictom/cnpj-sqlite), autor original do repositório.
