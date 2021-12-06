#!/bin/bash
# 
# Faz o download dos dados públicos de CNPJ da RECEITA FEDERAL, separa os arquivos ZIPs e extrai somente os
# arquivos que vão ser utilizados pela LIZARD que sãos os arquivos de ESTABELECIMENTO, EMPRESA, MUNICIPIO e SÓCIO.
#
# Jean Landim - <jewanbb@gmail.com>

set -e

# Faz o download dos arquivos.
function download_data(){ 
  # IP do site da receita federal, até a escrita 03/12/2021 deste script era
  # 200.152.38.155/CNPJ/. 

  FILE_SERVER_IP=200.152.38.155/CNPJ/
  LINKS_FILE=/tmp/links.txt

  if [ ! -f $LINKS_FILE ]; then
     echo "Baixando página de links do servidor da Receita Federal ($FILE_SERVER_IP)"
     wget http://200.152.38.155:/CNPJ/ -O /tmp/CNPJ_index.html -o /dev/null

     echo "Gerando links ($LINKS_FILE)"
     cat /tmp/CNPJ_index.html | sed 's/href=/\nhref=/g' | grep href=\" | sed 's/.*href="//g;s/".*//g' | grep 'zip' | sort | sed "s@^@$FILE_SERVER_IP@g" > $LINKS_FILE
  fi

  echo "Baixando os arquivos"
  for link in $(cat $LINKS_FILE); do
      axel -n 15 -a $link
  done
}

# Descompacta os arquivos .ZIP
function manage_zip_files(){
  echo "Descompactando os arquivos"
  unzip '*.zip' 
  
  echo "Movendo os arquivos ZIP"
  mv -v *.zip dados-publicos-zip 

  echo "Movendo os arquivos CSV"
  mv -v *K* dados-publicos
}

download_data()
manage_zip_files
