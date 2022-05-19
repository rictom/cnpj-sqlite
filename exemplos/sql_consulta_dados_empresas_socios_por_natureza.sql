SELECT t.*, te.*, ts.*
from empresas te
left join estabelecimento t on t.cnpj_basico=te.cnpj_basico
left join socios ts on ts.cnpj=t.cnpj
where te.natureza_juridica = "2143"
or te.natureza_juridica = "2330"