--exemplo de sql para pegar dados de um cnpj
select te.*, t.*, 
tnat.descricao as natureza_juridica_, 
tmot.descricao as motivo_situacao_cadastral_,
tmun.descricao as municipio_,
tc.descricao as cnae_fiscal_,
ifnull(tpa.descricao,'') as pais_
from estabelecimento t 
left join empresas te on te.cnpj_basico=t.cnpj_basico 
left join natureza_juridica tnat on tnat.codigo=te.natureza_juridica
left join motivo tmot on tmot.codigo=t.motivo_situacao_cadastral
left join municipio tmun on tmun.codigo=t.municipio
left join cnae tc on tc.codigo=t.cnae_fiscal
left join pais tpa on tpa.codigo=t.pais
left join qualificacao_socio tq on tq.codigo=te.qualificacao_responsavel
where t.cnpj="00000000000191"