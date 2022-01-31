-- lista empresas em que o Banco do Brasil tem participação societária, busca pelo cnpj
select te.razao_social, ts.*
from socios ts
left join empresas te on te.cnpj_basico=ts.cnpj_basico
where ts.cnpj_cpf_socio="00000000000191";

-- lista empresas em que o Banco do Brasil tem participação societária, busca por nome de sócio
select *
from socios ts
left join empresas te on te.cnpj_basico=ts.cnpj_basico
where ts.nome_socio="BANCO DO BRASIL SA";