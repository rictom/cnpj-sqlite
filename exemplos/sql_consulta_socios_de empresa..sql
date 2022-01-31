-- lista socios de cnpj
select *
from socios ts
left join empresas te on te.cnpj_basico=ts.cnpj_basico
where ts.cnpj="00000000000191"