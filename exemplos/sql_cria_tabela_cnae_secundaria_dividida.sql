--a coluna cnae_secundaria tem para cada estabelecimento vários códigos, separados por ponto e virgula
--este sql cria tabela cnae secundaria em uma única coluna para possibilitar busca
CREATE TABLE cnaes_estabelecimentos AS
WITH RECURSIVE split(cnpj, cnae_secundario, rest) AS (
   SELECT e.cnpj, '', e.cnae_fiscal_secundaria||',' FROM estabelecimento e
   UNION ALL SELECT
   cnpj,
   substr(rest, 0, instr(rest, ',')),
   substr(rest, instr(rest, ',')+1)
   FROM split WHERE rest!=''
)
SELECT cnpj, CAST(cnae_secundario as TEXT) as cnae, CAST('S' as TEXT) as tipo_cnae --S=secundário
FROM split
WHERE cnae_secundario!=''
UNION ALL 
SELECT e.cnpj, CAST(e.cnae_fiscal as TEXT) as cnae, CAST('P' as TEXT) as tipo_cnae from estabelecimento e --P=primário
;

CREATE INDEX idx_cnaes_estabelecimentos_cnpj ON cnaes_estabelecimentos(cnpj);
CREATE INDEX idx_cnaes_estabelecimentos_cnae ON cnaes_estabelecimentos(cnae);
CREATE INDEX idx_cnaes_estabelecimentos_tipo_cnae ON cnaes_estabelecimentos(tipo_cnae);