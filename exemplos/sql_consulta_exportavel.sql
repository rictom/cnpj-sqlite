SELECT 
estabelecimento.cnpj as cnpj,
CASE
        WHEN estabelecimento.situacao_cadastral = '01' THEN 'NULA'
        WHEN estabelecimento.situacao_cadastral = '02' THEN 'ATIVA'
        WHEN estabelecimento.situacao_cadastral = '03' THEN 'SUSPENSA'
        WHEN estabelecimento.situacao_cadastral = '04' THEN 'INAPTA'
        WHEN estabelecimento.situacao_cadastral = '05' THEN 'IRREGULAR'
        WHEN estabelecimento.situacao_cadastral = '08' THEN 'BAIXADA'
    END AS situacao_cadastral,
estabelecimento.data_inicio_atividades as data_abertura,
COALESCE(simples.data_opcao_simples, null) as data_opcao_simples,
empresas.razao_social as razao_social,
estabelecimento.nome_fantasia as nome_fantasia,
LOWER(COALESCE(estabelecimento.correio_eletronico, '')) as email_1,
CASE
        WHEN LOWER(estabelecimento.correio_eletronico) LIKE '%contab%' THEN 'contabilidade'
        WHEN LOWER(estabelecimento.correio_eletronico) LIKE '%cnt.br%' THEN 'contabilidade'
        ELSE 'padrao'
    END AS tipo_email,
CASE
        WHEN SUBSTR(estabelecimento.telefone1, 1, 1) >= '6' 
        THEN COALESCE((estabelecimento.ddd1 || 9 || estabelecimento.telefone1), '')
        ELSE COALESCE((estabelecimento.ddd1 || estabelecimento.telefone1), '')
    END AS telefone1,
CASE
        WHEN SUBSTR(estabelecimento.telefone2, 1, 1) >= '6' 
        THEN COALESCE((estabelecimento.ddd2 || 9 || estabelecimento.telefone2), '')
        ELSE COALESCE((estabelecimento.ddd2 || estabelecimento.telefone2), '')
    END AS telefone2,
CASE
		WHEN simples.opcao_mei = 'S' THEN 'MEI'
        WHEN empresas.porte_empresa = '00' THEN 'NAO_INFORMADO'
        WHEN empresas.porte_empresa = '01' THEN 'MICRO_EMPRESA'
        WHEN empresas.porte_empresa = '03' THEN 'PEQUENO_PORTE'
        WHEN empresas.porte_empresa = '05' THEN 'DEMAIS'
    END AS porte_empresa,
estabelecimento.cnae_fiscal || '-' || cnae_principal.descricao as cnae_principal,
estabelecimento.cnae_fiscal_secundaria as cnaes_secundarios,
(SELECT natureza_juridica.codigo || '-' || natureza_juridica.descricao FROM natureza_juridica WHERE natureza_juridica.codigo = empresas.natureza_juridica) as natureza_juridica,
empresas.capital_social as capital_social,
estabelecimento.cep as cep,
estabelecimento.uf as uf,
(SELECT municipio.descricao FROM municipio WHERE municipio.codigo = estabelecimento.municipio) as municipio,
estabelecimento.bairro as bairro,
estabelecimento.logradouro as logradouro,
estabelecimento.numero as numero, 
estabelecimento.complemento as complemento,
-- Melhorar essa atrocidade --
(SELECT GROUP_CONCAT((socios.nome_socio || '-' || socios.cnpj_cpf_socio || '-' || (SELECT descricao FROM qualificacao_socio WHERE qualificacao_socio.codigo = socios.qualificacao_socio))
, ' || ') AS socios
     FROM socios INDEXED BY idx_socios_cnpj_basico
     WHERE socios.cnpj_basico = estabelecimento.cnpj_basico
 ) AS socios
-- Fim da atrocidade --
FROM estabelecimento
INNER JOIN empresas ON empresas.cnpj_basico = estabelecimento.cnpj_basico
LEFT JOIN simples ON simples.cnpj_basico = estabelecimento.cnpj_basico
INNER JOIN cnae as cnae_principal ON cnae_principal.codigo = estabelecimento.cnae_fiscal
LEFT JOIN cnae as cnae_sec_1 ON cnae_sec_1.codigo = estabelecimento.cnae_fiscal_secundaria 
-- FILTROS AQUI --
WHERE estabelecimento.correio_eletronico != ''
AND
tipo_email != 'contabilidade'
LIMIT 50