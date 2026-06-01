--cria tabela de códigos porte e situacao
drop table if exists tporte
;
Create table tporte AS
SELECT  CAST('00' as TEXT) as codigo, CAST('Não informado' AS TEXT) as descricao
UNION
SELECT '01', 'Micro empresa'
UNION
SELECT '03', 'Empresa de pequeno porte'
UNION 
SELECT '05', 'Demais (Médio ou Grande porte)'
;
drop table if exists tsituacao
;
Create table tsituacao AS
SELECT CAST('01' AS TEXT) as codigo, CAST('Nula' AS TEXT) as descricao
UNION
SELECT '02', 'Ativa'
UNION
SELECT '03', 'Suspensa'
UNION
SELECT '04', 'Inapta'
UNION
SELECT '08', 'Baixada'
;

