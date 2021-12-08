CREATE TABLE new_socios (cnpj_b TEXT, socios TEXT);
CREATE INDEX idx_new_socios_cnpj_b ON new_socios (cnpj_b);
INSERT INTO new_socios (cnpj_b, socios) SELECT cnpj_b,
       "[" || Group_concat(
       "{'cnpj': " || cnpj ||
       ",'id_socio': "|| id_socio ||
       ", 'nom_socio': " || nom_socio ||
       ", 'cpf_socio': " || cpf_socio ||
       ", 'qual_socio': " || qual_socio ||
       ", 'dt_ent_soc': " || dt_ent_soc ||
       ", 'pais': " || pais ||
       ", 'rep_legal': " || rep_legal ||
       ", 'nome_rep': " || nome_rep ||
       ", 'qual_rep_legal': " || qual_rep_legal ||
       ", 'faixa_etaria': " || faixa_etaria) || "}]"
FROM   socios
GROUP  BY cnpj_b;

.mode json
.output cnpj.json

SELECT *
FROM empresas AS em
INNER JOIN estabelecimento AS es ON em.cnpj_b=es.cnpj_b
INNER JOIN new_socios AS so ON em.cnpj_b=so.cnpj_b;
