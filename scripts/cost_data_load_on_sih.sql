INSERT INTO datasus.sih_refined.tb_sih_cost_analysis 

SELECT
    ANO_CMPT,
    ESPEC,
    desc_leito, 
    N_AIH,
    desc_tipo_aih, 
    IDENT,
    MUNIC_RES,
    desc_uf_residencia, 
    desc_municipio_residencia, 
    desc_regiao_residencia, 
    SEXO,
    desc_sexo,
    PROC_SOLIC,
    desc_procedimento_solicitado, 
    PROC_REA,
    desc_procedimento_realizado,
    DIAG_PRINC,
    desc_diag_principal_capitulo,
    desc_diag_principal_categoria,
    desc_diag_principal_subcategoria,
    MUNIC_MOV,
    desc_uf_atendimento,
    desc_municipio_atendimento,
    desc_regiao_atendimento, 
    IDADE,
    CASE 
        WHEN IDADE BETWEEN 0 AND 5 THEN '0...5 anos'
        WHEN IDADE BETWEEN 6 AND 10 THEN '6...10 anos'
        WHEN IDADE BETWEEN 11 AND 15 THEN '11...15 anos'
        WHEN IDADE BETWEEN 16 AND 20 THEN '16...20 anos'
        WHEN IDADE BETWEEN 21 AND 25 THEN '21...25 anos'
        WHEN IDADE BETWEEN 26 AND 30 THEN '26...30 anos'
        WHEN IDADE BETWEEN 31 AND 35 THEN '31...35 anos'

        WHEN IDADE BETWEEN 36 AND 40 THEN '36...40 anos'
        WHEN IDADE BETWEEN 41 AND 45 THEN '41...45 anos'
        WHEN IDADE BETWEEN 46 AND 50 THEN '46...50 anos'
        WHEN IDADE BETWEEN 51 AND 55 THEN '51...55 anos'
        WHEN IDADE BETWEEN 46 AND 60 THEN '56...60 anos'
        WHEN IDADE BETWEEN 61 AND 65 THEN '61...65 anos'
        WHEN IDADE BETWEEN 66 AND 70 THEN '66...70 anos'

        WHEN IDADE BETWEEN 71 AND 75 THEN '71...75 anos'
        WHEN IDADE BETWEEN 76 AND 80 THEN '76...80 anos'
        WHEN IDADE BETWEEN 81 AND 85 THEN '81...85 anos'
        WHEN IDADE BETWEEN 86 AND 90 THEN '86...90 anos'
        ELSE 'Acima de 90 anos'
    END desc_faixa_etaria,
    COMPLEX,
    desc_complexidade,
    SUM(VAL_SH) AS VLR_TOTAL_SERV_HOSP, -- Mantem
    SUM(VAL_SP) AS VLR_TOTAL_SERV_PROF, -- Mantem
    SUM(VAL_TOT) AS VLR_TOTAL_AIH, -- Mantem
    SUM(VAL_UTI) AS VLR_TOTAL_UTI -- Mantem
FROM datasus.sih_refined.tb_datasus_sih
GROUP BY
    ANO_CMPT,
    ESPEC,
    desc_leito, 
    N_AIH,
    desc_tipo_aih, 
    IDENT,
    MUNIC_RES,
    desc_uf_residencia, 
    desc_municipio_residencia, 
    desc_regiao_residencia, 
    SEXO,
    desc_sexo,
    PROC_SOLIC,
    desc_procedimento_solicitado, 
    PROC_REA,
    desc_procedimento_realizado,
    DIAG_PRINC,
    desc_diag_principal_capitulo,
    desc_diag_principal_categoria,
    desc_diag_principal_subcategoria,
    MUNIC_MOV,
    desc_uf_atendimento,
    desc_municipio_atendimento,
    desc_regiao_atendimento, 
    IDADE,
    desc_faixa_etaria,
    DIAS_PERM,
    COMPLEX,
    desc_complexidade
;
