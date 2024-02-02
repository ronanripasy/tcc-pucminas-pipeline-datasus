INSERT INTO datasus.sih_refined.tb_datasus_sih
WITH TB_UF AS (
    SELECT 
        CODIGO, DESCRICAO 
    FROM datasus.sih_refined.TB_UF
),
TB_MUNICIPIO AS (
    SELECT 
        MUNCOD, MUNNOME 
    FROM datasus.sih_refined.TB_MUNICIPIO
),
TB_REGIAO_UF AS (
    SELECT 
        CODIGO, DESCRICAO
    FROM datasus.sih_refined.TB_REGIAO_UF
)
,CTE_PRINCIPAL AS (
SELECT
    A.UF_ZI,
    A.ANO_CMPT,
    A.MES_CMPT,
    A.ESPEC,
    LE.DESCRICAO AS desc_leito, -- Nova coluna
    A.CGC_HOSP,
    A.N_AIH,
    CASE 
        WHEN A.IDENT='1' THEN 'Normal' 
        WHEN A.IDENT='5' THEN 'Longa permanência' 
        ELSE 'Não identificado'
    END AS desc_tipo_aih, -- Nova coluna
    A.IDENT,
    A.CEP,
    A.MUNIC_RES,
    UF_RESID.DESCRICAO AS desc_uf_residencia,-- Nova coluna
    MUN_RESID.MUNNOME AS desc_municipio_residencia,-- Nova coluna
    REG_RESID.DESCRICAO AS desc_regiao_residencia, -- Nova coluna
    A.DT_NASC as DT_NASC,
    A.SEXO,
    CASE 
        WHEN A.SEXO='1' THEN 'Masculino' 
        WHEN A.SEXO IN ('2','3') THEN 'Feminino' 
        WHEN A.SEXO IN ('0','9') THEN '' 
        ELSE 'Não identificado'
    END AS desc_sexo, -- Nova coluna
    A.UTI_MES_IN::int AS UTI_MES_IN,
    A.UTI_MES_AN::int AS UTI_MES_AN,
    A.UTI_MES_AL::int AS UTI_MES_AL,
    A.UTI_MES_TO::int AS UTI_MES_TO,
    A.MARCA_UTI,
    A.UTI_INT_IN::int AS UTI_INT_IN,
    A.UTI_INT_AN::int AS UTI_INT_AN,
    A.UTI_INT_AL::int AS UTI_INT_AL,
    A.UTI_INT_TO::int AS UTI_INT_TO,
    A.DIAR_ACOM::int AS DIAR_ACOM,
    A.QT_DIARIAS::int AS QT_DIARIAS,
    A.PROC_SOLIC,
    SOLIC.DESCRICAO AS desc_procedimento_solicitado, -- Nova coluna
    A.PROC_REA,
    REA.DESCRICAO AS desc_procedimento_realizado, --Nova coluna
    A.VAL_SH,
    A.VAL_SP,
    A.VAL_SADT,
    A.VAL_RN,
    A.VAL_ACOMP,
    A.VAL_ORTP,
    A.VAL_SANGUE,
    A.VAL_SADTSR,
    A.VAL_TRANSP,
    A.VAL_OBSANG,
    A.VAL_PED1AC,
    A.VAL_TOT,
    A.VAL_UTI,
    A.US_TOT,
    A.DT_INTER,
    A.DT_SAIDA,
    A.DIAG_PRINC,
    CAP.DESCRICAO_BREVE AS desc_diag_principal_capitulo, --Nova coluna
    CAT.DESCRICAO_BREVE AS desc_diag_principal_categoria, --Nova coluna
    SUB.DESCRICAO_BREVE AS desc_diag_principal_subcategoria, --Nova coluna
    A.DIAG_SECUN,
    A.COBRANCA,
    A.NATUREZA,
    A.NAT_JUR,
    A.GESTAO, 
    A.RUBRICA,
    A.IND_VDRL,
    A.MUNIC_MOV,
    UF_ATEND.DESCRICAO AS desc_uf_atendimento,-- Nova coluna
    MUN_ATEND.MUNNOME AS desc_municipio_atendimento,-- Nova coluna
    REG_ATEND.DESCRICAO AS desc_regiao_atendimento, -- Nova coluna
    A.COD_IDADE,
    A.IDADE,
    A.DIAS_PERM,
    A.MORTE,
    A.NACIONAL,
    A.NUM_PROC,
    A.CAR_INT,
    A.TOT_PT_SP,
    A.CPF_AUT,
    A.HOMONIMO,
    A.NUM_FILHOS,
    A.INSTRU,
    A.CID_NOTIF,
    A.CONTRACEP1,
    A.CONTRACEP2,
    A.GESTRISCO,
    A.INSC_PN,
    A.SEQ_AIH5,
    A.CBOR,
    A.CNAER,
    A.VINCPREV,
    A.GESTOR_COD,
    A.GESTOR_TP,
    A.GESTOR_CPF,
    A.GESTOR_DT,
    A.CNES,
    A.CNPJ_MANT,
    A.INFEHOSP,
    A.CID_ASSO,
    A.CID_MORTE,
    A.COMPLEX,
    CASE 
        WHEN A.COMPLEX='01' then 'Atenção Básica'
        WHEN A.COMPLEX='02' then 'Média complexidade'
        WHEN A.COMPLEX='03' then 'Alta complexidade'
        WHEN A.COMPLEX='00' then ''
        WHEN A.COMPLEX='99' then ''
        ELSE 'Não identificado'
    END AS desc_complexidade,  -- Nova coluna
    A.FINANC,
    A.FAEC_TP,
    A.REGCT,
    A.RACA_COR,
    A.ETNIA,
    A.SEQUENCIA,
    A.REMESSA,
    A.AUD_JUST,
    A.SIS_JUST,
    A.VAL_SH_FED,
    A.VAL_SP_FED,
    A.VAL_SH_GES,
    A.VAL_SP_GES,
    A.VAL_UCI,
    A.MARCA_UCI,
    A.DIAGSEC1,
    A.DIAGSEC2,
    A.DIAGSEC3,
    A.DIAGSEC4,
    A.DIAGSEC5,
    A.DIAGSEC6,
    A.DIAGSEC7,
    A.DIAGSEC8,
    A.DIAGSEC9,
    A.TPDISEC1,
    A.TPDISEC2,
    A.TPDISEC3,
    A.TPDISEC4,
    A.TPDISEC5,
    A.TPDISEC6,
    A.TPDISEC7,
    A.TPDISEC8,
    A.TPDISEC9,
    A.DOWNLOADED_AT
FROM datasus.sih_trusted.datasus_sih AS A
LEFT JOIN TB_UF AS UF_ATEND
    ON SUBSTRING(A.MUNIC_MOV,1,2)=UF_ATEND.CODIGO
LEFT JOIN TB_UF AS UF_RESID
    ON SUBSTRING(A.MUNIC_RES,1,2)=UF_RESID.CODIGO
LEFT JOIN TB_MUNICIPIO AS MUN_ATEND
    ON SUBSTRING(A.MUNIC_MOV,1,6)=MUN_ATEND.MUNCOD
LEFT JOIN TB_MUNICIPIO AS MUN_RESID
    ON SUBSTRING(A.MUNIC_RES,1,6)=MUN_RESID.MUNCOD
LEFT JOIN TB_REGIAO_UF AS REG_ATEND
    ON SUBSTRING(A.MUNIC_MOV,1,2)=REG_ATEND.CODIGO
LEFT JOIN TB_REGIAO_UF AS REG_RESID
    ON SUBSTRING(A.MUNIC_RES,1,2)=REG_RESID.CODIGO
LEFT JOIN datasus.sih_refined.tb_leito AS LE
    ON A.ESPEC=LE.CODIGO
LEFT JOIN datasus.sih_refined.tb_procedimento AS REA
    ON A.PROC_REA=REA.CODIGO
LEFT JOIN datasus.sih_refined.tb_procedimento AS SOLIC
    ON A.PROC_SOLIC=SOLIC.CODIGO
LEFT JOIN datasus.sih_refined.tb_cid_10_capitulo AS CAP
    ON SUBSTRING(A.DIAG_PRINC,1,3)=CAP.CODIGO
LEFT JOIN datasus.sih_refined.tb_cid_10_categoria AS CAT
    ON SUBSTRING(A.DIAG_PRINC,1,3)=CAT.CODIGO
LEFT JOIN datasus.sih_refined.tb_cid_10_subcategoria AS SUB
    ON RPAD(A.DIAG_PRINC, 4,'0')=RPAD(SUB.CODIGO, 4,'0')
WHERE DOWNLOADED_AT >= CURRENT_DATE
)
SELECT
*
FROM CTE_PRINCIPAL

;