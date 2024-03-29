INSERT INTO datasus.sih_trusted.datasus_sih
WITH DATASUS_SIH AS (
SELECT
    UF_ZI,
    ANO_CMPT,
    MES_CMPT,
    ESPEC,
    CGC_HOSP,
    N_AIH,
    IDENT,
    CEP,
    MUNIC_RES,
    TO_DATE(CASE WHEN NASC = '' THEN NULL ELSE NASC END, 'YYYYMMDD') AS DT_NASC,
    SEXO,
    CAST(UTI_MES_IN AS integer) AS UTI_MES_IN,
    CAST(UTI_MES_AN AS integer) AS UTI_MES_AN,
    CAST(UTI_MES_AL AS integer) AS UTI_MES_AL,
    CAST(UTI_MES_TO AS integer) AS UTI_MES_TO,
    MARCA_UTI,
    CAST(UTI_INT_IN AS integer) AS UTI_INT_IN,
    CAST(UTI_INT_AN AS integer) AS UTI_INT_AN,
    CAST(UTI_INT_AL AS integer) AS UTI_INT_AL,
    CAST(UTI_INT_TO AS integer) AS UTI_INT_TO,
    CAST(DIAR_ACOM AS integer) AS DIAR_ACOM,
    CAST(QT_DIARIAS AS integer) AS QT_DIARIAS,
    PROC_SOLIC,
    PROC_REA,
    CAST(VAL_SH AS decimal(8,2)) AS VAL_SH,
    CAST(VAL_SP AS decimal(8,2)) AS VAL_SP,
    CAST(VAL_SADT AS decimal(8,2)) AS VAL_SADT,
    CAST(VAL_RN AS decimal(8,2)) AS VAL_RN,
    CAST(VAL_ACOMP AS decimal(13,2)) AS VAL_ACOMP,
    CAST(VAL_ORTP AS decimal(8,2)) AS VAL_ORTP,
    CAST(VAL_SANGUE AS decimal(8,2)) AS VAL_SANGUE,
    CAST(VAL_SADTSR AS decimal(8,2)) AS VAL_SADTSR,
    CAST(VAL_TRANSP AS decimal(8,2)) AS VAL_TRANSP,
    CAST(VAL_OBSANG AS decimal(8,2)) AS VAL_OBSANG,
    CAST(VAL_PED1AC AS decimal(8,2)) AS VAL_PED1AC,
    CAST(VAL_TOT AS decimal(9,2)) AS VAL_TOT,
    CAST(VAL_UTI AS decimal(8,2)) AS VAL_UTI,
    CAST(US_TOT AS decimal(8,2)) AS US_TOT,
    TO_DATE(CASE WHEN DT_INTER = '' THEN NULL ELSE DT_INTER END, 'YYYYMMDD') AS DT_INTER,
    TO_DATE(CASE WHEN DT_SAIDA = '' THEN NULL ELSE DT_SAIDA END, 'YYYYMMDD') AS DT_SAIDA,
    DIAG_PRINC,
    DIAG_SECUN,
    COBRANCA,
    NATUREZA,
    NAT_JUR,
    GESTAO, 
    CAST(RUBRICA AS integer) AS RUBRICA,
    IND_VDRL,
    MUNIC_MOV,
    COD_IDADE,
    CAST(IDADE AS integer) AS IDADE,
    CAST(DIAS_PERM AS integer) AS DIAS_PERM,
    CAST(MORTE AS integer) AS MORTE,
    NACIONAL,
    NUM_PROC,
    CAR_INT,
    CAST(TOT_PT_SP AS integer) AS TOT_PT_SP,
    CPF_AUT,
    HOMONIMO,
    CAST(NUM_FILHOS AS integer) AS NUM_FILHOS,
    INSTRU,
    CID_NOTIF,
    CONTRACEP1,
    CONTRACEP2,
    GESTRISCO,
    INSC_PN,
    CAST(SEQ_AIH5 AS integer) AS SEQ_AIH5,
    CBOR,
    CNAER,
    VINCPREV,
    GESTOR_COD,
    GESTOR_TP,
    GESTOR_CPF,
    TO_DATE(CASE WHEN GESTOR_DT = '' THEN NULL ELSE GESTOR_DT END, 'YYYYMMDD') AS GESTOR_DT,
    CNES,
    CNPJ_MANT,
    INFEHOSP,
    CID_ASSO,
    CID_MORTE,
    COMPLEX,
    FINANC,
    FAEC_TP,
    REGCT,
    RACA_COR,
    ETNIA,
    CAST(SEQUENCIA AS integer) AS SEQUENCIA,
    REMESSA,
    AUD_JUST,
    SIS_JUST,
    CAST(VAL_SH_FED AS decimal(10,2)) AS VAL_SH_FED,
    CAST(VAL_SP_FED AS decimal(10,2)) AS VAL_SP_FED,
    CAST(VAL_SH_GES AS decimal(10,2)) AS VAL_SH_GES,
    CAST(VAL_SP_GES AS decimal(10,2)) AS VAL_SP_GES,
    CAST(VAL_UCI AS decimal(10,2)) AS VAL_UCI,
    MARCA_UCI,
    DIAGSEC1,
    DIAGSEC2,
    DIAGSEC3,
    DIAGSEC4,
    DIAGSEC5,
    DIAGSEC6,
    DIAGSEC7,
    DIAGSEC8,
    DIAGSEC9,
    TPDISEC1,
    TPDISEC2,
    TPDISEC3,
    TPDISEC4,
    TPDISEC5,
    TPDISEC6,
    TPDISEC7,
    TPDISEC8,
    TPDISEC9,
    DOWNLOADED_AT    
FROM datasus.sih_raw.datasus_sih
WHERE DOWNLOADED_AT >= CURRENT_DATE
QUALIFY ROW_NUMBER() OVER(PARTITION BY N_AIH ORDER BY DT_SAIDA DESC) = 1
)
SELECT 
*
FROM DATASUS_SIH
;