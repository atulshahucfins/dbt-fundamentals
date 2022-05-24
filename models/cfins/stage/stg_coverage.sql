{{config ( 
    materialized ='table',
    tags =["tag_in_code"]
)}}

with coverage_fc as (
select ltrim(POLICY,0) Policy_Number,
ltrim(c1.LOCNUM,0) location_number,
ltrim(c1.BLDNUM,0) building_number,
ltrim(c1.PRMSTE,0) PRMSTE,
ltrim(c1.CLASX,0) CLASX,
ltrim(c1.SUBLN,0) SUBLN,
ltrim(c1.INTCOV,0) internal_coverage,
date(nullifzero(c1.EFFDTE),'YYYYMMDD')  Coverage_Effective_Dt,
date(nullifzero(c1.COVEND),'YYYYMMDD')  Coverage_End_Dt,
date(nullifzero(c1.EXPDTE),'YYYYMMDD')  Coverage_Expiration_Dt,
c2.coverage_code_id Coverage_Code_ID,
try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') Coverage_Start_Dt,
'NA' Active_Ind,
'NA'Transaction_Key
          
from {{source('PRD_CAESAR_RL_DB_WINSFC','DWXP050')}} c1
left join {{ref('stg_coverage_code')}} c2
on concat(trim(c1.PROD),'||',trim(c1.INTCOV)) = C2.coverage_code
and c2.coverage_source ='FC_WINS'
),

coverage_cf as(
select ltrim(POLICY,0) Policy_Number,
ltrim(c1.LOCNUM,0) location_number,
ltrim(c1.BLDNUM,0) building_number,
ltrim(c1.PRMSTE,0) PRMSTE,
ltrim(c1.CLASX,0) CLASX,
ltrim(c1.SUBLN,0) SUBLN,
ltrim(c1.INTCOV,0) internal_coverage,
date(nullifzero(c1.EFFDTE),'YYYYMMDD')  Coverage_Effective_Dt,
date(nullifzero(c1.COVEND),'YYYYMMDD')  Coverage_End_Dt,
date(nullifzero(c1.EXPDTE),'YYYYMMDD')  Coverage_Expiration_Dt,
c2.coverage_code_id Coverage_Code_ID,
try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') Coverage_Start_Dt,
'NA' Active_Ind,
'NA'Transaction_Key           
from {{source('PRD_CAESAR_RL_DB_WINSCF','DWXP050')}} c1
left join {{ref('stg_coverage_code')}} c2
on concat(trim(c1.PROD),'||',trim(c1.INTCOV)) = C2.coverage_code
and c2.coverage_source ='CF_WINS'
)
select sha2(concat('FCW','||',Policy_Number,'||',
              Coverage_Effective_Dt,'||',building_number,'||',PRMSTE,'||', CLASX,'||', SUBLN,'||',internal_coverage) ) coverage_id, 
        concat('FCW','||',Policy_Number,'||',Coverage_Effective_Dt) policy_key,  concat('CFW','||',Policy_Number,'||',
              Coverage_Effective_Dt,'||',building_number,'||',PRMSTE,'||', CLASX,'||', SUBLN,'||',internal_coverage) Coverage_Key , 
        coverage_code_id, coverage_effective_dt,coverage_end_dt,coverage_Expiration_dt,coverage_Start_dt,active_ind, transaction_key 
from coverage_fc
UNION ALL
select sha2(concat('CFW','||',Policy_Number,'||',
              Coverage_Effective_Dt,'||',building_number,'||',PRMSTE,'||', CLASX,'||', SUBLN,'||',internal_coverage) ) coverage_id, 
        concat('CFW','||',Policy_Number,'||',Coverage_Effective_Dt) policy_key,  concat('CFW','||',Policy_Number,'||',
              Coverage_Effective_Dt,'||',building_number,'||',PRMSTE,'||', CLASX,'||', SUBLN,'||',internal_coverage) Coverage_Key , coverage_code_id, coverage_effective_dt, 
        coverage_end_dt,coverage_Expiration_dt,coverage_Start_dt,active_ind, transaction_key
from coverage_cf
           