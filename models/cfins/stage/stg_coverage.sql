{{config ( 
    materialized ='table',
    tags =["tag_in_code"]
)}}

with coverage_fc as (
select concat('FCW',cast ((ltrim(c1.POLICY,0)) as varchar(50)),'||', cast ((ltrim(c1.EFFDTE,0)) as varchar(50))) as Policy_Key,
 concat('FCW',cast ((ltrim(c1.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(c1.EFFDTE,0)) as varchar(50)),'||',
             ltrim(c1.LOCNUM,0),'||',
             ltrim(c1.BLDNUM,0),'||',
             ltrim(c1.PRMSTE,0),'||',
             ltrim(c1.CLASX,0),'||',
             ltrim(c1.SUBLN,0),'||',
             ltrim(c1.INTCOV,0)) as Coverage_Key ,
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
select concat('CFW',cast ((ltrim(c1.POLICY,0)) as varchar(50)),'||', cast ((ltrim(c1.EFFDTE,0)) as varchar(50))) as Policy_Key,
 concat('CFW',cast ((ltrim(c1.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(c1.EFFDTE,0)) as varchar(50)),'||',
             ltrim(c1.LOCNUM,0),'||',
             ltrim(c1.BLDNUM,0),'||',
             ltrim(c1.PRMSTE,0),'||',
             ltrim(c1.CLASX,0),'||',
             ltrim(c1.SUBLN,0),'||',
             ltrim(c1.INTCOV,0)) as Coverage_Key ,
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
select sha2(coverage_key) coverage_id, policy_key,  coverage_key, coverage_code_id, coverage_effective_dt, 
coverage_end_dt,coverage_Expiration_dt,coverage_Start_dt,active_ind, transaction_key from
coverage_fc
UNION ALL
select sha2(coverage_key) coverage_id, policy_key,  coverage_key, coverage_code_id, coverage_effective_dt, 
coverage_end_dt,coverage_Expiration_dt,coverage_Start_dt,active_ind, transaction_key
from coverage_cf
           