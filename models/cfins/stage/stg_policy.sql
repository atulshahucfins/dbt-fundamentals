{{config ( 
    materialized ='table',
    tags =["policy"]
)}}

with policy_fcw as ( 
select 
ltrim(POLICY,0)  Policy_Number,
NVL(NULLIF(TRIM(RTEMTD ), ''),'NA') Source_System_ID,
CO Writing_Company_ID,
DIRASS Business_Type,
date(nullifzero(EFFDTE),'YYYYMMDD') Policy_Effective_Date,
date(nullifzero(EXPDTE),'YYYYMMDD')  Policy_Expiration_Date,
POLTRM Policy_Term,
try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date

FROM 
{{source('PRD_CAESAR_RL_DB_WINSFC','DWXP010')}}  ),

policy_cfw as(
select
ltrim(POLICY,0) Policy_Number,
NVL(NULLIF(TRIM(RTEMTD ), ''),'NA')  Source_System_ID,
CO Writing_Company_ID,
DIRASS Business_Type,
date(nullifzero(EFFDTE),'YYYYMMDD') Policy_Effective_Date,
date(nullifzero(EXPDTE),'YYYYMMDD')  Policy_Expiration_Date,
FIELDF Product_specification_key,
POLTRM Policy_Term,
try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date

FROM 
{{source('PRD_CAESAR_RL_DB_WINSCF','DWXP010')}} ),

final as (
select 
  concat('FCW','||',Policy_Number,'||',Policy_Effective_Date) Product_specification_key,Policy_Number,
  Source_System_ID,Writing_Company_ID,Business_Type,
Policy_Effective_Date,Policy_Expiration_Date,Policy_Term,Start_Date from policy_fcw

UNION ALL

select 
  concat('CFW','||',Policy_Number,'||',Policy_Effective_Date) Product_specification_key,Policy_Number,
  Source_System_ID,Writing_Company_ID,Business_Type,
Policy_Effective_Date,Policy_Expiration_Date,Policy_Term,Start_Date from policy_cfw)

select distinct Product_specification_key,Policy_Number,Source_System_ID,Writing_Company_ID,Business_Type,
Policy_Effective_Date,Policy_Expiration_Date,Policy_Term,Start_Date,
sha2(concat( Product_specification_key,'||',Policy_Number,'||',Source_System_ID,'||',Writing_Company_ID,'||',Business_Type,'||',
Policy_Effective_Date,'||',Policy_Expiration_Date,'||',Policy_Term,'||',Start_Date)) SHA2_RECORD

from final