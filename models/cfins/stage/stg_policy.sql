{{config ( 
    materialized ='table',
    tags =["tag_in_code"]
)}}

with policy_fcw as ( 
select 
POLICYNUM Policy_Number,
RTEMTD Source_System_ID,
CO Writing_Company_ID,
DIRASS Business_Type,
date(nullifzero(EFFDTE),'YYYYMMDD') Policy_Effective_Date,
date(nullifzero(EXPDTE),'YYYYMMDD')  Policy_Expiration_Date,
POLTRM Policy_Term

FROM 
 {{source('PRD_CAESAR_RL_DB_WINSFC','DWXP010')}} ),

policy_cfw as(
select
POLICYNUM Policy_Number,
RTEMTD Source_System_ID,
CO Writing_Company_ID,
DIRASS Business_Type,
date(nullifzero(EFFDTE),'YYYYMMDD') Policy_Effective_Date,
date(nullifzero(EXPDTE),'YYYYMMDD')  Policy_Expiration_Date,
FIELDF Product_specification_key,
POLTRM Policy_Term

FROM 
{{source('PRD_CAESAR_RL_DB_WINSCF','DWXP010')}})

select concat('FCW','|',Policy_Number,'|',Policy_Effective_Date) Product_specification_key,Policy_Number,Source_System_ID,Writing_Company_ID,Business_Type,
Policy_Effective_Date,Policy_Expiration_Date,Policy_Term from policy_fcw

UNION ALL

select concat('CFW','|',Policy_Number,'|',Policy_Effective_Date) Product_specification_key,Policy_Number,Source_System_ID,Writing_Company_ID,Business_Type,
Policy_Effective_Date,Policy_Expiration_Date,Policy_Term from policy_cfw