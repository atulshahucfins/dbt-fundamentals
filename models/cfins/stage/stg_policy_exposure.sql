{{config ( 
    materialized ='table',
    tags =["tag_in_code","exposure_entity"]
)}}


with policy_exposure_cf as
(
select distinct ltrim(POLICY,0) Policy_Number,date(nullifzero(EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date ,EXPANN Exposure_Amount,EXPOSE Exposure_Count, 'NA'  Exposure_Type,
  try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date
  from  {{source('PRD_CAESAR_RL_DB_WINSCF','DWXP050')}}
)
,
 policy_exposure_fc as
(
select distinct ltrim(POLICY,0) Policy_Number,date(nullifzero(EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date ,EXPANN Exposure_Amount,EXPOSE Exposure_Count, CONLIM  Exposure_Type,--LEXPIND ,
  try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date
  from  {{source('PRD_CAESAR_RL_DB_WINSFC','DWXP050')}}
),

final as (
select concat('CFW','||',Policy_Number,'||',Effective_Date) Policy_Key, Exposure_Amount, Exposure_Count, Exposure_Type,
Effective_Date, Expiration_Date , Start_Date
from policy_exposure_cf
UNION ALL
select concat('FCW','||',Policy_Number,'||',Effective_Date) Policy_Key, Exposure_Amount, Exposure_Count, Exposure_Type,
Effective_Date, Expiration_Date , Start_Date
from policy_exposure_fc)

select Policy_Key, Exposure_Amount, Exposure_Count, Exposure_Type,
Effective_Date, Expiration_Date , Start_Date, 
sha2(concat(Policy_Key,'||', Exposure_Amount,'||', Exposure_Count,'||', Exposure_Type,'||',
Effective_Date,'||', Expiration_Date ,'||', Start_Date)) as sha2_record
from final