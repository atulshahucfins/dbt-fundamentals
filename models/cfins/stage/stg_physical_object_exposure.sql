{{config ( 
    materialized ='table',
    tags =["tag_in_code","exposure_entity"]
)}}

with physical_object_exposure_cf as
(
select distinct ltrim(POLICY,0) Policy_Number,
  ltrim(LOCNUM,0) location_number,
ltrim(BLDNUM,0) building_number,
ltrim(PRMSTE,0) PRMSTE,
ltrim(CLASX,0) CLASX,
ltrim(SUBLN,0) SUBLN,
ltrim(INTCOV,0) internal_coverage, date(nullifzero(EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date ,EXPANN Exposure_Amount,EXPOSE Exposure_Count, 'NA'  Exposure_Type,
  try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date
  from  {{source('PRD_CAESAR_RL_DB_WINSCF','DWXP050')}}
)
,
 physical_object_exposure_fc as
(
select distinct ltrim(POLICY,0) Policy_Number,
  ltrim(LOCNUM,0) location_number,
ltrim(BLDNUM,0) building_number,
ltrim(PRMSTE,0) PRMSTE,
ltrim(CLASX,0) CLASX,
ltrim(SUBLN,0) SUBLN,
ltrim(INTCOV,0) internal_coverage,date(nullifzero(EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date ,EXPANN Exposure_Amount,EXPOSE Exposure_Count, CONLIM  Exposure_Type,--LEXPIND ,
  try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date
  from {{source('PRD_CAESAR_RL_DB_WINSFC','DWXP050')}}
),

final as (
select concat('CFW','||',Policy_Number,'||',Effective_Date) Policy_Key,
  nvl(concat('CFW','||',Policy_Number,'||',Effective_Date,'||',building_number,'||',PRMSTE,'||', CLASX,'||', SUBLN,'||',internal_coverage),'NA') Coverage_Key ,
  nvl(concat(Location_number,'||',Building_number,'||',Policy_Number),'NA') as Physical_object_Key,
  Exposure_Amount, Exposure_Count, Exposure_Type,Effective_Date, Expiration_Date , Start_Date
from physical_object_exposure_cf
UNION ALL
select concat('FCW','||',Policy_Number,'||',Effective_Date) Policy_Key,
  nvl(concat('FCW','||',Policy_Number,'||',Effective_Date,'||',building_number,'||',PRMSTE,'||', CLASX,'||', SUBLN,'||',internal_coverage),'NA') Coverage_Key ,
  nvl(concat(Location_number,'||',Building_number,'||',Policy_Number),'NA') as Physical_object_Key,
  Exposure_Amount, Exposure_Count, Exposure_Type, Effective_Date, Expiration_Date , Start_Date
from physical_object_exposure_fc)

select Policy_Key,Coverage_Key,Physical_object_Key, Exposure_Amount, Exposure_Count, Exposure_Type,
Effective_Date, Expiration_Date , Start_Date, 
sha2(concat(Policy_Key,'||',Coverage_Key,'||',Physical_object_Key,'||', Exposure_Amount,'||', Exposure_Count,'||', Exposure_Type,'||',
Effective_Date,'||', Expiration_Date ,'||', Start_Date)) as sha2_record
from final