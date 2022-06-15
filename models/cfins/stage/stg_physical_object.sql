{{config ( 
    materialized ='table',
    tags =["tag_in_code"]
)}}

with physical_object_cf 
as
(
 
select  ANNMIL as Annual_Miles,BLDNUM  as Building_Number,
LICDTE as License_Date, LOCNUM as Location_Number, 'NA' as Address_Id ,
ltrim(POLICY,0) Policy_Number,date(nullifzero(EFFDTE),'YYYYMMDD') Effective_Date,
date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date,
'NA' as END_Date, PRMSTE,
try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date
  from {{source('PRD_CAESAR_RL_DB_WINSCF','DWXP050')}}
),
physical_object_fc
as
(
 
select  ANNMIL as Annual_Miles,BLDNUM  as Building_Number,
LICDTE as License_Date, LOCNUM as Location_Number, 'NA' as Address_Id ,
ltrim(POLICY,0) Policy_Number,date(nullifzero(EFFDTE),'YYYYMMDD') Effective_Date,
date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date,
'NA' as END_Date, PRMSTE,
try_to_timestamp_ntz(concat(TRDATE::varchar,' ',TRTIME::varchar),'YYYYMMDD HH24MISSFF9') start_date
  from {{source('PRD_CAESAR_RL_DB_WINSFC','DWXP050')}}
),

final as ( 
select concat(Location_number,'||',Building_number,'||',Policy_Number) as Physical_object_Key, sha2(concat(Location_number,'||',Building_number,'||',Policy_Number)) as Physical_Object_Id, 
Annual_Miles, Building_Number, License_Date, Location_Number, concat('CFW','||',cast ((ltrim(Policy_Number,0)) as varchar(50)),'||',
              cast ((ltrim(Effective_Date,0)) as varchar(50)),'||',
             ltrim(Location_number,0),'||',
             ltrim(Building_Number,0),'||',
             ltrim(PRMSTE,0)) as Physical_Object_Description, concat('CFW','||',Policy_Number,'||',Effective_Date) Product_specification_key, END_Date,Effective_Date, Expiration_Date,Start_Date


 from physical_object_cf
UNION ALL
select concat(Location_number,'||',Building_number,'||',Policy_Number) as Physical_object_Key, sha2(concat(Location_number,'||',Building_number,'||',Policy_Number)) as Physical_Object_Id, 
Annual_Miles, Building_Number, License_Date, Location_Number, concat('FCW','||',cast ((ltrim(Policy_Number,0)) as varchar(50)),'||',
              cast ((ltrim(Effective_Date,0)) as varchar(50)),'||',
             ltrim(Location_number,0),'||',
             ltrim(Building_Number,0),'||',
             ltrim(PRMSTE,0)) as Physical_Object_Description, concat('FCW','||',Policy_Number,'||',Effective_Date) Product_specification_key, END_Date,Effective_Date, Expiration_Date,Start_Date


 from physical_object_fc)
 
 select Physical_object_Key, Physical_Object_Id, Annual_Miles, Building_Number, License_Date, Location_Number, 
 Physical_Object_Description, Product_specification_key, END_Date,Effective_Date, Expiration_Date,Start_Date, 
 sha2(concat( Physical_object_Key,'||', Physical_Object_Id,'||', Annual_Miles,'||', Building_Number,'||', License_Date,'||', Location_Number, '||',
 Physical_Object_Description,'||',Product_specification_key,'||', END_Date,'||',Effective_Date,'||', Expiration_Date,'||',Start_Date)) sha2_record
 from final