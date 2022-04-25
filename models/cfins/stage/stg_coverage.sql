{{config ( 
    materialized ='table',
    tags =["stg"]
)}}


select concat('FCW',cast ((ltrim(POLICY,0)) as varchar(50)),'||', cast ((ltrim(EFFDTE,0)) as varchar(50))) as PolicyKey,
 concat('FCW',cast ((ltrim(POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(EFFDTE,0)) as varchar(50)),'||',
             ltrim(LOCNUM,0),'||',
             ltrim(BLDNUM,0),'||',
             ltrim(PRMSTE,0),'||',
             ltrim(CLASX,0),'||',
             ltrim(SUBLN,0),'||',
             ltrim(INTCOV,0)) as Coverage_Key ,
date(nullifzero(EFFDTE),'YYYYMMDD')  Coverage_Effective_Dt,
date(nullifzero(COVEND),'YYYYMMDD')  Coverage_End_Dt,
date(nullifzero(EXPDTE),'YYYYMMDD')  Coverage_Expiration_Dt,
'NA' Coverage_Code_ID,
'NA' Coverage_ID,
'NA' Coverage_Start_Dt,
'NA' Active_Ind,
'NA'Transaction_Key
          
from CF_PreStaging.Public.DWXP050 
UNION ALL
select concat('CFW',cast ((ltrim(POLICY,0)) as varchar(50)),'||', cast ((ltrim(EFFDTE,0)) as varchar(50))) as PolicyKey,
 concat('CFW',cast ((ltrim(POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(EFFDTE,0)) as varchar(50)),'||',
             ltrim(LOCNUM,0),'||',
             ltrim(BLDNUM,0),'||',
             ltrim(PRMSTE,0),'||',
             ltrim(CLASX,0),'||',
             ltrim(SUBLN,0),'||',
             ltrim(INTCOV,0)) as Coverage_Key ,
date(nullifzero(EFFDTE),'YYYYMMDD')  Coverage_Effective_Dt,
date(nullifzero(COVEND),'YYYYMMDD')  Coverage_End_Dt,
date(nullifzero(EXPDTE),'YYYYMMDD')  Coverage_Expiration_Dt,
'NA' Coverage_Code_ID,
'NA' Coverage_ID,
'NA' Coverage_Start_Dt,
'NA' Active_Ind,
'NA'Transaction_Key           
from CF_PreStaging.Public.CF2FLT150P_DWXP050