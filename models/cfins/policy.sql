{{config ( 
    materialized ='table'
)}}

select 
POLICYNUM Policy_Number,
RTEMTD Source_System_ID,
CO Writing_Company_ID,
DIRASS Business_Type,
date(nullifzero(EFFDTE),'YYYYMMDD') Policy_Effective_Date,
date(nullifzero(EXPDTE),'YYYYMMDD')  Policy_Expiration_Date,
FIELDF Product_specification_key,
'NA' Policy_Term_ID

FROM 
CF_PreStaging.Public.DWXP010
UNION ALL
select 
POLICYNUM Policy_Number,
RTEMTD Source_System_ID,
CO Writing_Company_ID,
DIRASS Business_Type,
date(nullifzero(EFFDTE),'YYYYMMDD') Policy_Effective_Date,
date(nullifzero(EXPDTE),'YYYYMMDD')  Policy_Expiration_Date,
FIELDF Product_specification_key,
'NA' Policy_Term_ID

FROM 
CF_PreStaging.Public.CF2FLT150P_DWXP010