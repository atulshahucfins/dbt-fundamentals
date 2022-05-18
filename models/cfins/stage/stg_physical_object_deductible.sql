{{config ( 
    materialized ='table',
    tags =["tag_in_code"]
)}}


with physical_object_deductible_cf as
(
select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,t2.BLDNUM as Building_Number, t2.LOCNUM as Location_Number, t1.EDSNO, GPMDDT as Deductible_Type, 
  'GLPremisesDeductible' as Deductible_Option , GPMDD as Deductible_Amount
  ,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date  
  from "PRD_CAESAR_RL_DB"."WINSCF".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSCF".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(GPMDD), '') is not null -- to remove records which are not having any GL premises value
UNION ALL
select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,t2.BLDNUM as Building_Number, t2.LOCNUM as Location_Number, t1.EDSNO, GPRDDT as Deductible_Type, 
  'ProductsDeductible' as Deductible_Option , GPRDD as Deductible_Amount
  ,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date 
  from "PRD_CAESAR_RL_DB"."WINSCF".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSCF".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where NULLIF(TRIM(GPRDD), '') is not null -- to remove records which are not having any GL premises value
),
physical_object_deductible_fc as
(
select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,t2.BLDNUM as Building_Number, t2.LOCNUM as Location_Number, t1.EDSNO, GPMDDT as Deductible_Type, 
  'GLPremisesDeductible' as Deductible_Option , GPMDD as Deductible_Amount
  ,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date  
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(GPMDD), '') is not null -- to remove records which are not having any GL premises value
UNION ALL
select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,t2.BLDNUM as Building_Number, t2.LOCNUM as Location_Number, t1.EDSNO, GPRDDT as Deductible_Type, 
  'ProductsDeductible' as Deductible_Option , GPRDD as Deductible_Amount
  ,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date 
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where NULLIF(TRIM(GPRDD), '') is not null -- to remove records which are not having any GL premises value
)
select concat('CFW','|',Policy_Number,'|',Effective_Date) Product_specification_key,Coverage_Key,
concat(Location_number,'||',Building_number,'||',Policy_Number) as Physical_object_Key,Deductible_Type, Deductible_Option, Deductible_Amount,
Effective_Date, Expiration_Date , dateadd(ns,EDSNO*100,Effective_Date) as Start_Date
from physical_object_deductible_cf
UNION ALL
select concat('FCW','|',Policy_Number,'|',Effective_Date) Product_specification_key,Coverage_Key,
concat(Location_number,'||',Building_number,'||',Policy_Number) as Physical_object_Key,Deductible_Type, Deductible_Option, Deductible_Amount,
Effective_Date, Expiration_Date , dateadd(ns,EDSNO*100,Effective_Date) as Start_Date
from physical_object_deductible_fc