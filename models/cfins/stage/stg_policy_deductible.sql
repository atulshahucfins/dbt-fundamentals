{{config ( 
    materialized ='table',
    tags =["tag_in_code","deductible_entity"]
)}}

with policy_deductible_cf as
(
select distinct ltrim(t1.POLICY,0) Policy_Number,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date , t1.EDSNO, GPRDDT as Deductible_Type, 
  'ProductsDeductible' as Deductible_Option , GPRDD as Deductible_Amount
  from "PRD_CAESAR_RL_DB"."WINSCF".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSCF".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(GPRDD), '') is not null -- to remove records which are not having any GL premises value
UNION ALL
select distinct ltrim(t1.POLICY,0) Policy_Number,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date , t1.EDSNO, GPMDDT as Deductible_Type, 
  'GLPremisesDeductible' as Deductible_Option , GPMDD as Deductible_Amount
  from "PRD_CAESAR_RL_DB"."WINSCF".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSCF".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where NULLIF(TRIM(GPMDD), '') is not null -- to remove records which are not having any GL premises value
)
,
 policy_deductible_fc as
(
select distinct ltrim(t1.POLICY,0) Policy_Number,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date , t1.EDSNO, GPRDDT as Deductible_Type, 
  'ProductsDeductible' as Deductible_Option , GPRDD as Deductible_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP010 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(GPRDD), '') is not null -- to remove records which are not having any GL premises value
UNION ALL
select distinct ltrim(t1.POLICY,0) Policy_Number,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nvl(nullifzero(EXPDTE),99991231),'YYYYMMDD') Expiration_Date , t1.EDSNO, GPMDDT as Deductible_Type, 
  'GLPremisesDeductible' as Deductible_Option , GPMDD as Deductible_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP010 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where NULLIF(TRIM(GPMDD), '') is not null -- to remove records which are not having any GL premises value
),

final as (
select concat('CFW','||',Policy_Number,'||',Effective_Date) Product_specification_key, Deductible_Type, Deductible_Option, Deductible_Amount,
Effective_Date, Expiration_Date , dateadd(ns,EDSNO*100,Effective_Date) as Start_Date
from policy_deductible_cf
UNION ALL
select concat('FCW','||',Policy_Number,'||',Effective_Date) Product_specification_key, Deductible_Type, Deductible_Option, Deductible_Amount,
Effective_Date, Expiration_Date , dateadd(ns,EDSNO*100,Effective_Date) as Start_Date
from policy_deductible_fc)

select Product_specification_key, Deductible_Type, Deductible_Option, Deductible_Amount,
Effective_Date, Expiration_Date , Start_Date, 
sha2(concat(Product_specification_key,'||', Deductible_Type,'||', Deductible_Option,'||', Deductible_Amount,'||',
Effective_Date,'||', Expiration_Date ,'||', Start_Date)) as  sha2_record
from final