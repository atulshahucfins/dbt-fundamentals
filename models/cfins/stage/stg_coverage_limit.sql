{{config ( 
    materialized ='table',
    tags =["tag_in_code","limit_entity"]
)}}


with coverage_limit_fc as
(
  --BIAutoCombinedSingleLimits
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIAuto'  as Limit_Type, 
  'CombinedSingle' as Limit_Option , LIACSL  as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWBP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(LIACSL), '') is not null -- to remove records which are not having any BiAutoCombinedSingle value
UNION ALL
    --BIPerOccurrenceLimits
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIPer'  as Limit_Type, 
  'Occurrence' as Limit_Option , LIABIO   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWBP020 t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(LIABIO), '') is not null -- to remove records which are not having any BIPerOccurrenceLimits value
UNION ALL
    --BIGarageCombinedSingleLimits 
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIGarage'  as Limit_Type, 
  'CombinedSingle' as Limit_Option , LIACSL   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DMGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(LIACSL), '') is not null -- to remove records which are not having any BIGarageCombinedSingleLimits value  
UNION ALL
    --BIGaragePerOccurrenceLimits 
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIGaragePer'  as Limit_Type, 
  'Occurrence' as Limit_Option , LIABIO   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DMGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(LIABIO), '') is not null -- to remove records which are not having any BIGaragePerOccurrenceLimits value     
UNION ALL
    --BIPDGLSplitLimitAggregate 
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIPDGL'  as Limit_Type, 
  'SplitLimitAggregate' as Limit_Option , SAGGRG as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(SAGGRG), '') is not null -- to remove records which are not having any BIPDGLSplitLimitAggregate value
UNION ALL
    --BIGLAggregate  
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIGL'  as Limit_Type, 
  'Aggregate' as Limit_Option , BAGGRG as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(BAGGRG), '') is not null -- to remove records which are not having any BIGLAggregate value  
UNION ALL
    --BIGLGeneralAggregate   
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BIGL'  as Limit_Type, 
  'GeneralAggregate' as Limit_Option , GALMT as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(GALMT), '') is not null -- to remove records which are not having any BIGLGeneralAggregate  value  
UNION ALL
    --PDGLProductsCompletedAggregate    
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'PDGLProducts'  as Limit_Type, 
  'CompletedAggregate' as Limit_Option , PCOLMT as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(PCOLMT), '') is not null -- to remove records which are not having any PDGLProductsCompletedAggregate   value 
UNION ALL
    --UmbrellaOccurance    
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'Umbrella'  as Limit_Type, 
  'Occurance' as Limit_Option , SINOCC as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWYP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(SINOCC), '') is not null -- to remove records which are not having any UmbrellaOccurance   value   
UNION ALL
    --GeneralLiabilityPerOccurrenceLimit    
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'GeneralLiabilityPer'  as Limit_Type, 
  'Occurance' as Limit_Option , EOLMT  as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWGP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(EOLMT), '') is not null -- to remove records which are not having any GeneralLiabilityPerOccurrenceLimit value   
UNION ALL
    --UmbrellaAutoDeductibleAttachmentPoint     
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'UmbrellaAuto'  as Limit_Type, 
  'DeductibleAttachmentPoint' as Limit_Option , ATTPNTAU  as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWYP020  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(ATTPNTAU), '') is not null -- to remove records which are not having any UmbrellaAutoDeductibleAttachmentPoint value 
UNION ALL
    --BuildingLimits      
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'NA'  as Limit_Type, 
  'BuildingLimits' as Limit_Option , BLDLIM   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWFP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(BLDLIM), '') is not null -- to remove records which are not having any UmbrellaAutoDeductibleAttachmentPoint value 
UNION ALL
    --PersonalPropertyLimit       
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'NA'  as Limit_Type, 
  'PersonalPropertyLimit' as Limit_Option , PERLIM   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWFP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(PERLIM), '') is not null -- to remove records which are not having any PersonalPropertyLimit value   
UNION ALL
    --BusinessIncomeLimit       
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'NA'  as Limit_Type, 
  'BusinessIncomeLimit' as Limit_Option , BUSLIM   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWFP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(BUSLIM), '') is not null -- to remove records which are not having any BusinessIncomeLimit value   
UNION ALL
    --TotalPropertyLimit        
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'NA'  as Limit_Type, 
  'TotalPropertyLimit' as Limit_Option , EXPLIM   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWFP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(EXPLIM), '') is not null -- to remove records which are not having any TotalPropertyLimit value    
UNION ALL
    --InlandMarineOccurenceLocationLimit          
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'InlandMarine'  as Limit_Type, 
  'OccurenceLocationLimit' as Limit_Option , LOCLMT   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWIP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(LOCLMT), '') is not null -- to remove records which are not having any InlandMarineOccurenceLocationLimit value
UNION ALL
    --BOPBuildingLimit         
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BOP'  as Limit_Type, 
  'BuildingLimit' as Limit_Option , BLDLIM   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWNP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(BLDLIM), '') is not null -- to remove records which are not having any BOPBuildingLimit value
UNION ALL
    --BOPPersonalPropertyLimit        
  select distinct ltrim(t1.POLICY,0) Policy_Number,
  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
             ltrim(t2.LOCNUM,0),'||',
             ltrim(t2.BLDNUM,0),'||',
             ltrim(t2.PRMSTE,0),'||',
             ltrim(t2.CLASX,0),'||',
             ltrim(t2.SUBLN,0),'||',
             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'BOP'  as Limit_Type, 
  'PersonalPropertyLimit' as Limit_Option , BUSPPT   as Limit_Amount
  from "PRD_CAESAR_RL_DB"."WINSFC".DWNP030  t1
  left join "PRD_CAESAR_RL_DB"."WINSFC".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
  where  NULLIF(TRIM(BUSPPT), '') is not null -- to remove records which are not having any BOPBuildingLimit value  
)
//,
//coverage_limit_cf as 
//(
//      --ESPropertyLimitAttachmentPoint         
//  select distinct ltrim(t1.POLICY,0) Policy_Number,
//  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
//              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
//             ltrim(t2.LOCNUM,0),'||',
//             ltrim(t2.BLDNUM,0),'||',
//             ltrim(t2.PRMSTE,0),'||',
//             ltrim(t2.CLASX,0),'||',
//             ltrim(t2.SUBLN,0),'||',
//             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
//   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'ES'  as Limit_Type, 
//  'PropertyLimitAttachmentPoint' as Limit_Option , ATCHPOIN    as Limit_Amount
//  from "PRD_CAESAR_RL_DB"."WINSCF".DWXE010   t1
//  left join "PRD_CAESAR_RL_DB"."WINSCF".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
//  where  NULLIF(TRIM(ATCHPOIN ), '') is not null -- to remove records which are not having any ESPropertyLimitAttachmentPoint value
//UNION ALL
//      --ESPropertyLimit         
//  select distinct ltrim(t1.POLICY,0) Policy_Number,
//  concat('CFW',cast ((ltrim(t2.POLICY,0)) as varchar(50)),'||',
//              cast ((ltrim(t2.EFFDTE,0)) as varchar(50)),'||',
//             ltrim(t2.LOCNUM,0),'||',
//             ltrim(t2.BLDNUM,0),'||',
//             ltrim(t2.PRMSTE,0),'||',
//             ltrim(t2.CLASX,0),'||',
//             ltrim(t2.SUBLN,0),'||',
//             ltrim(t2.INTCOV,0)) as Coverage_Key,date(nullifzero(t1.EFFDTE),'YYYYMMDD') Effective_Date,
//   date(nullifzero(t2.EXPDTE),'YYYYMMDD') Expiration_Date , t1.EDSNO, 'ES'  as Limit_Type, 
//  'PropertyLimit' as Limit_Option , ATCHPOIN    as Limit_Amount
//  from "PRD_CAESAR_RL_DB"."WINSCF".DWXE010   t1
//  left join "PRD_CAESAR_RL_DB"."WINSCF".DWXP050 t2 on t1.policy=t2.policy and t1.EFFDTE=t2.EFFDTE and t1.EDSNO=t2.EDSNO
//  where  NULLIF(TRIM(ATCHPOIN ), '') is not null -- to remove records which are not having any ESPropertyLimit value  
//)
select concat('FCW','|',Policy_Number,'|',Effective_Date) Product_specification_key, Coverage_Key, Limit_Type, Limit_Option, Limit_Amount,
Effective_Date, Expiration_Date , dateadd(ns,EDSNO*100,Effective_Date) as Start_Date
from coverage_limit_fc
//UNION ALL
//select concat('CFW','|',Policy_Number,'|',Effective_Date) Product_specification_key, Coverage_Key,Limit_Type, Limit_Option, Limit_Amount,
//Effective_Date, Expiration_Date , dateadd(ns,EDSNO*100,Effective_Date) as Start_Date
//from coverage_limit_cf
