{{config ( 
    materialized ='table',
    tags =["ref_table"]
)}}

with coverage_code_winscf as
 (
 select sha2(concat(cdkey1,'||',cdkey2)) as coverage_code_id,
   'CF_WINS' as coverage_source,
   concat(cdkey1,'||',cdkey2) as coverage_code,
   cddesc as Coverage_Description,
   date(nvl(nullifzero(EFFDTE),99991231),'YYYYMMDD') as start_date,
   date(nvl(nullifzero(ENDDTE),99991231),'YYYYMMDD') as end_date
   
   from "PRD_CAESAR_RL_DB"."WINSCF"."DWXF024"
 ),
 
coverage_code_winsfc as
 (
 select sha2(concat(cdkey1,'||',cdkey2)) as coverage_code_id,
   'FC_WINS' as coverage_source,
   concat(cdkey1,'||',cdkey2) as coverage_code,
   cddesc as Coverage_Description,
   date(nvl(nullifzero(EFFDTE),99991231),'YYYYMMDD') as start_date,
   date(nvl(nullifzero(ENDDTE),99991231),'YYYYMMDD') as end_date
 
   
   from "PRD_CAESAR_RL_DB"."WINSFC"."DWXF024"
    where enddte<>20111131
 )
 select *, iff(current_date>end_date,'N','Y') as ACTIVE from coverage_code_winscf
 union all
 select *, iff(current_date>end_date,'N','Y') as ACTIVE from coverage_code_winsfc