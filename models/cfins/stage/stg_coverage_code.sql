{{config ( 
    materialized ='table',
    tags =["ref_table"]
)}}

 
 with coverage_code_winscf as
 (
 select sha2(concat(trim(cdkey1),'||',trim(cdkey2))) as coverage_code_id,
   'CF_WINS' as coverage_source,
   concat(trim(cdkey1),'||',trim(cdkey2)) as coverage_code,
   cddesc as Coverage_Description,
   date(nvl(nullifzero(EFFDTE),99991231),'YYYYMMDD') as start_date,
   date(nvl(nullifzero(ENDDTE),99991231),'YYYYMMDD') as end_date
   
   from {{source('PRD_CAESAR_RL_DB_WINSCF','DWXF024')}}
 ),
 coverage_code_winscf_rnk as(
 select *, iff(current_date>end_date,'N','Y') as ACTIVE, row_number() over( partition by coverage_code_id order by start_date desc) rnk
 from coverage_code_winscf
 ),
 
coverage_code_winsfc as
 (
 select sha2(concat(trim(cdkey1),'||',trim(cdkey2))) as coverage_code_id,
   'FC_WINS' as coverage_source,
   concat(trim(cdkey1),'||',trim(cdkey2)) as coverage_code,
   cddesc as Coverage_Description,
   date(nvl(nullifzero(EFFDTE),99991231),'YYYYMMDD') as start_date,
   date(nvl(nullifzero(ENDDTE),99991231),'YYYYMMDD') as end_date
 
   
   from {{source('PRD_CAESAR_RL_DB_WINSFC','DWXF024')}}
    where enddte<>20111131
 ),
  coverage_code_winsfc_rnk as(
 select *, iff(current_date>end_date,'N','Y') as ACTIVE, row_number() over( partition by coverage_code_id order by start_date desc) rnk
 from coverage_code_winsfc
 )
 select coverage_code_id,coverage_source,coverage_code,coverage_description,start_date,end_date,active from coverage_code_winscf_rnk
 where rnk=1
 union all
 select coverage_code_id,coverage_source,coverage_code,coverage_description,start_date,end_date,active from coverage_code_winsfc_rnk
 where rnk=1