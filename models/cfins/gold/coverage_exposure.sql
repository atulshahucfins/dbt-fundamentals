{{
  config(
    materialized='incremental',
    tags =["coverage_exposure","gold"]
  )
}}

select distinct Policy_Key, 
  Coverage_key,
  Exposure_Amount, 
  Exposure_Count,
  Exposure_Type,
  Effective_Date, 
  Expiration_Date ,
  Start_Date,
  sha2_record,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from {{ref('stg_coverage_exposure')}}
where sha2_record not in (select sha2_record from   {{ this }} )