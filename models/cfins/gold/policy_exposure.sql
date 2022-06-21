{{
  config(
    materialized='incremental',
    tags =["policy_exposure","gold","exposure_entity"]
  )
}}

select distinct Policy_Key, 
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
from {{ref('stg_policy_exposure')}}
where sha2_record not in (select sha2_record from  {{ this }} )