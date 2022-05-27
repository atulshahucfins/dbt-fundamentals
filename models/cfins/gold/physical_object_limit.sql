{{
  config(
    materialized='incremental',
    tags =["physical_object_limit","gold"]
  )
}}


select distinct Product_specification_key,
  Coverage_Key ,
  Physical_object_Key,
  Limit_Type, 
  Limit_Option, 
  Limit_Amount,
  Effective_Date,
  Expiration_Date ,
  Start_Date,
  sha2_record,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from {{ref('stg_physical_object_limit')}}
where sha2_record not in (select sha2_record from  {{ this }} )