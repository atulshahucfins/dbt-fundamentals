{{
  config(
    materialized='incremental',
    tags =["physical_object_deductible","gold"]
  )
}}


select distinct Product_specification_key,
  Coverage_Key,
  Physical_Object_Key,
  Deductible_Type, 
  Deductible_Option,
  Deductible_Amount,
  Effective_Date, 
  Expiration_Date ,
  Start_Date,
  sha2_record,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from "DBT_STAGE"."DBT_ASHAHU"."STG_PHYSICAL_OBJECT_DEDUCTIBLE"
where sha2_record not in (select sha2_record from  {{ this }} )