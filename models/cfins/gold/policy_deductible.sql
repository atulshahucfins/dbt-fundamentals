{{
  config(
    materialized='incremental',
    tags =["policy_deductible","gold"]
  )
}}

select distinct Product_specification_key, 
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
from {{ref('stg_policy_deductible')}}
where sha2_record not in (select sha2_record from  {{ this }})