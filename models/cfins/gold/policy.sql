{{
  config(
    materialized='incremental',
    tags =["policy","gold"]
  )
}}


with final as (
select  distinct 
  product_specification_key ,
  policy_number ,
  source_system_id ,
  writing_company_id ,
  business_type ,
  policy_effective_date ,
  policy_expiration_date ,
  policy_term ,
  start_date,
  sha2_record,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from {{ref('stg_policy')}}
where sha2_record not in (select sha2_record from  {{ this }} ))
select seq_policy.NEXTVAL policy_id ,
  product_specification_key ,
  policy_number ,
  source_system_id ,
  writing_company_id ,
  business_type ,
  policy_effective_date ,
  policy_expiration_date ,
  policy_term ,
  start_date,
  sha2_record,
  created_dttm,
  modified_dttm,
  created_user,
  modified_user
from final