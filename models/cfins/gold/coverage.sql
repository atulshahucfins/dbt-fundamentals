{{
  config(
    materialized='incremental',
    tags =["coverage","gold"]
  )
}}


select distinct coverage_id,
  policy_key,
  coverage_key,
  coverage_code_id,
  coverage_effective_dt,
  coverage_end_dt,
  coverage_expiration_dt,
  coverage_start_dt,
  active_ind,
  transaction_key,
  sha2_record,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from {{ref('stg_coverage')}}
where sha2_record not in (select sha2_record from  {{ this }} )