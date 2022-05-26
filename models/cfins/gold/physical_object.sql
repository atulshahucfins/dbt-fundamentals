{{
  config(
    materialized='incremental',
    tags =["physcial_object","gold"]
  )
}}


select distinct Physical_object_Key, 
  Physical_Object_Id, 
  Annual_Miles, 
  Building_Number, 
  License_Date, 
  Location_Number, 
  Physical_Object_Description, 
  Product_specification_key, 
  END_Date,
  Effective_Date, 
  Expiration_Date,
  Start_Date,
  sha2_record,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from {{ref('stg_physical_object')}}
where sha2_record not in (select sha2_record from  {{ this }} )