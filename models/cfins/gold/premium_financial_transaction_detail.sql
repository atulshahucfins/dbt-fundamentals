{{
  config(
    materialized='incremental',
    tags =["premium_financial","gold"]
  )
}}


select seq_prem_fin_tran_det.NEXTVAL PREMIUM_FINANCIAL_TRANSACTION_DETAIL_ID, CLASS_LIMIT_CD,  EXPOSURE_CLASS_CD,  INDIVIDUAL_RISK_RATING_MODIFICATION, 
 TYPE_OF_EXPOSURE_GL,   LOSS_COST_DT,  NAICS_CD,  NUMBER_OF_ADDITIONAL_LOCATIONS,
 NUMBER_OF_RATABLE_EMPLOYESS,  TOTAL_NUMBER_OF_EMPLOYESS, POLICY_FORM,
 RATE_DT, RATE_DEPARTURE_FACTOR_CD, RATING_BASIS_IND, RATING_GROUP_CD,  RATING_ID_CD , 
 RATING_UNIT_VALUE_CD, SCHEDULE_RATING_MODIFICATION_FACTOR_CD,STATE_EXCEPTION_CD, 
 STATE_EXCEPTION_CD_II, STATE_EXCEPTION_CD_II_BOP,POLICY_LIMIT_CD_GL, RATE_MODE_CD, 
 RATE_MODIFICATION_FACTOR_CD, STATE_EXCEPTION_CD_TYPE,ZONE_RATING_ID_CD,  
 COVERAGE_DECLARATION_PAGE_CD,  PART_OF_PREMIUM_IND, POLICY_TYPE_ISO_CD,
 SHA2_RECORD,
  current_timestamp::timestamp_ntz created_dttm,
  current_timestamp::timestamp_ntz modified_dttm,
  current_user created_user,
  current_user modified_user
from  {{ref('stg_premium_financial_transaction_detail')}}
where sha2_record not in (select sha2_record from  {{ this }} )