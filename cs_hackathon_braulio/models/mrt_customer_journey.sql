SELECT
  `sf_acc_name`,
  `sf_acc_status_c`,
  `sf_acc_date_of_first_contract_c`,
  `sf_acc_region_c`,
  `sf_acc_kick_off_date_c`,
  `sf_acc_time_to_engagement_in_days_c`,
  `sf_acc_implementation_achieved_date_c`,
  `sf_acc_time_to_implementation_in_days_c`,
  `sf_acc_adoption_achieved_date_c`,
  `sf_acc_time_to_adoption_in_days_c`,
  `sf_acc_strategic_account_c`,
  `sf_acc_ideal_serving_mode_c`,
  `sf_acc_cs_owner_name`,
  CASE
  WHEN `sf_acc_kick_off_date_c` IS NULL THEN 'Kick-off'
  WHEN `sf_acc_implementation_achieved_date_c` IS NULL THEN 'Implementation'
  WHEN `sf_acc_adoption_achieved_date_c` IS NULL THEN 'Adoption'
  ELSE 'Complete'
  END AS next_journey_stage,
  CASE
  WHEN `sf_acc_kick_off_date_c` IS NULL THEN DATE_DIFF(CURRENT_DATE(),`sf_acc_date_of_first_contract_c`, DAY)
  WHEN `sf_acc_implementation_achieved_date_c` IS NULL THEN DATE_DIFF(CURRENT_DATE(),`sf_acc_kick_off_date_c`, DAY)
  WHEN `sf_acc_adoption_achieved_date_c` IS NULL THEN DATE_DIFF(CURRENT_DATE(),`sf_acc_implementation_achieved_date_c`,DAY)
  END AS days_in_current_stage
  FROM {{ source('cs_hackathon_braulio', 'mrt_sf_customers') }}
