SELECT t1.`job_id`, space_id, path,
  `product`,
  `model_type`,
  `company_id`,
  `job_status`,
  `import_status`,
  `submitted_at`,
  `finished_at`,
  `triggered_by_type`,
  `triggered_by_id`,
  `error_code`,
  `company_slug`,
  `source_type`,
  `parent_job_id`,
  `job_type`,
  `import_row_count`,
  `row_count`,
  `size_mb`,
  `tests_status`,
  `data_contracts_status`,
  `sf_acc_name`, -- SF
  `sf_acc_type`, -- SF
  `sf_acc_date_of_first_contract_c`, -- SF
  `sf_acc_status_c`, -- SF
  `sf_acc_region_c`, -- SF
  `sf_acc_cs_owner_name`, --SF
  `sf_acc_sales_owner_name` --SF
                     FROM {{ ref('cs_hackathon_braulio', 'stg_jobs') }}  t1
                     LEFT JOIN  (SELECT job_id,
        MAX(IF(step_type = 'import_discover_finished', JSON_EXTRACT_SCALAR(details, '$.write_disposition'), NULL)) job_type,
        MAX(IF(step_type = 'import_finished', CAST(JSON_QUERY(details, '$.current_import_row_count') as INT), 0)) import_row_count,
        MAX(IF(step_type = 'import_finished', CAST(JSON_QUERY(details, '$.row_count') as INT), 0)) row_count,
        MAX(IF(step_type = 'import_finished', CAST(JSON_QUERY(details, '$.size_in_bytes') as INT)/1024/1024, NULL)) size_mb
     FROM {{ source('cs_hackathon_braulio', 'src_job_info_service_job_step') }}
     WHERE step_type IN ('import_discover_finished', 'import_finished')
     GROUP BY 1
 )  t2
                     ON t1.`job_id` = t2.`job_id`
LEFT JOIN {{ ref('cs_hackathon_braulio', 'stg_sf_y42_orgs') }} t3
ON t1.`company_slug` = t3.`slug`
LEFT JOIN {{ source('cs_hackathon_braulio', 'mrt_sf_accounts') }} t4
ON t3.`sf_account_id` = t4.`sf_acc_id`
