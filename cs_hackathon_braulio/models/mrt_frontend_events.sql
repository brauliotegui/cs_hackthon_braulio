Select `context_library_name`,
  `organization_slug`,
  `organization_name`,
  `url`,
  `original_timestamp`,
  `user_id`,
  `uuid_ts`,
  `context_library_version`,
  `customer_email`,
  `is_customer`,
  `name`,
  `id`,
  `customer_id`,
  `product_area`,
  `sf_acc_name`, -- SF
  `sf_acc_type`, -- SF
  `sf_acc_date_of_first_contract_c`, -- SF
  `sf_acc_status_c`, -- SF
  `sf_acc_region_c`, -- SF
  `sf_acc_cs_owner_name`, --SF
  `sf_acc_sales_owner_name` --SF
                     from {{ ref('cs_hackathon_braulio', 'src_page_views') }} t1
LEFT JOIN {{ ref('cs_hackathon_braulio', 'stg_sf_y42_orgs') }} t2
ON t1.`organization_slug` = t2.`slug`
LEFT JOIN {{ source('cs_hackathon_braulio', 'mrt_sf_accounts') }} t3
ON t2.`sf_account_id` = t3.`sf_acc_id`
