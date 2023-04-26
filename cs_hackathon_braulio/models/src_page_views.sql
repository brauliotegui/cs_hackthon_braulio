Select
`context_library_name`,
  `context_library_version`,
  `organization_slug`,
  `organization_name`,
  `url`,
  TIMESTAMP(`original_timestamp`) AS original_timestamp,
  `user_id`,
  `uuid_ts`,
  `customer_email`,
 CASE WHEN REGEXP_CONTAINS(`customer_email`, r'@y42') OR customer_email IS NULL THEN FALSE
  ELSE TRUE
  END AS is_customer,
  `name`,
  `id`,
  `customer_id`,
  CASE
  WHEN REGEXP_CONTAINS(`url`, r'/org/select') THEN 'Home Page'
  WHEN REGEXP_CONTAINS(`url`, r'/space/select') THEN 'Select Space'
  WHEN REGEXP_CONTAINS(`url`, r'/space/create') THEN 'Create Space'
  WHEN REGEXP_CONTAINS(`url`, r'/settings') THEN 'Settings'
  WHEN REGEXP_CONTAINS(`url`, r'/visualizations') OR REGEXP_CONTAINS(`url`, r'visualizations/') THEN 'Visualizations'
  WHEN REGEXP_CONTAINS(`url`, r'/status-alerts') THEN 'Status Alerts'
  WHEN REGEXP_CONTAINS(`url`, r'/exports') OR REGEXP_CONTAINS(`url`, r'exports/') THEN 'Exports'
  WHEN REGEXP_CONTAINS(`url`, r'/lineage') OR REGEXP_CONTAINS(`url`, r'lineage/') THEN 'Lineage'
  WHEN REGEXP_CONTAINS(`url`, r'/integrations') OR REGEXP_CONTAINS(`url`, r'integrations/') THEN 'Integrations'
  WHEN REGEXP_CONTAINS(`url`, r'/models/ui') OR REGEXP_CONTAINS(`url`, r'models/ui/') THEN 'UI Model'
  WHEN REGEXP_CONTAINS(`url`, r'/models/sql') OR REGEXP_CONTAINS(`url`, r'models/sql/') THEN 'SQL Model'
  ELSE 'Other/NULL'
  END AS product_area
                     from {{ source('cs_hackathon_braulio', 'src_segment_pages') }}
-- union with historical data from segment from the datos-reporting bq project
UNION DISTINCT
Select `context_library_name`,
  `context_library_version`,
  `organization_slug`,
  `organization_name`,
  `url`,
  TIMESTAMP(`original_timestamp`) AS original_timestamp,
  `user_id`,
  `uuid_ts`,
  `customer_email`,
  CASE WHEN REGEXP_CONTAINS(`customer_email`, r'@y42') OR customer_email IS NULL THEN FALSE
  ELSE TRUE
  END AS is_customer,
  `name`,
  `id`,
  `customer_id`,
  CASE
  WHEN REGEXP_CONTAINS(`url`, r'/org/select') THEN 'Home Page'
  WHEN REGEXP_CONTAINS(`url`, r'/space/select') THEN 'Select Space'
  WHEN REGEXP_CONTAINS(`url`, r'/space/create') THEN 'Create Space'
  WHEN REGEXP_CONTAINS(`url`, r'/settings') THEN 'Settings'
  WHEN REGEXP_CONTAINS(`url`, r'/visualizations') OR REGEXP_CONTAINS(`url`, r'visualizations/') THEN 'Visualizations'
  WHEN REGEXP_CONTAINS(`url`, r'/status-alerts') THEN 'Status Alerts'
  WHEN REGEXP_CONTAINS(`url`, r'/exports') OR REGEXP_CONTAINS(`url`, r'exports/') THEN 'Exports'
  WHEN REGEXP_CONTAINS(`url`, r'/lineage') OR REGEXP_CONTAINS(`url`, r'lineage/') THEN 'Lineage'
  WHEN REGEXP_CONTAINS(`url`, r'/integrations') OR REGEXP_CONTAINS(`url`, r'integrations/') THEN 'Integrations'
  WHEN REGEXP_CONTAINS(`url`, r'/models/ui') OR REGEXP_CONTAINS(`url`, r'models/ui/') THEN 'UI Model'
  WHEN REGEXP_CONTAINS(`url`, r'/models/sql') OR REGEXP_CONTAINS(`url`, r'models/sql/') THEN 'SQL Model'
  ELSE 'Other/NULL'
  END AS product_area
                     from {{ source('cs_hackathon_braulio', 'src_segment_historical_pages') }}
