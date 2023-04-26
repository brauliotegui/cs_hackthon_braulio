SELECT job_id
, SPLIT(path, '/')[OFFSET(1)] AS product
, IF(SPLIT(path, '/')[OFFSET(1)]='Models',SPLIT(path, '/')[OFFSET(2)],null) AS model_type
, `company_id`
, space_id
, path
, `job_status`
, import_status
, `submitted_at`
, `finished_at`
, triggered_by_type
, triggered_by_id
, JSON_QUERY(error, '$.code') AS error_code
, JSON_EXTRACT_SCALAR(request, '$.context.company_slug') AS company_slug
, SPLIT(JSON_QUERY(request, '$.source_settings'), '/')[OFFSET(4)] AS source_type
, `parent_job_id`
, `tests_status`
, `data_contracts_status`
from {{ source('vdemo_public_cshackathoneumultiregion_main', 'Integrations_Y42_Analytics_Integrations_src_job_info_service_job_info_service_public_job_1') }}
