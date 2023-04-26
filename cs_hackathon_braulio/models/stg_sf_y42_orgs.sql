Select
ID AS sf_y42orgv2_id,
NAME AS slug,
ACCOUNT__C AS sf_account_id
from {{ source('cs_hackathon_source_data_salesforce', 'SF_Y42_ORGS_V2') }}
WHERE isdeleted IS FALSE
