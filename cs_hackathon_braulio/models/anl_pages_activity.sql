WITH table1 AS
(SELECT organization_name, sf_acc_name
, MIN(DATE(original_timestamp)) AS first_day_active
, COUNT(DISTINCT DATE(original_timestamp)) AS total_days_active
 FROM {{ref ('cs_hackathon_braulio', 'mrt_frontend_events')}}
 WHERE sf_acc_name IS NOT NULL
 GROUP BY organization_name, sf_acc_name)
 SELECT sf_acc_name, organization_name, DATE_DIFF(CURRENT_DATE(),first_day_active, DAY) AS days_since_first_active, total_days_active, first_day_active
 FROM table1
