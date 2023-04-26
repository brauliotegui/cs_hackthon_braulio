WITH customer_health AS (
SELECT t1.sf_acc_name, t1.organization_name
,(total_days_active/days_since_first_active)*100 AS active_days_pct
, fail_job_rate
, total_bugs_count
, next_journey_stage
, days_in_current_stage
FROM {{ ref('cs_hackathon_braulio', 'anl_pages_activity') }} t1
JOIN (SELECT sf_acc_name, 100*SUM(CASE WHEN import_status='invalid' THEN 1 ELSE 0 END)/COUNT(job_id) AS fail_job_rate
FROM {{ ref('cs_hackathon_braulio', 'mrt_jobs') }}
GROUP BY sf_acc_name) t2
ON t1.sf_acc_name=t2.sf_acc_name
LEFT JOIN (SELECT organization_name, COUNT(ticket_id) AS total_bugs_count
FROM {{ ref('cs_hackathon_braulio', 'stg_zendesk_tickets') }}
WHERE product_ticket_type='bugs'
GROUP BY organization_name) t3
ON t1.sf_acc_name=t3.organization_name
LEFT JOIN {{ ref('cs_hackathon_braulio', 'mrt_customer_journey') }} t4
ON t1.sf_acc_name=t4.sf_acc_name)
SELECT sf_acc_name, active_days_pct, fail_job_rate, total_bugs_count, next_journey_stage, days_in_current_stage,
CASE
WHEN active_days_pct > 67 THEN 2
ELSE 1
END AS active_days_score,
CASE
WHEN fail_job_rate < 16 THEN 2
ELSE 1
END AS fail_rate_score,
CASE
WHEN total_bugs_count < 7 THEN 2
ElSE 1
END AS bugs_count_score,
CASE
WHEN next_journey_stage != 'Kick-off' OR (next_journey_stage = 'Implementation' AND days_in_current_stage>100) THEN 1
ELSE 2
END AS stage_score
FROM customer_health
