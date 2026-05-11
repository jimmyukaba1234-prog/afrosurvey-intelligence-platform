
- fact_survey_responses
response_id
country
submission_time
completion_status   -- completed / partial
age
gender
region
source             -- api / csv / field_agent
agent_id
is_duplicate       -- true/false
is_valid           -- passed validation
missing_fields_count


- dim_country
country_code
country_name
region_group

- dim_agent
agent_id
agent_name
source_type
country

- pipeline_metrics (ENGINEERING TABLE)
run_id
stage              -- ingestion / cleaning / gold
country
rows_in
rows_out
duplicates_removed
processing_time_sec
status
created_at
