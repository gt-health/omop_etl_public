

-- care site  
INSERT INTO etl.stage_care_site
(
  id,
  care_site_name,
  address_1,
  address_2,
  city,
  state,
  zip,
  county,
  location_source_value,
  care_site_source_value,
  place_of_service_source_value,
  load_id,
  loaded
)

-- person
INSERT INTO etl.stage_person
(
  id,
  person_source_value,
  gender,
  year_of_birth,
  month_of_birth,
  day_of_birth,
  time_of_birth,
  race,
  address_1,
  address_2,
  city,
  state,
  zip,
  county,
  ethnicity,
  ethnicity_source_value,
  gender_source_value,
  race_source_value,
  provider_source_value,
  care_site_source_value,
  location_source_value,
  load_id,
  loaded
)


-- death
INSERT INTO etl.stage_death
(
  id,
  person_source_value,
  dod,
  death_type_source_value,
  cause_source_value,
  load_id,
  loaded
)


-- provider
INSERT INTO etl.stage_provider
(
  id,
  provider_name,
  npi,
  dea,
  specialty_source_value,
  care_site_source_value,
  location_source_value,
  gender,
  year_of_birth,
  address_1,
  address_2,
  city,
  state,
  zip,
  county,
  gender_source_value,
  provider_source_value,
  load_id,
  loaded
)

INSERT INTO etl.stage_visit
(
  id,
  visit_source_value,
  visit_type,
  visit_source_type_value,
  visit_start_date,
  visit_end_date,
  total_charge,
  total_cost,
  total_paid,
  paid_by_payer,
  paid_by_patient,
  paid_patient_copay,
  paid_patient_coinsurance,
  paid_patient_deductible,
  paid_by_primary,
  person_source_value,
  provider_source_value,
  care_site_source_value,
  load_id,
  loaded
)

-- stage_condition
INSERT INTO etl.stage_condition_temp
(
  id,
  condition_code_source_type,
  condition_source_value,
  condition_source_type_value,
  start_date,
  end_date,
  stop_reason,
  visit_source_value,
  person_source_value,
  provider_source_value,
  load_id,
  loaded
)


--procedure
INSERT INTO etl.stage_procedure_temp
(
  id,
  procedure_code_source_type,
  procedure_source_value,
  procedure_source_type_value,
  code_modifier,
  procedure_date,
  quantity,
  stop_reason,
  total_charge,
  total_cost,
  total_paid,
  paid_by_payer,
  paid_by_patient,
  paid_patient_copay,
  paid_patient_coinsurance,
  paid_patient_deductible,
  paid_by_primary,
  visit_source_value,
  person_source_value,
  provider_source_value,
  load_id,
  loaded
)

-- stage_lab_temp
INSERT INTO etl.stage_lab_temp
(
  id,
  measurement_source_type,
  measurement_source_value,
  measurement_source_type_value,
  measurement_date,
  operator_source_value,
  unit_source_value,
  value_source_value,
  value_as_number,
  value_as_string,
  range_low,
  range_high,
  visit_source_value,
  person_source_value,
  provider_source_value,
  load_id,
  loaded
)


-- rx
INSERT INTO etl.stage_rx_temp
(
  id,
  drug_source_type,
  drug_source_value,
  drug_source_type_value,
  drug_start_date,
  drug_end_date,
  stop_reason,
  refills,
  quantity,
  days_supply,
  dose_unit_source_value,
  effective_drug_dose,
  total_charge,
  total_cost,
  total_paid,
  paid_by_payer,
  paid_by_patient,
  paid_patient_copay,
  paid_patient_coinsurance,
  paid_patient_deductible,
  paid_by_primary,
  paid_ingredient_cost,
  pait_dispensing_fee,
  route_source_value,
  visit_source_value,
  person_source_value,
  provider_source_value,
  load_id,
  loaded
)
