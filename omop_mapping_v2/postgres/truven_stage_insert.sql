
-- probably not necessary
/*
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
*/


-- locations are built from truven geoloc table.  use egeoloc for all the location_source_values
insert into omop.location
(
  state,
  location_source_value
)
select
  state_id as state,
  geoloc as location_source_value
from truven.geoloc
;

-- person
INSERT INTO etl.stage_person
(
  id,
  person_source_value,
  gender,
  year_of_birth,
  gender_source_value,
  location_source_value

)
select 
  ed.enrolid as person_source_value,
  case ed.sex 
    when '1' then 'M'
    when '2' then 'F'
    else null 
  end as gender,
  ed.dobyr as year_of_birth,
  ed.sex as gender_source_value,
  ed.egeoloc as location_source_value
from 
(
  select distinct
    enrolid,
    last_value( sex ) as sex,
    last_value( dobyr ) as dobyr,
    last_value( egeoloc ) as egeoloc
    from truven.enrollment_detail ed
    group by enrolid
     order by dtend  
)
;

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
  provider_source_value,
  specialty_source_value
)
select distinct
  provid as provider_source_value,
  stdprov as specialty_source_value
from truven.facility_header
where provid is not null
union
select distinct
  provid as provider_source_value,
  stdprov as specialty_source_value
from truven.inpatient_services
where provid is not null
union
select distinct
  provid as provider_source_value,
  stdprov as specialty_source_value
from truven.outpatient_services
where provid is not null
union
select distinct
  provid as provider_source_value,
  stdprov as specialty_source_value
from truven.lab
where provid is not null
;


-- from inpatient services
INSERT INTO etl.stage_visit
(

  visit_source_value,
  visit_type,
  visit_source_type_value,
  visit_start_date,
  visit_end_date,
  person_source_value,
  provider_source_value
)
with cte_claims as 
(
  select 
          svcdate as startdate,
          coalesce(tsvcdat, svcdate) as enddate,
          provid,
          caseid,
          case 
            when 
              df.STDPLAC = 23 
                OR (REVCODE >= '0450' AND df.REVCODE <= '0459')
                OR REVCODE = '0981'
                OR PROC1 IN ('99281','99282','99283','99284','99285')
              then 'ER'
            else 'IP'
          end as visit_type
        from truven.inpatient_services
)
  select distinct
    enrolid||'_'||caseid||'_'||provid||'_'||visit_type as visit_source_value,   -- ugghhhh  need to look at data...
    visit_type as visit_type,
    'CLAIM' as visit_source_type_value,
    startdate as visit_start_date,
    enddate as visit_end_date,
    enrolid as person_source_value,
    provid as provider_source_value
  from
  (
    select 
      enrolid,
      first_value(provid)   as provid,
      min(startdate) as startdate,
      max(enddate) as enddate,
      first_value(caseid) as caseid
    from
    (
      select 
        startdate, enddate, id, count(is_reset) over ( partition by id, visit_type order by startdate, enddate ) as grp
      from
      (  
        select 
          startdate as startdate,
          enddate as enddate,
          provid,
          caseid,
          case
            when lag(coalesce(tsvcdat, svcdate)) over ( partition by id, visit_type order by svcdate, tsvcdat ) < (svcdate -1 )
              then 1 
          end as is_reset,
          visit_type
        from cte_claims   
      ) b
    ) a
    group by id,visit_type, grp
    order by id, startdate, enddate
  ) c
;
  



-- from outpatient services
INSERT INTO etl.stage_visit
(

  visit_source_value,
  visit_type,
  visit_source_type_value,
  visit_start_date,
  visit_end_date,
  person_source_value,
  provider_source_value
)
with cte_claims as 
(
  select 
          svcdate as startdate,
          coalesce(tsvcdat, svcdate) as enddate,
          provid,
          caseid,
          case 
            when 
              df.STDPLAC = 23 
                OR (REVCODE >= '0450' AND df.REVCODE <= '0459')
                OR REVCODE = '0981'
                OR PROC1 IN ('99281','99282','99283','99284','99285')
              then 'ER'
            when
              (REVCODE >= '0100' AND REVCODE <='0219') --Room and Board Charges
              OR (REVCODE >= '0720' AND REVCODE <='0729')  --Labor Room and Delivery
              OR (REVCODE >= '0800' AND REVCODE <='0809') --Inpatient Renal Dialysis
            then 'IP'
            else 'OUT'
          end as visit_type
        from truven.outpatient_services
)
  select distinct
    enrolid||'_'||caseid||'_'||provid||'_'||visit_type as visit_source_value,   -- ugghhhh  need to look at data...
    visit_type as visit_type,
    'CLAIM' as visit_source_type_value,
    startdate as visit_start_date,
    enddate as visit_end_date,
    enrolid as person_source_value,
    provid as provider_source_value
  from
  (
    select 
      enrolid,
      provid as provid,
      min(startdate) as startdate,
      max(enddate) as enddate,
      first_value(caseid) as caseid
    from
    (
      select 
        startdate, enddate, id, count(is_reset) over ( partition by id, visit_type order by startdate, enddate ) as grp
      from
      (  
        select 
          startdate as startdate,
          enddate as enddate,
          provid,
          caseid,
          case
            when lag(coalesce(tsvcdat, svcdate)) over ( partition by id, visit_type order by svcdate, tsvcdat ) < (svcdate -1 )
              then 1 
          end as is_reset,
          visit_type
        from cte_claims  
      ) b
    ) a
    group by id,visit_type, grp
    order by id, startdate, enddate
  ) c
;
  




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
