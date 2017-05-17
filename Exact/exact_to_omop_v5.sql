I should not be running here;


-- patient locations
INSERT INTO omop_v5.location
(
	-- location_id,  -- auto increment encounter
	address_1,
	address_2,
	city,
	state,
	zip,
	location_source_value
)
select
	-- nextval(location_seq) as location_id,  -- auto increment
	address_1,
	address_2,
	city,
	state,
	zip,
	location_source_value
from
(
	select distinct
		address_line_1 as address_1,
        address_line_2 as address_2,
        city,
        state,
        zip_code as zip,
        substr( concat_ws( '_', zip_code, city, address_line_1, address_line_2, state ), 1, 50 ) as location_source_value
    from exact.enrollment  
) a;





INSERT INTO omop_v5.care_site
(
	-- care_site_id,  -- autogen
	care_site_name,
	care_site_source_value,
	place_of_service_source_value
)
select 
	-- care_site_id,
	primary_clinic_location as care_site_name,
	substr(primary_clinic_location, 1, 50 ) as care_site_source_value,
	'NULL' as place_of_service_source_value
from
(
	select distinct
		primary_clinic_location
	from exact.provider
) a
;

INSERT INTO omop_v5.provider
(
	-- provider_id,  -- autogen
	provider_name,
	NPI,
	-- DEA,
	specialty_concept_id,
	care_site_id,
	year_of_birth,
	gender_concept_id,
	provider_source_value,
	specialty_source_value,
	specialty_source_concept_id,
	gender_source_value,
	gender_source_concept_id
) 
select distinct
		pr.name as provider_name,
		pr.provider_npi as NPI,
		-- DEA,
		0 as specialty_concept_id,
		cs.care_site_id as care_site_id,
		year( str_to_date(pr.dob, '%Y-%m-%d')  ) as year_of_birth,
		case sex
				  when 'male' then 8507
				  when 'female' then 8532
				  when null then null
				  else 8551
		end as gender_concept_id,
		pr.provider_id as provider_source_value,
		pr.specialty as specialty_source_value,
		0 as specialty_source_concept_id,
		pr.sex as gender_source_value,
		0 as gender_source_concept_id
	from exact.provider pr
    left join omop_v5.care_site cs on substr(pr.primary_clinic_location, 1, 50 ) = cs.care_site_source_value
;




INSERT INTO omop_v5.person
(
	-- person_id,  autogen
	gender_concept_id,
	year_of_birth,
	month_of_birth,
	day_of_birth,
	race_concept_id,
	ethnicity_concept_id,
	location_id,
	person_source_value,
	gender_source_value,
	gender_source_concept_id,
	race_source_value,
	race_source_concept_id,
	ethnicity_source_value,
	ethnicity_source_concept_id
)
select distinct
	case gender
				  when 'M' then 8507
				  when 'F' then 8532
				  when 'A' then 8570
				  when 'U' then 8551
				  when 'O' then 8521
				  when null then null
				  else 8551
	end as gender_concept_id,
	year(DOB) as year_of_birth,
	month(DOB) as month_of_birth,
	day(DOB) as day_of_birth,
	0 as race_concept_id,
	0 as ethnicity_concept_id,
	location_id,
	person_source_value,
	gender_source_value,
	0 as gender_source_concept_id,
	race_source_value,
	0 as race_source_concept_id,
	ethnicity_source_value,
	0 as ethnicity_source_concept_id
from
(
	select 
		str_to_date(DOB, '%Y-%m-%d' ) as DOB,
		loc.location_id as location_id,
		pat.member_id as person_source_value,
        gender,
		coalesce( gender, 'NI' ) as gender_source_value,
		coalesce( race, 'NI' ) as race_source_value,
		coalesce( ethnicity, 'NI' ) as ethnicity_source_value
	from exact.enrollment pat
    left join omop_v5.location loc 
		on	substr( concat_ws( '_', pat.zip_code, pat.city, pat.address_line_1, pat.address_line_2, pat.state ), 1, 50 ) 
			= loc.location_source_value
) a
;

--  complete up to here.

INSERT INTO omop_v5.death
(
	person_id,
	death_date,
	death_type_concept_id,
	cause_concept_id,
	cause_source_value,
	cause_source_concept_id
)
select 
	per.person_id as person_id,
    date(str_to_date(DOD, '%Y-%m-%d %T' ) ) as death_date,
    '38003569' as death_type_concept_id,  -- EHR record patient status "Deceased"
    0 as cause_concept_id,
	'NULL' as cause_source_value,
	0 as cause_source_concept_id
from exact.death pat
join omop_v5.person per on pat.member_id = per.person_source_value
;



/*
visit:
'9201','Inpatient Visit','IP'
'9202','Outpatient Visit','OP'
'9203','Emergency Room Visit','ER'
'42898160','Long Term Care Visit','LTCP'

visit type:
'44818517','Visit derived from encounter on claim','OMOP generated'
'44818518','Visit derived from EHR record','OMOP generated'
'44818519','Clinical Study visit','OMOP generated'
*/


INSERT INTO omop_v5.visit_occurrence
(
	-- visit_occurrence_id,   autogen
	person_id,
	visit_concept_id,
	visit_start_date,
	visit_start_time,
	visit_end_date,
	-- visit_end_time,
	visit_type_concept_id,
	provider_id,
    care_site_id,
	visit_source_value,
	visit_source_concept_id
)
select distinct
	per.person_id as person_id,
	case  clinic_type
		when 'outpatient_medical' then 9202
		when 'pharmacy'  then 9202
		when 'lab'  then 9202
		when 'outpatient_emergency' then 9203
		when 'inpatient_medical' then 9201
		else 0
	end
    as visit_concept_id,
	date(str_to_date(encounter_datetime, '%Y-%m-%d %T' ))  as visit_start_date,
	time(str_to_date(encounter_datetime, '%Y-%m-%d %T' ))  as visit_start_time,
	date(str_to_date(coalesce(adm.checkout_datetime, vis.encounter_datetime), '%Y-%m-%d %T' ))  as visit_end_date,
	-- vis.visit_stop_time as visit_end_time,
	44818518 as visit_type_concept_id,
	prov.provider_id as provider_id,
    care.care_site_id as care_site_id,
	vis.encounter_id as visit_source_value,
	0 as visit_source_concept_id
from exact.encounter vis
join omop_v5.person per on vis.member_id = per.person_source_value
left join exact.admission adm on vis.encounter_id = adm.encounter_id
left join omop_v5.provider prov on vis.provider_id = prov.provider_source_value
left join  omop_v5.care_site care on vis.clinic_id = care.care_site_source_value
;


/*

INSERT INTO omop_v5.visit_occurrence
(
		-- visit_occurrence_id,   autogen
	person_id,
	visit_concept_id,
	visit_start_date,
	visit_start_time,
	visit_end_date,
	-- visit_end_time,
	visit_type_concept_id,
	provider_id,
    care_site_id,
	visit_source_value,
	visit_source_concept_id
)
select distinct
	per.person_id as person_id,
	case  clinic_type
		when 'outpatient_medical' then 9202
		when 'pharmacy'  then 9202
		when 'lab'  then 9202
		when 'outpatient_emergency' then 9202
		when 'inpatient_medical' then 9201
		else 0
	end
    as visit_concept_id,
	date(str_to_date(encounter_datetime, '%Y-%m-%d %T' ))  as visit_start_date,
	time(str_to_date(encounter_datetime, '%Y-%m-%d %T' ))  as visit_start_time,
	date(str_to_date(coalesce(adm.checkout_datetime, vis.encounter_datetime), '%Y-%m-%d %T' ))  as visit_end_date,
	-- vis.visit_stop_time as visit_end_time,
	44818518 as visit_type_concept_id,
	prov.provider_id as provider_id,
    care.care_site_id as care_site_id,
	vis.encounter_id as visit_source_value,
	0 as visit_source_concept_id
from exact.encounter_outpatient vis
join omop_v5.person per on vis.member_id = per.person_source_value
left join exact.admission adm on vis.encounter_id = adm.encounter_id
left join omop_v5.provider prov on vis.provider_id = prov.provider_source_value
left join  omop_v5.care_site care on vis.clinic_id = care.care_site_source_value
;
*/

-- INSERT INTO omop_v5.visit_cost


INSERT INTO omop_v5.condition_occurrence
(
	-- condition_occurrence_id,   -- autogen
	person_id,
	condition_concept_id,
	condition_start_date,
	condition_type_concept_id,
	provider_id,
	visit_occurrence_id,
	condition_source_value,
	condition_source_concept_id
)
select
	-- condition_occurrence_id.nextval as condition_occurrence_id,
	vis.person_id,
	coalesce(tar.concept_id, 0 ) as condition_concept_id,
	vis.visit_start_date as condition_start_date,
	44786627 as condition_type_concept_id,   -- primary condition
	vis.provider_id as provider_id,
	vis.visit_occurrence_id as visit_occurrence_id,
	edx.code as condition_source_value,
	coalesce( src.concept_id, 0 ) as condition_source_concept_id
FROM exact.encounter_dx edx
	join omop_v5.visit_occurrence vis on edx.encounter_id = vis.visit_source_value
	left join omop_v5.concept src on edx.code = src.concept_code and src.vocabulary_id = 'ICD9CM' 
	left join omop_v5.concept_relationship cr on src.concept_id = cr.concept_id_1 and cr.relationship_id = 'Maps to' 
	left join omop_v5.concept tar on cr.concept_id_2 = tar.concept_id and tar.standard_concept = 'S' 
    
;


--  PROBLEM TABLE for Conditions!!!
-- 38000245  -- problem list
INSERT INTO omop_v5.condition_occurrence
(
	-- condition_occurrence_id,   -- autogen
	person_id,
	condition_concept_id,
	condition_start_date,
	condition_type_concept_id,
-- 	provider_id,
-- 	visit_occurrence_id,
	condition_source_value,
	condition_source_concept_id
)
select
	-- condition_occurrence_id.nextval as condition_occurrence_id,
	per.person_id,
	coalesce(tar.concept_id, 0 ) as condition_concept_id,
	str_to_date( prb.onset_date, '%Y-%m-%d %T' ) as condition_start_date,
	38000245 as condition_type_concept_id,   -- problem list
	prb.problem_code as condition_source_value,
	coalesce( src.concept_id, 0 ) as condition_source_concept_id
FROM exact.problem prb
	join omop_v5.person per on prb.member_id = per.person_source_value
	left join omop_v5.concept src on prb.problem_code = src.concept_code and src.vocabulary_id = 'ICD9CM' 
	left join omop_v5.concept_relationship cr on src.concept_id = cr.concept_id_1 and cr.relationship_id = 'Maps to' 
	left join omop_v5.concept tar on cr.concept_id_2 = tar.concept_id and tar.standard_concept = 'S' 
    
;


-- therapy

INSERT INTO omop_v5.procedure_occurrence
(
	-- procedure_occurrence_id,  autogen
	person_id,
	procedure_concept_id,
	procedure_date,
	procedure_type_concept_id,
	-- 	modifier_concept_id,
	-- 	quantity,
	provider_id,
	visit_occurrence_id,
	procedure_source_value,
	procedure_source_concept_id
	-- qualifier_source_value
)
select distinct
	vis.person_id as person_id,
	coalesce( tar.concept_id, 0 )  as procedure_concept_id,
	vis.visit_start_date  as procedure_date,
	38000275 as procedure_type_concept_id,
	vis.provider_id as provider_id,
	vis.visit_occurrence_id as visit_occurrence_id,
	pro.code as procedure_source_value,
	src.concept_id as procedure_source_concept_id		
from exact.therapy_actions pro
join omop_v5.visit_occurrence vis on pro.encounter_id = vis.visit_source_value
left join omop_v5.concept src on pro.code = src.concept_code and src.vocabulary_id = 'ICD9Proc'
left join omop_v5.concept_relationship cr on src.concept_id = cr.concept_id_1 and cr.relationship_id = 'Maps to' 
left join omop_v5.concept tar on cr.concept_id_2 = tar.concept_id and tar.standard_concept = 'S' 
;


-- done to here

-- immunizations

INSERT INTO omop_v5.procedure_occurrence
(
	-- procedure_occurrence_id,  autogen
	person_id,
	procedure_concept_id,
	procedure_date,
	procedure_type_concept_id,
	-- 	modifier_concept_id,
	-- 	quantity,
	provider_id,
	visit_occurrence_id,
	procedure_source_value,
	procedure_source_concept_id
	-- qualifier_source_value
)
select distinct
	per.person_id as person_id,
	coalesce( tar.concept_id, 0 )  as procedure_concept_id,
	date(str_to_date(imm.vaccination_date, '%Y-%m-%d %T' ))  as procedure_date,
	38000275 as procedure_type_concept_id,
	pro.provider_id as provider_id,
	vis.visit_occurrence_id as visit_occurrence_id,
	imm.vaccine_cvx as procedure_source_value,
	45754855 as procedure_source_concept_id   -- vaccine concept class
from exact.immunization   imm
	join omop_v5.person per on imm.member_id = per.person_source_value
	left join exact.cvx_to_cpt cvx on imm.vaccine_cvx = cvx.cvx_code 
	left join omop_v5.visit_occurrence vis on imm.encounter_id = vis.visit_source_value
    left join omop_v5.provider pro on imm.provider_id = pro.provider_source_value
	left join omop_v5.concept src on cvx.cpt_code = src.concept_code
	left join omop_v5.concept_relationship cr on src.concept_id = cr.concept_id_1 and cr.relationship_id = 'Maps to' 
	left join omop_v5.concept tar on cr.concept_id_2 = tar.concept_id and tar.standard_concept = 'S' 
;

-- lab orders

INSERT INTO omop_v5.measurement
(
	-- measurement_id,  autogen
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_time,
	measurement_type_concept_id,
	-- operator_concept_id,
	value_as_number,
	value_as_concept_id,
	unit_concept_id,
-- 	range_low,
-- 	range_high,
	provider_id,
	visit_occurrence_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
select 
	-- measurement_id,
	vis.person_id,
	coalesce(tar.concept_id, 0 ) as measurement_concept_id,
	date(str_to_date( lr.date_collected, '%Y-%m-%d %T' )) as measurement_date,
	time(str_to_date( lr.date_collected, '%Y-%m-%d %T' )) as measurement_time,
	0 as measurement_type_concept_id,
	-- operator_concept_id,
	0 + lr.numeric_result as value_as_number,
	0 as value_as_concept_id,
	coalesce( cu.concept_id, 0 ) as unit_concept_id,
	pro.provider_id,
	vis.visit_occurrence_id,
	lr.result_loinc as measurement_source_value,
	44819102 as measurement_source_concept_id,   -- LOINC
	lr.units as unit_source_value,
	substr(lr.result_description, 1, 50 ) as value_source_value
from exact.lab_results lr
	left join omop_v5.concept src on lr.result_loinc = src.concept_code and src.vocabulary_id = 'LOINC' 
	left join omop_v5.concept_relationship cr on src.concept_id = cr.concept_id_1 and cr.relationship_id = 'Maps to' 
	left join omop_v5.concept tar on cr.concept_id_2 = tar.concept_id and tar.standard_concept = 'S' 
left join omop_v5.visit_occurrence vis on lr.encounter_id = vis.visit_source_value
left join omop_v5.provider pro on lr.provider_id = pro.provider_source_value
left join omop_v5.concept cu on  lr.units = cu.concept_code and cu.standard_concept = 'S' and cu.domain_id = 'Unit'

;

-- hack to get around mysql float conversion issues 
update omop_v5.measurement
set value_as_number = null
where value_as_number = 0;



-- vitals    
-- 44818701  from physical examination

-- 3036277  height      -- LOINC		-- 8302-2
-- 3013762	weight      -- LOINC		-- 3141-9  body weight measured
-- 3004249	systolic    -- LOINC		-- 8480-6
-- 3012888	diastolic   -- LOINC		-- 8462-4
-- 3027018	pulse       -- LOINC		-- 8867-4
-- 3024171	respiration -- LOINC		-- 9279-1
-- 3020891	temperature -- LOINC		-- 8310-5

INSERT INTO omop_v5.measurement
(
	-- measurement_id,  autogen
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_time,
	measurement_type_concept_id,
	-- operator_concept_id,
	value_as_number,
	value_as_concept_id,
	unit_concept_id,
-- 	range_low,
-- 	range_high,
	provider_id,
	visit_occurrence_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
select 
	-- measurement_id,
	vis.person_id,
	vs.measurement_concept_id as measurement_concept_id,
	date(str_to_date( vs.encounter_date, '%Y-%m-%d %T' )) as measurement_date,
	time(str_to_date( vs.encounter_date, '%Y-%m-%d %T' )) as measurement_time,
	44818701 as measurement_type_concept_id,   -- From physical exam
	-- operator_concept_id,
	vs.value_as_number as value_as_number,
	vs.value_as_concept_id as value_as_concept_id,
	coalesce( vs.unit_concept_id, 0 ) as unit_concept_id,
	vis.provider_id,
	vis.visit_occurrence_id,
	vs.org_source as measurement_source_value,
	44819102 as measurement_source_concept_id,   -- LOINC
	vs.units as unit_source_value,
	vs.org_value as value_source_value

from
(

	select
		member_id,
		3036277 as measurement_concept_id,  -- height
		encounter_date,
		case height_units
			when 'cm' 
				then round(height *0.393701)
			else height
		end as value_as_number,
		null as value_as_concept_id,
        8533 as unit_concept_id,   -- inches
		encounter_id,
		'Height' as org_source,
		height_units as units,
		height as org_value
    from exact.vital_sign
    union
	select
		member_id,
		3013762 as measurement_concept_id,   -- weight
		encounter_date,
		case weight_units
			when 'kg' 
				then round(weight * 2.20462 )
			else weight
		end as value_as_number,
		null as value_as_concept_id,
        8739 as unit_concept_id,   -- pound
		encounter_id,
		'Weight' as org_source,
		weight_units as units,
		weight as org_value
    from exact.vital_sign   
	union
	select
		member_id,
		3004249 as measurement_concept_id,   -- systolic
		encounter_date,
		systolicbp as value_as_number,
		null as value_as_concept_id,
        8876 as unit_concept_id,   -- mm hg
		encounter_id,
		'SystolicBP' as org_source,
		'mmHg' as units,
		concat( systolicbp, '/', diastolicbp ) as org_value
    from exact.vital_sign 
	union
	select
		member_id,
		3012888 as measurement_concept_id,   -- diastolic
		encounter_date,
		diastolicbp as value_as_number,
		null as value_as_concept_id,
        8876 as unit_concept_id,   -- mm hg
		encounter_id,
		'DiastolicBP' as org_source,
		'mmHg' as units,
		concat( systolicbp, '/', diastolicbp ) as org_value
    from exact.vital_sign
    	union
	select
		member_id,
		3027018 as measurement_concept_id,   -- pulse
		encounter_date,
		pulse as value_as_number,
		null as value_as_concept_id,
        8541 as unit_concept_id,   -- /Min
		encounter_id,
		'Pulse' as org_source,
		'/min' as units,
		pulse as org_value
    from exact.vital_sign
	union
	select
		member_id,
		3024171 as measurement_concept_id,   -- respiration
		encounter_date,
		respiration as value_as_number,
		null as value_as_concept_id,
        8541 as unit_concept_id,   -- mm hg
		encounter_id,
		'Respiration' as org_source,
		'/min' as units,
		diastolicbp as org_value
    from exact.vital_sign
	union
	select
		member_id,
		3020891 as measurement_concept_id,   -- temperature
		encounter_date,
		case temperature_units
			when 'C' 
				then round( temperature * (9/5) + 32 )
			else temperature
		end as value_as_number,
		null as value_as_concept_id,
        9289 as unit_concept_id,   -- degF
		encounter_id,
		'Temperature' as org_source,
		temperature_units as units,
		temperature as org_value
    from exact.vital_sign  
)  vs
left join omop_v5.visit_occurrence vis on vs.encounter_id = vis.visit_source_value
;




-- 3020891	temperature -- LOINC		-- 8310-5
-- allergies



-- medication

INSERT INTO omop_v5.drug_exposure
(
	-- drug_exposure_id,
	person_id,
	drug_concept_id,
	drug_exposure_start_date,
	drug_exposure_end_date,
	drug_type_concept_id,
	stop_reason,
	refills,
	quantity,
	days_supply,
	sig,
	route_concept_id,
	effective_drug_dose,
	dose_unit_concept_id,
	lot_number,
	provider_id,
	visit_occurrence_id,
	drug_source_value,
	drug_source_concept_id,
	route_source_value,
	dose_unit_source_value
)
select 
	-- drug_exposure_id,
	per.person_id as person_id,
	coalesce(c2.concept_id, c1.concept_id, 0 ) as drug_concept_id,
	date(str_to_date(mh.dispense_date, '%Y-%m-%d %T' ))  as drug_exposure_start_date,
	null as drug_exposure_end_date,
	38000175 as drug_type_concept_id,  -- prescription dispensed in pharmacy
	null as stop_reason,
	mo.refills as refills,
	mh.dispense_qty as quantity,
	mh.days_of_supply as days_supply,
	mh.sig as sig,
	null as route_concept_id,
	null as effective_drug_dose,
	coalesce( c3.concept_id, 0 ) as dose_unit_concept_id,
	null as lot_number,
	coalesce( pr.provider_id, 0 )  as provider_id,
	coalesce( vo.visit_occurrence_id, 0 ) as visit_occurrence_id,
	mh.drug_ndc as drug_source_value,
	44819105 as drug_source_concept_id,   -- NDC
	mo.route as route_source_value,
	mh.units as dose_unit_source_value
 
from exact.medication_fulfillment mh
join omop_v5.person per on mh.member_id = per.person_source_value
left join omop_v5.concept c1 on mh.ndc_norm = c1.concept_code and c1.vocabulary_id = 'NDC' 
left join omop_v5.concept_relationship cr on c1.concept_id = cr.concept_id_1 and cr.relationship_id = 'Maps to'
left join omop_v5.concept c2 on cr.concept_id_2 = c2.concept_id and c2.vocabulary_id = 'RxNorm'
left join omop_v5.concept c3 on mh.Units = c3.concept_code and c3.domain_id = 'Unit'
left join exact.medication_orders mo on mh.order_id = mo.order_id
left join omop_v5.provider pr on mo.order_provider_id = pr.provider_source_value
left join omop_v5.visit_occurrence vo on mh.encounter_id = vo.visit_source_value
;



