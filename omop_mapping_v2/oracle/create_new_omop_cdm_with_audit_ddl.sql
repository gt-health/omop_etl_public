/*
   Copyright 2016  Georgia Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. 
*/
/*********************************************************************************
# Copyright 2014-6 Observational Health Data Sciences and Informatics
#
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

/************************

 ####### #     # ####### ######      #####  ######  #     #           ####### 
 #     # ##   ## #     # #     #    #     # #     # ##   ##    #    # #       
 #     # # # # # #     # #     #    #       #     # # # # #    #    # #       
 #     # #  #  # #     # ######     #       #     # #  #  #    #    # ######  
 #     # #     # #     # #          #       #     # #     #    #    #       # 
 #     # #     # #     # #          #     # #     # #     #     #  #  #     # 
 ####### #     # ####### #           #####  ######  #     #      ##    #####  
                                                                              

script to create OMOP common data model, version 5.0 for PostgreSQL database

last revised: 1-May-2016

Authors:  Patrick Ryan, Christian Reich


*************************/


/************************

Standardized vocabulary

************************/


CREATE TABLE omop.concept (
  concept_id			number			NOT NULL,
  concept_name			varchar2(255)	NOT NULL,
  domain_id				varchar2(20)		NOT NULL,
  vocabulary_id			varchar2(20)		NOT NULL,
  concept_class_id		varchar2(20)		NOT NULL,
  standard_concept		varchar2(1)		NULL,
  concept_code			varchar2(50)		NOT NULL,
  valid_start_date		DATE			NOT NULL,
  valid_end_date		DATE			NOT NULL,
  invalid_reason		varchar2(1)		NULL
)
;




CREATE TABLE omop.vocabulary (
  vocabulary_id			varchar2(20)		NOT NULL,
  vocabulary_name		varchar2(255)	NOT NULL,
  vocabulary_reference	varchar2(255)	NULL,
  vocabulary_version	varchar2(255)	NULL,
  vocabulary_concept_id	number			NOT NULL
)
;




CREATE TABLE omop.domain (
  domain_id			varchar2(20)		NOT NULL,
  domain_name		varchar2(255)	NOT NULL,
  domain_concept_id	number			NOT NULL
)
;



CREATE TABLE omop.concept_class (
  concept_class_id			varchar2(20)		NOT NULL,
  concept_class_name		varchar2(255)	NOT NULL,
  concept_class_concept_id	number			NOT NULL
)
;




CREATE TABLE omop.concept_relationship (
  concept_id_1			number			NOT NULL,
  concept_id_2			number			NOT NULL,
  relationship_id		varchar2(20)		NOT NULL,
  valid_start_date		DATE			NOT NULL,
  valid_end_date		DATE			NOT NULL,
  invalid_reason		varchar2(1)		NULL)
;



CREATE TABLE omop.relationship (
  relationship_id			varchar2(20)		NOT NULL,
  relationship_name			varchar2(255)	NOT NULL,
  is_hierarchical			varchar2(1)		NOT NULL,
  defines_ancestry			varchar2(1)		NOT NULL,
  reverse_relationship_id	varchar2(20)		NOT NULL,
  relationship_concept_id	number			NOT NULL
)
;


CREATE TABLE omop.concept_synonym (
  concept_id			number			NOT NULL,
  concept_synonym_name	varchar2(1000)	NOT NULL,
  language_concept_id	number			NOT NULL
)
;


CREATE TABLE omop.concept_ancestor (
  ancestor_concept_id		number		NOT NULL,
  descendant_concept_id		number		NOT NULL,
  min_levels_of_separation	number		NOT NULL,
  max_levels_of_separation	number		NOT NULL
)
;



CREATE TABLE omop.source_to_concept_map (
  source_code				varchar2(50)		NOT NULL,
  source_concept_id			number			NOT NULL,
  source_vocabulary_id		varchar2(20)		NOT NULL,
  source_code_description	varchar2(255)	NULL,
  target_concept_id			number			NOT NULL,
  target_vocabulary_id		varchar2(20)		NOT NULL,
  valid_start_date			DATE			NOT NULL,
  valid_end_date			DATE			NOT NULL,
  invalid_reason			varchar2(1)		NULL
)
;




CREATE TABLE omop.drug_strength (
  drug_concept_id				number		NOT NULL,
  ingredient_concept_id			number		NOT NULL,
  amount_value					number		NULL,
  amount_unit_concept_id		number		NULL,
  numerator_value				number		NULL,
  numerator_unit_concept_id		number		NULL,
  denominator_value				number		NULL,
  denominator_unit_concept_id	number		NULL,
  box_size			number 	null,
  valid_start_date				DATE		NOT NULL,
  valid_end_date				DATE		NOT NULL,
  invalid_reason				varchar2(1)	NULL
)
;



CREATE TABLE omop.cohort_definition (
  cohort_definition_id				number			NOT NULL,
  cohort_definition_name			varchar2(255)	NOT NULL,
  cohort_definition_description		clob	NULL,
  definition_type_concept_id		number			NOT NULL,
  cohort_definition_syntax			clob	NULL,
  subject_concept_id				number			NOT NULL,
  cohort_initiation_date			DATE			NULL
)
;


CREATE TABLE omop.attribute_definition (
  attribute_definition_id		number			NOT NULL,
  attribute_name				varchar2(255)	NOT NULL,
  attribute_description			clob	NULL,
  attribute_type_concept_id		number			NOT NULL,
  attribute_syntax				clob	NULL
)
;


/**************************

Standardized meta-data

***************************/


CREATE TABLE omop.cdm_source 
    (  
     cdm_source_name					varchar2(255)	NOT NULL,
	 cdm_source_abbreviation			varchar2(25)		NULL,
	 cdm_holder							varchar2(255)	NULL,
	 source_description					clob	NULL,
	 source_documentation_reference		varchar2(255)	NULL,
	 cdm_etl_reference					varchar2(255)	NULL,
	 source_release_date				DATE			NULL,
	 cdm_release_date					DATE			NULL,
	 cdm_version						varchar2(10)		NULL,
	 vocabulary_version					varchar2(20)		NULL
    ) 
;



/************************

Standardized clinical data

************************/


CREATE TABLE omop.person 
    (
     person_id						number		NOT NULL , 
     gender_concept_id				number		NOT NULL , 
     year_of_birth					number		NOT NULL , 
     month_of_birth					number		NULL, 
     day_of_birth					number		NULL, 
	 time_of_birth					varchar2(10)	NULL,
     race_concept_id				number		NOT NULL, 
     ethnicity_concept_id			number		NOT NULL, 
     location_id					number		NULL, 
     provider_id					number		NULL, 
     care_site_id					number		NULL, 
     person_source_value			varchar2(50) NULL, 
     gender_source_value			varchar2(50) NULL,
	 gender_source_concept_id		number		NULL, 
     race_source_value				varchar2(50) NULL, 
	 race_source_concept_id			number		NULL, 
     ethnicity_source_value			varchar2(50) NULL,
	 ethnicity_source_concept_id	number		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate	date default sysdate,
     x_updatedate       date  default sysdate
     
    ) 
;





CREATE TABLE omop.observation_period 
    ( 
     observation_period_id				number		NOT NULL , 
     person_id							number		NOT NULL , 
     observation_period_start_date		DATE		NOT NULL , 
     observation_period_end_date		DATE		NOT NULL ,
	 period_type_concept_id				number		NOT NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.specimen
    ( 
     specimen_id						number			NOT NULL ,
	 person_id							number			NOT NULL ,
	 specimen_concept_id				number			NOT NULL ,
	 specimen_type_concept_id			number			NOT NULL ,
	 specimen_date						DATE			NOT NULL ,
	 specimen_time						varchar2(10)		NULL ,
	 quantity							number			NULL ,
	 unit_concept_id					number			NULL ,
	 anatomic_site_concept_id			number			NULL ,
	 disease_status_concept_id			number			NULL ,
	 specimen_source_id					varchar2(50)		NULL ,
	 specimen_source_value				varchar2(50)		NULL ,
	 unit_source_value					varchar2(50)		NULL ,
	 anatomic_site_source_value			varchar2(50)		NULL ,
	 disease_status_source_value		varchar2(50)		NULL,
     x_srcid		number,   
     x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

	)
;



CREATE TABLE omop.death 
    ( 
     person_id							number			NOT NULL , 
     death_date							DATE			NOT NULL , 
     death_type_concept_id				number			NOT NULL , 
     cause_concept_id					number			NULL , 
     cause_source_value					varchar2(50)		NULL,
	 cause_source_concept_id			number			NULL,
     x_srcid		number,   
	x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.visit_occurrence 
    ( 
     visit_occurrence_id			number			NOT NULL , 
     person_id						number			NOT NULL , 
     visit_concept_id				number			NOT NULL , 
	 visit_start_date				DATE			NOT NULL , 
	 visit_start_time				varchar2(10)		NULL ,
     visit_end_date					DATE			NOT NULL ,
	 visit_end_time					varchar2(10)		NULL , 
	 visit_type_concept_id			number			NOT NULL ,
	 provider_id					number			NULL,
     care_site_id					number			NULL, 
     visit_source_value				varchar2(50)		NULL,
	 visit_source_concept_id		number			NULL,
     x_srcid		number,   
		x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.procedure_occurrence 
    ( 
     procedure_occurrence_id		number			NOT NULL , 
     person_id						number			NOT NULL , 
     procedure_concept_id			number			NOT NULL , 
     procedure_date					DATE			NOT NULL , 
     procedure_type_concept_id		number			NOT NULL ,
	 modifier_concept_id			number			NULL ,
	 quantity						number			NULL , 
     provider_id					number			NULL , 
     visit_occurrence_id			number			NULL , 
     procedure_source_value			varchar2(50)		NULL ,
	 procedure_source_concept_id	number			NULL ,
	 qualifier_source_value			varchar2(50)		NULL,
     x_srcid		number,   
     x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.drug_exposure 
    ( 
     drug_exposure_id				number			NOT NULL , 
     person_id						number			NOT NULL , 
     drug_concept_id				number			NOT NULL , 
     drug_exposure_start_date		DATE			NOT NULL , 
     drug_exposure_end_date			DATE			NULL , 
     drug_type_concept_id			number			NOT NULL , 
     stop_reason					varchar2(20)		NULL , 
     refills						number			NULL , 
     quantity						number			NULL , 
     days_supply					number			NULL , 
     sig							clob	NULL , 
	 route_concept_id				number			NULL ,
	 effective_drug_dose			number			NULL ,
	 dose_unit_concept_id			number			NULL ,
	 lot_number						varchar2(50)		NULL ,
     provider_id					number			NULL , 
     visit_occurrence_id			number			NULL , 
     drug_source_value				varchar2(50)		NULL ,
	 drug_source_concept_id			number			NULL ,
	 route_source_value				varchar2(50)		NULL ,
	 dose_unit_source_value			varchar2(50)		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;


CREATE TABLE omop.device_exposure 
    ( 
     device_exposure_id				number			NOT NULL , 
     person_id						number			NOT NULL , 
     device_concept_id				number			NOT NULL , 
     device_exposure_start_date		DATE			NOT NULL , 
     device_exposure_end_date		DATE			NULL , 
     device_type_concept_id			number			NOT NULL , 
	 unique_device_id				varchar2(50)		NULL ,
	 quantity						number			NULL ,
     provider_id					number			NULL , 
     visit_occurrence_id			number			NULL , 
     device_source_value			varchar2(100)	NULL ,
	 device_source_concept_id		number			NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;


CREATE TABLE omop.condition_occurrence 
    ( 
     condition_occurrence_id		number			NOT NULL , 
     person_id						number			NOT NULL , 
     condition_concept_id			number			NOT NULL , 
     condition_start_date			DATE			NOT NULL , 
     condition_end_date				DATE			NULL , 
     condition_type_concept_id		number			NOT NULL , 
     stop_reason					varchar2(20)		NULL , 
     provider_id					number			NULL , 
     visit_occurrence_id			number			NULL , 
     condition_source_value			varchar2(50)		NULL ,
	 condition_source_concept_id	number			NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.measurement 
    ( 
     measurement_id					number			NOT NULL , 
     person_id						number			NOT NULL , 
     measurement_concept_id			number			NOT NULL , 
     measurement_date				DATE			NOT NULL , 
     measurement_time				varchar2(10)		NULL ,
	 measurement_type_concept_id	number			NOT NULL ,
	 operator_concept_id			number			NULL , 
     value_as_number				number			NULL , 
     value_as_concept_id			number			NULL , 
     unit_concept_id				number			NULL , 
     range_low						number			NULL , 
     range_high						number			NULL , 
     provider_id					number			NULL , 
     visit_occurrence_id			number			NULL ,  
     measurement_source_value		varchar2(50)		NULL , 
	 measurement_source_concept_id	number			NULL ,
     unit_source_value				varchar2(50)		NULL ,
	 value_source_value				varchar2(50)		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.note 
    ( 
     note_id						number			NOT NULL , 
     person_id						number			NOT NULL , 
     note_date						DATE			NOT NULL ,
	 note_time						varchar2(10)		NULL ,
	 note_type_concept_id			number			NOT NULL ,
	 note_text						clob	NOT NULL ,
     provider_id					number			NULL ,
	 visit_occurrence_id			number			NULL ,
	 note_source_value				varchar2(50)		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.observation 
    ( 
     observation_id					number			NOT NULL , 
     person_id						number			NOT NULL , 
     observation_concept_id			number			NOT NULL , 
     observation_date				DATE			NOT NULL , 
     observation_time				varchar2(10)		NULL , 
     observation_type_concept_id	number			NOT NULL , 
	 value_as_number				number			NULL , 
     value_as_string				varchar2(60)		NULL , 
     value_as_concept_id			number			NULL , 
	 qualifier_concept_id			number			NULL ,
     unit_concept_id				number			NULL , 
     provider_id					number			NULL , 
     visit_occurrence_id			number			NULL , 
     observation_source_value		varchar2(50)		NULL ,
	 observation_source_concept_id	number			NULL , 
     unit_source_value				varchar2(50)		NULL ,
	 qualifier_source_value			varchar2(50)		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.fact_relationship 
    ( 
     domain_concept_id_1			number			NOT NULL , 
	 fact_id_1						number			NOT NULL ,
	 domain_concept_id_2			number			NOT NULL ,
	 fact_id_2						number			NOT NULL ,
	 relationship_concept_id		number			NOT NULL
	)
;




/************************

Standardized health system data

************************/



CREATE TABLE omop.location 
    ( 
     location_id					number			NOT NULL , 
     address_1						varchar2(50)		NULL , 
     address_2						varchar2(50)		NULL , 
     city							varchar2(50)		NULL , 
     state							varchar2(2)		NULL , 
     zip							varchar2(9)		NULL , 
     county							varchar2(20)		NULL , 
     location_source_value			varchar2(50)		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;



CREATE TABLE omop.care_site 
    ( 
     care_site_id						number			NOT NULL , 
	 care_site_name						varchar2(255)	NULL ,
     place_of_service_concept_id		number			NULL ,
     location_id						number			NULL , 
     care_site_source_value				varchar2(50)		NULL , 
     place_of_service_source_value		varchar2(50)		NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;


	
CREATE TABLE omop.provider 
    ( 
     provider_id					number			NOT NULL ,
	 provider_name					varchar2(255)	NULL , 
     NPI							varchar2(20)		NULL , 
     DEA							varchar2(20)		NULL , 
     specialty_concept_id			number			NULL , 
     care_site_id					number			NULL , 
	 year_of_birth					number			NULL ,
	 gender_concept_id				number			NULL ,
     provider_source_value			varchar2(50)		NULL , 
     specialty_source_value			varchar2(50)		NULL ,
	 specialty_source_concept_id	number			NULL , 
	 gender_source_value			varchar2(50)		NULL ,
	 gender_source_concept_id		number			NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;




/************************

Standardized health economics

************************/


CREATE TABLE omop.payer_plan_period 
    ( 
     payer_plan_period_id			number			NOT NULL , 
     person_id						number			NOT NULL , 
     payer_plan_period_start_date	DATE			NOT NULL , 
     payer_plan_period_end_date		DATE			NOT NULL , 
     payer_source_value				varchar2 (50)	NULL , 
     plan_source_value				varchar2 (50)	NULL , 
     family_source_value			varchar2 (50)	NULL ,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;


/* The individual cost tables are being phased out and will disappear soon

CREATE TABLE omop.visit_cost 
    ( 
     visit_cost_id					number			NOT NULL , 
     visit_occurrence_id			number			NOT NULL , 
	 currency_concept_id			number			NULL ,
     paid_copay						number			NULL , 
     paid_coinsurance				number			NULL , 
     paid_toward_deductible			number			NULL , 
     paid_by_payer					number			NULL , 
     paid_by_coordination_benefits	number			NULL , 
     total_out_of_pocket			number			NULL , 
     total_paid						number			NULL ,  
     payer_plan_period_id			number			NULL
    ) 
;



CREATE TABLE omop.procedure_cost 
    ( 
     procedure_cost_id				number			NOT NULL , 
     procedure_occurrence_id		number			NOT NULL , 
     currency_concept_id			number			NULL ,
     paid_copay						number			NULL , 
     paid_coinsurance				number			NULL , 
     paid_toward_deductible			number			NULL , 
     paid_by_payer					number			NULL , 
     paid_by_coordination_benefits	number			NULL , 
     total_out_of_pocket			number			NULL , 
     total_paid						number			NULL ,
	 revenue_code_concept_id		number			NULL ,  
     payer_plan_period_id			number			NULL ,
	 revenue_code_source_value		varchar2(50)		NULL
	) 
;



CREATE TABLE omop.drug_cost 
    (
     drug_cost_id					number			NOT NULL , 
     drug_exposure_id				number			NOT NULL , 
     currency_concept_id			number			NULL ,
     paid_copay						number			NULL , 
     paid_coinsurance				number			NULL , 
     paid_toward_deductible			number			NULL , 
     paid_by_payer					number			NULL , 
     paid_by_coordination_benefits	number			NULL , 
     total_out_of_pocket			number			NULL , 
     total_paid						number			NULL , 
     ingredient_cost				number			NULL , 
     dispensing_fee					number			NULL , 
     average_wholesale_price		number			NULL , 
     payer_plan_period_id			number			NULL
    ) 
;





CREATE TABLE omop.device_cost 
    (
     device_cost_id					number			NOT NULL , 
     device_exposure_id				number			NOT NULL , 
     currency_concept_id			number			NULL ,
     paid_copay						number			NULL , 
     paid_coinsurance				number			NULL , 
     paid_toward_deductible			number			NULL , 
     paid_by_payer					number			NULL , 
     paid_by_coordination_benefits	number			NULL , 
     total_out_of_pocket			number			NULL , 
     total_paid						number			NULL , 
     payer_plan_period_id			number			NULL
    ) 
;
*/


CREATE TABLE omop.cost 
    (
     cost_id					number	    NOT NULL , 
     cost_event_id       number     NOT NULL ,
     cost_domain_id       varchar2(20)    NOT NULL ,
     cost_type_concept_id       number     NOT NULL ,
     currency_concept_id			number			NULL ,
     total_charge						number			NULL , 
     total_cost						number			NULL , 
     total_paid						number			NULL , 
     paid_by_payer					number			NULL , 
     paid_by_patient						number			NULL , 
     paid_patient_copay						number			NULL , 
     paid_patient_coinsurance				number			NULL , 
     paid_patient_deductible			number			NULL , 
     paid_by_primary						number			NULL , 
     paid_ingredient_cost				number			NULL , 
     paid_dispensing_fee					number			NULL , 
     payer_plan_period_id			number			NULL ,
     amount_allowed		number			NULL , 
     revenue_code_concept_id		number			NULL , 
     reveue_code_source_value    varchar2(50)   NULL,
     x_srcid		number,   
	 x_srcloadid   number,
     x_srcfile          varchar2(20),
     x_createdate       date default sysdate,
     x_updatedate       date  default sysdate

    ) 
;





/************************

Standardized derived elements

************************/

CREATE TABLE omop.cohort 
    ( 
	 cohort_definition_id			number			NOT NULL , 
     subject_id						number			NOT NULL ,
	 cohort_start_date				DATE			NOT NULL , 
     cohort_end_date				DATE			NOT NULL
    ) 
;


CREATE TABLE omop.cohort_attribute 
    ( 
	 cohort_definition_id			number			NOT NULL , 
     cohort_start_date				DATE			NOT NULL , 
     cohort_end_date				DATE			NOT NULL , 
     subject_id						number			NOT NULL , 
     attribute_definition_id		number			NOT NULL ,
	 value_as_number				number			NULL ,
	 value_as_concept_id			number			NULL
    ) 
;




CREATE TABLE omop.drug_era 
    ( 
     drug_era_id					number			NOT NULL , 
     person_id						number			NOT NULL , 
     drug_concept_id				number			NOT NULL , 
     drug_era_start_date			DATE			NOT NULL , 
     drug_era_end_date				DATE			NOT NULL , 
     drug_exposure_count			number			NULL ,
	 gap_days						number			NULL
    ) 
;


CREATE TABLE omop.dose_era 
    (
     dose_era_id					number			NOT NULL , 
     person_id						number			NOT NULL , 
     drug_concept_id				number			NOT NULL , 
	 unit_concept_id				number			NOT NULL ,
	 dose_value						number			NOT NULL ,
     dose_era_start_date			DATE			NOT NULL , 
     dose_era_end_date				DATE			NOT NULL 
    ) 
;




CREATE TABLE omop.condition_era 
    ( 
     condition_era_id				number			NOT NULL , 
     person_id						number			NOT NULL , 
     condition_concept_id			number			NOT NULL , 
     condition_era_start_date		DATE			NOT NULL , 
     condition_era_end_date			DATE			NOT NULL , 
     condition_occurrence_count		number			NULL
    ) 
;







