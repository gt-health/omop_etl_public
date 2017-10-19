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

create schema if not exists omop;

set search_path = omop;


CREATE TABLE concept (
  concept_id			INTEGER			NOT NULL,
  concept_name			VARCHAR(255)	NOT NULL,
  domain_id				VARCHAR(20)		NOT NULL,
  vocabulary_id			VARCHAR(20)		NOT NULL,
  concept_class_id		VARCHAR(20)		NOT NULL,
  standard_concept		VARCHAR(1)		NULL,
  concept_code			VARCHAR(50)		NOT NULL,
  valid_start_date		DATE			NOT NULL,
  valid_end_date		DATE			NOT NULL,
  invalid_reason		VARCHAR(1)		NULL
)
;




CREATE TABLE vocabulary (
  vocabulary_id			VARCHAR(20)		NOT NULL,
  vocabulary_name		VARCHAR(255)	NOT NULL,
  vocabulary_reference	VARCHAR(255)	NULL,
  vocabulary_version	VARCHAR(255)	NULL,
  vocabulary_concept_id	INTEGER			NOT NULL
)
;




CREATE TABLE domain (
  domain_id			VARCHAR(20)		NOT NULL,
  domain_name		VARCHAR(255)	NOT NULL,
  domain_concept_id	INTEGER			NOT NULL
)
;



CREATE TABLE concept_class (
  concept_class_id			VARCHAR(20)		NOT NULL,
  concept_class_name		VARCHAR(255)	NOT NULL,
  concept_class_concept_id	INTEGER			NOT NULL
)
;




CREATE TABLE concept_relationship (
  concept_id_1			INTEGER			NOT NULL,
  concept_id_2			INTEGER			NOT NULL,
  relationship_id		VARCHAR(20)		NOT NULL,
  valid_start_date		DATE			NOT NULL,
  valid_end_date		DATE			NOT NULL,
  invalid_reason		VARCHAR(1)		NULL)
;



CREATE TABLE relationship (
  relationship_id			VARCHAR(20)		NOT NULL,
  relationship_name			VARCHAR(255)	NOT NULL,
  is_hierarchical			VARCHAR(1)		NOT NULL,
  defines_ancestry			VARCHAR(1)		NOT NULL,
  reverse_relationship_id	VARCHAR(20)		NOT NULL,
  relationship_concept_id	INTEGER			NOT NULL
)
;


CREATE TABLE concept_synonym (
  concept_id			INTEGER			NOT NULL,
  concept_synonym_name	VARCHAR(1000)	NOT NULL,
  language_concept_id	INTEGER			NOT NULL
)
;


CREATE TABLE concept_ancestor (
  ancestor_concept_id		INTEGER		NOT NULL,
  descendant_concept_id		INTEGER		NOT NULL,
  min_levels_of_separation	INTEGER		NOT NULL,
  max_levels_of_separation	INTEGER		NOT NULL
)
;



CREATE TABLE source_to_concept_map (
  source_code				VARCHAR(50)		NOT NULL,
  source_concept_id			INTEGER			NOT NULL,
  source_vocabulary_id		VARCHAR(20)		NOT NULL,
  source_code_description	VARCHAR(255)	NULL,
  target_concept_id			INTEGER			NOT NULL,
  target_vocabulary_id		VARCHAR(20)		NOT NULL,
  valid_start_date			DATE			NOT NULL,
  valid_end_date			DATE			NOT NULL,
  invalid_reason			VARCHAR(1)		NULL
)
;




CREATE TABLE drug_strength (
  drug_concept_id				INTEGER		NOT NULL,
  ingredient_concept_id			INTEGER		NOT NULL,
  amount_value					NUMERIC		NULL,
  amount_unit_concept_id		INTEGER		NULL,
  numerator_value				NUMERIC		NULL,
  numerator_unit_concept_id		INTEGER		NULL,
  denominator_value				NUMERIC		NULL,
  denominator_unit_concept_id	INTEGER		NULL,
  box_size			integer 	null,
  valid_start_date				DATE		NOT NULL,
  valid_end_date				DATE		NOT NULL,
  invalid_reason				VARCHAR(1)	NULL
)
;



CREATE TABLE cohort_definition (
  cohort_definition_id				INTEGER			NOT NULL,
  cohort_definition_name			VARCHAR(255)	NOT NULL,
  cohort_definition_description		TEXT	NULL,
  definition_type_concept_id		INTEGER			NOT NULL,
  cohort_definition_syntax			TEXT	NULL,
  subject_concept_id				INTEGER			NOT NULL,
  cohort_initiation_date			DATE			NULL
)
;


CREATE TABLE attribute_definition (
  attribute_definition_id		INTEGER			NOT NULL,
  attribute_name				VARCHAR(255)	NOT NULL,
  attribute_description			TEXT	NULL,
  attribute_type_concept_id		INTEGER			NOT NULL,
  attribute_syntax				TEXT	NULL
)
;


/**************************

Standardized meta-data

***************************/


CREATE TABLE cdm_source 
    (  
     cdm_source_name					VARCHAR(255)	NOT NULL,
	 cdm_source_abbreviation			VARCHAR(25)		NULL,
	 cdm_holder							VARCHAR(255)	NULL,
	 source_description					TEXT	NULL,
	 source_documentation_reference		VARCHAR(255)	NULL,
	 cdm_etl_reference					VARCHAR(255)	NULL,
	 source_release_date				DATE			NULL,
	 cdm_release_date					DATE			NULL,
	 cdm_version						VARCHAR(10)		NULL,
	 vocabulary_version					VARCHAR(20)		NULL
    ) 
;



/************************

Standardized clinical data

************************/


CREATE TABLE person 
    (
     person_id						INTEGER		NOT NULL , 
     gender_concept_id				INTEGER		NOT NULL , 
     year_of_birth					INTEGER		NOT NULL , 
     month_of_birth					INTEGER		NULL, 
     day_of_birth					INTEGER		NULL, 
	 time_of_birth					VARCHAR(10)	NULL,
     race_concept_id				INTEGER		NOT NULL, 
     ethnicity_concept_id			INTEGER		NOT NULL, 
     location_id					INTEGER		NULL, 
     provider_id					INTEGER		NULL, 
     care_site_id					INTEGER		NULL, 
     person_source_value			VARCHAR(50) NULL, 
     gender_source_value			VARCHAR(50) NULL,
	 gender_source_concept_id		INTEGER		NULL, 
     race_source_value				VARCHAR(50) NULL, 
	 race_source_concept_id			INTEGER		NULL, 
     ethnicity_source_value			VARCHAR(50) NULL,
	 ethnicity_source_concept_id	INTEGER		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate	date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()
     
    ) 
;





CREATE TABLE observation_period 
    ( 
     observation_period_id				INTEGER		NOT NULL , 
     person_id							INTEGER		NOT NULL , 
     observation_period_start_date		DATE		NOT NULL , 
     observation_period_end_date		DATE		NOT NULL ,
	 period_type_concept_id				INTEGER		NOT NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE specimen
    ( 
     specimen_id						INTEGER			NOT NULL ,
	 person_id							INTEGER			NOT NULL ,
	 specimen_concept_id				INTEGER			NOT NULL ,
	 specimen_type_concept_id			INTEGER			NOT NULL ,
	 specimen_date						DATE			NOT NULL ,
	 specimen_time						VARCHAR(10)		NULL ,
	 quantity							NUMERIC			NULL ,
	 unit_concept_id					INTEGER			NULL ,
	 anatomic_site_concept_id			INTEGER			NULL ,
	 disease_status_concept_id			INTEGER			NULL ,
	 specimen_source_id					VARCHAR(50)		NULL ,
	 specimen_source_value				VARCHAR(50)		NULL ,
	 unit_source_value					VARCHAR(50)		NULL ,
	 anatomic_site_source_value			VARCHAR(50)		NULL ,
	 disease_status_source_value		VARCHAR(50)		NULL,
     x_srcid		integer,   
     x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

	)
;



CREATE TABLE death 
    ( 
     person_id							INTEGER			NOT NULL , 
     death_date							DATE			NOT NULL , 
     death_type_concept_id				INTEGER			NOT NULL , 
     cause_concept_id					INTEGER			NULL , 
     cause_source_value					VARCHAR(50)		NULL,
	 cause_source_concept_id			INTEGER			NULL,
     x_srcid		integer,   
	x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE visit_occurrence 
    ( 
     visit_occurrence_id			bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     visit_concept_id				INTEGER			NOT NULL , 
	 visit_start_date				DATE			NOT NULL , 
	 visit_start_time				VARCHAR(10)		NULL ,
     visit_end_date					DATE			NOT NULL ,
	 visit_end_time					VARCHAR(10)		NULL , 
	 visit_type_concept_id			INTEGER			NOT NULL ,
	 provider_id					INTEGER			NULL,
     care_site_id					INTEGER			NULL, 
     visit_source_value				VARCHAR(50)		NULL,
	 visit_source_concept_id		INTEGER			NULL,
     x_srcid		integer,   
		x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE procedure_occurrence 
    ( 
     procedure_occurrence_id		bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     procedure_concept_id			INTEGER			NOT NULL , 
     procedure_date					DATE			NOT NULL , 
     procedure_type_concept_id		INTEGER			NOT NULL ,
	 modifier_concept_id			INTEGER			NULL ,
	 quantity						INTEGER			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			bigint			NULL , 
     procedure_source_value			VARCHAR(50)		NULL ,
	 procedure_source_concept_id	INTEGER			NULL ,
	 qualifier_source_value			VARCHAR(50)		NULL,
     x_srcid		integer,   
     x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE drug_exposure 
    ( 
     drug_exposure_id				bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     drug_concept_id				INTEGER			NOT NULL , 
     drug_exposure_start_date		DATE			NOT NULL , 
     drug_exposure_end_date			DATE			NULL , 
     drug_type_concept_id			INTEGER			NOT NULL , 
     stop_reason					VARCHAR(20)		NULL , 
     refills						INTEGER			NULL , 
     quantity						NUMERIC			NULL , 
     days_supply					INTEGER			NULL , 
     sig							TEXT	NULL , 
	 route_concept_id				INTEGER			NULL ,
	 effective_drug_dose			NUMERIC			NULL ,
	 dose_unit_concept_id			INTEGER			NULL ,
	 lot_number						VARCHAR(50)		NULL ,
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			bigint			NULL , 
     drug_source_value				VARCHAR(50)		NULL ,
	 drug_source_concept_id			INTEGER			NULL ,
	 route_source_value				VARCHAR(50)		NULL ,
	 dose_unit_source_value			VARCHAR(50)		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;


CREATE TABLE device_exposure 
    ( 
     device_exposure_id				bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     device_concept_id				INTEGER			NOT NULL , 
     device_exposure_start_date		DATE			NOT NULL , 
     device_exposure_end_date		DATE			NULL , 
     device_type_concept_id			INTEGER			NOT NULL , 
	 unique_device_id				VARCHAR(50)		NULL ,
	 quantity						INTEGER			NULL ,
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			bigint			NULL , 
     device_source_value			VARCHAR(100)	NULL ,
	 device_source_concept_id		INTEGER			NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;


CREATE TABLE condition_occurrence 
    ( 
     condition_occurrence_id		bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     condition_concept_id			INTEGER			NOT NULL , 
     condition_start_date			DATE			NOT NULL , 
     condition_end_date				DATE			NULL , 
     condition_type_concept_id		INTEGER			NOT NULL , 
     stop_reason					VARCHAR(20)		NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			bigint			NULL , 
     condition_source_value			VARCHAR(50)		NULL ,
	 condition_source_concept_id	INTEGER			NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE measurement 
    ( 
     measurement_id					bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     measurement_concept_id			INTEGER			NOT NULL , 
     measurement_date				DATE			NOT NULL , 
     measurement_time				VARCHAR(10)		NULL ,
	 measurement_type_concept_id	INTEGER			NOT NULL ,
	 operator_concept_id			INTEGER			NULL , 
     value_as_number				NUMERIC			NULL , 
     value_as_concept_id			INTEGER			NULL , 
     unit_concept_id				INTEGER			NULL , 
     range_low						NUMERIC			NULL , 
     range_high						NUMERIC			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			bigint			NULL ,  
     measurement_source_value		VARCHAR(50)		NULL , 
	 measurement_source_concept_id	INTEGER			NULL ,
     unit_source_value				VARCHAR(50)		NULL ,
	 value_source_value				VARCHAR(50)		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE note 
    ( 
     note_id						bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     note_date						DATE			NOT NULL ,
	 note_time						VARCHAR(10)		NULL ,
	 note_type_concept_id			INTEGER			NOT NULL ,
	 note_text						TEXT	NOT NULL ,
     provider_id					INTEGER			NULL ,
	 visit_occurrence_id			bigint			NULL ,
	 note_source_value				VARCHAR(50)		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE observation 
    ( 
     observation_id					bigint			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     observation_concept_id			INTEGER			NOT NULL , 
     observation_date				DATE			NOT NULL , 
     observation_time				VARCHAR(10)		NULL , 
     observation_type_concept_id	INTEGER			NOT NULL , 
	 value_as_number				NUMERIC			NULL , 
     value_as_string				VARCHAR(60)		NULL , 
     value_as_concept_id			INTEGER			NULL , 
	 qualifier_concept_id			INTEGER			NULL ,
     unit_concept_id				INTEGER			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			bigint			NULL , 
     observation_source_value		VARCHAR(50)		NULL ,
	 observation_source_concept_id	INTEGER			NULL , 
     unit_source_value				VARCHAR(50)		NULL ,
	 qualifier_source_value			VARCHAR(50)		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE fact_relationship 
    ( 
     domain_concept_id_1			INTEGER			NOT NULL , 
	 fact_id_1						INTEGER			NOT NULL ,
	 domain_concept_id_2			INTEGER			NOT NULL ,
	 fact_id_2						INTEGER			NOT NULL ,
	 relationship_concept_id		INTEGER			NOT NULL
	)
;




/************************

Standardized health system data

************************/



CREATE TABLE location 
    ( 
     location_id					INTEGER			NOT NULL , 
     address_1						VARCHAR(50)		NULL , 
     address_2						VARCHAR(50)		NULL , 
     city							VARCHAR(50)		NULL , 
     state							VARCHAR(2)		NULL , 
     zip							VARCHAR(9)		NULL , 
     county							VARCHAR(20)		NULL , 
     location_source_value			VARCHAR(50)		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;



CREATE TABLE care_site 
    ( 
     care_site_id						INTEGER			NOT NULL , 
	 care_site_name						VARCHAR(255)	NULL ,
     place_of_service_concept_id		INTEGER			NULL ,
     location_id						INTEGER			NULL , 
     care_site_source_value				VARCHAR(50)		NULL , 
     place_of_service_source_value		VARCHAR(50)		NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;


	
CREATE TABLE provider 
    ( 
     provider_id					INTEGER			NOT NULL ,
	 provider_name					VARCHAR(255)	NULL , 
     NPI							VARCHAR(20)		NULL , 
     DEA							VARCHAR(20)		NULL , 
     specialty_concept_id			INTEGER			NULL , 
     care_site_id					INTEGER			NULL , 
	 year_of_birth					INTEGER			NULL ,
	 gender_concept_id				INTEGER			NULL ,
     provider_source_value			VARCHAR(50)		NULL , 
     specialty_source_value			VARCHAR(50)		NULL ,
	 specialty_source_concept_id	INTEGER			NULL , 
	 gender_source_value			VARCHAR(50)		NULL ,
	 gender_source_concept_id		INTEGER			NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;




/************************

Standardized health economics

************************/


CREATE TABLE payer_plan_period 
    ( 
     payer_plan_period_id			INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     payer_plan_period_start_date	DATE			NOT NULL , 
     payer_plan_period_end_date		DATE			NOT NULL , 
     payer_source_value				VARCHAR (50)	NULL , 
     plan_source_value				VARCHAR (50)	NULL , 
     family_source_value			VARCHAR (50)	NULL ,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;


/* The individual cost tables are being phased out and will disappear soon

CREATE TABLE visit_cost 
    ( 
     visit_cost_id					INTEGER			NOT NULL , 
     visit_occurrence_id			INTEGER			NOT NULL , 
	 currency_concept_id			INTEGER			NULL ,
     paid_copay						NUMERIC			NULL , 
     paid_coinsurance				NUMERIC			NULL , 
     paid_toward_deductible			NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_coordination_benefits	NUMERIC			NULL , 
     total_out_of_pocket			NUMERIC			NULL , 
     total_paid						NUMERIC			NULL ,  
     payer_plan_period_id			INTEGER			NULL
    ) 
;



CREATE TABLE procedure_cost 
    ( 
     procedure_cost_id				INTEGER			NOT NULL , 
     procedure_occurrence_id		INTEGER			NOT NULL , 
     currency_concept_id			INTEGER			NULL ,
     paid_copay						NUMERIC			NULL , 
     paid_coinsurance				NUMERIC			NULL , 
     paid_toward_deductible			NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_coordination_benefits	NUMERIC			NULL , 
     total_out_of_pocket			NUMERIC			NULL , 
     total_paid						NUMERIC			NULL ,
	 revenue_code_concept_id		INTEGER			NULL ,  
     payer_plan_period_id			INTEGER			NULL ,
	 revenue_code_source_value		VARCHAR(50)		NULL
	) 
;



CREATE TABLE drug_cost 
    (
     drug_cost_id					INTEGER			NOT NULL , 
     drug_exposure_id				INTEGER			NOT NULL , 
     currency_concept_id			INTEGER			NULL ,
     paid_copay						NUMERIC			NULL , 
     paid_coinsurance				NUMERIC			NULL , 
     paid_toward_deductible			NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_coordination_benefits	NUMERIC			NULL , 
     total_out_of_pocket			NUMERIC			NULL , 
     total_paid						NUMERIC			NULL , 
     ingredient_cost				NUMERIC			NULL , 
     dispensing_fee					NUMERIC			NULL , 
     average_wholesale_price		NUMERIC			NULL , 
     payer_plan_period_id			INTEGER			NULL
    ) 
;





CREATE TABLE device_cost 
    (
     device_cost_id					INTEGER			NOT NULL , 
     device_exposure_id				INTEGER			NOT NULL , 
     currency_concept_id			INTEGER			NULL ,
     paid_copay						NUMERIC			NULL , 
     paid_coinsurance				NUMERIC			NULL , 
     paid_toward_deductible			NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_coordination_benefits	NUMERIC			NULL , 
     total_out_of_pocket			NUMERIC			NULL , 
     total_paid						NUMERIC			NULL , 
     payer_plan_period_id			INTEGER			NULL
    ) 
;
*/


CREATE TABLE cost 
    (
     cost_id					bigint	    NOT NULL , 
     cost_event_id       bigint     NOT NULL ,
     cost_domain_id       VARCHAR(20)    NOT NULL ,
     cost_type_concept_id       INTEGER     NOT NULL ,
     currency_concept_id			INTEGER			NULL ,
     total_charge						NUMERIC			NULL , 
     total_cost						NUMERIC			NULL , 
     total_paid						NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_patient						NUMERIC			NULL , 
     paid_patient_copay						NUMERIC			NULL , 
     paid_patient_coinsurance				NUMERIC			NULL , 
     paid_patient_deductible			NUMERIC			NULL , 
     paid_by_primary						NUMERIC			NULL , 
     paid_ingredient_cost				NUMERIC			NULL , 
     paid_dispensing_fee					NUMERIC			NULL , 
     payer_plan_period_id			INTEGER			NULL ,
     amount_allowed		NUMERIC			NULL , 
     revenue_code_concept_id		INTEGER			NULL , 
     reveue_code_source_value    VARCHAR(50)   NULL,
     x_srcid		integer,   
	 x_srcloadid   integer,
     x_srcfile          varchar(20),
     x_createdate       date default statement_timestamp(),
     x_updatedate       date  default statement_timestamp()

    ) 
;





/************************

Standardized derived elements

************************/

CREATE TABLE cohort 
    ( 
	 cohort_definition_id			INTEGER			NOT NULL , 
     subject_id						INTEGER			NOT NULL ,
	 cohort_start_date				DATE			NOT NULL , 
     cohort_end_date				DATE			NOT NULL
    ) 
;


CREATE TABLE cohort_attribute 
    ( 
	 cohort_definition_id			INTEGER			NOT NULL , 
     cohort_start_date				DATE			NOT NULL , 
     cohort_end_date				DATE			NOT NULL , 
     subject_id						INTEGER			NOT NULL , 
     attribute_definition_id		INTEGER			NOT NULL ,
	 value_as_number				NUMERIC			NULL ,
	 value_as_concept_id			INTEGER			NULL
    ) 
;




CREATE TABLE drug_era 
    ( 
     drug_era_id					INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     drug_concept_id				INTEGER			NOT NULL , 
     drug_era_start_date			DATE			NOT NULL , 
     drug_era_end_date				DATE			NOT NULL , 
     drug_exposure_count			INTEGER			NULL ,
	 gap_days						INTEGER			NULL
    ) 
;


CREATE TABLE dose_era 
    (
     dose_era_id					INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     drug_concept_id				INTEGER			NOT NULL , 
	 unit_concept_id				INTEGER			NOT NULL ,
	 dose_value						NUMERIC			NOT NULL ,
     dose_era_start_date			DATE			NOT NULL , 
     dose_era_end_date				DATE			NOT NULL 
    ) 
;




CREATE TABLE condition_era 
    ( 
     condition_era_id				INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     condition_concept_id			INTEGER			NOT NULL , 
     condition_era_start_date		DATE			NOT NULL , 
     condition_era_end_date			DATE			NOT NULL , 
     condition_occurrence_count		INTEGER			NULL
    ) 
;



CREATE unlogged TABLE condition_occurrence_temp
(
   condition_occurrence_id      bigint       NOT NULL,
   person_id                    integer       NOT NULL,
   condition_concept_id         integer       NOT NULL,
   condition_start_date         date          NOT NULL,
   condition_end_date           date,
   condition_type_concept_id    integer       NOT NULL,
   stop_reason                  varchar(20),
   provider_id                  integer,
   visit_occurrence_id          bigint,
   condition_source_value       varchar(50),
   condition_source_concept_id  integer,
   x_srcid                      integer,
   x_srcloadid                  integer,
   x_srcfile                    varchar(20),
   x_createdate                 date          DEFAULT statement_timestamp(),
   x_updatedate                 date          DEFAULT statement_timestamp()
)
with (autovacuum_enabled=FALSE)
;
-- Column condition_occurrence_id is associated with sequence condition_occurrence_id_seq


CREATE unlogged TABLE device_exposure_temp
(
   device_exposure_id          bigint         NOT NULL,
   person_id                   integer        NOT NULL,
   device_concept_id           integer        NOT NULL,
   device_exposure_start_date  date           NOT NULL,
   device_exposure_end_date    date,
   device_type_concept_id      integer        NOT NULL,
   unique_device_id            varchar(50),
   quantity                    integer,
   provider_id                 integer,
   visit_occurrence_id         bigint,
   device_source_value         varchar(100),
   device_source_concept_id    integer,
   x_srcid                     integer,
   x_srcloadid                 integer,
   x_srcfile                   varchar(20),
   x_createdate                date           DEFAULT statement_timestamp(),
   x_updatedate                date           DEFAULT statement_timestamp()
)
with (autovacuum_enabled=false);


CREATE unlogged TABLE drug_exposure_temp
(
   drug_exposure_id          bigint       NOT NULL,
   person_id                 integer       NOT NULL,
   drug_concept_id           integer       NOT NULL,
   drug_exposure_start_date  date          NOT NULL,
   drug_exposure_end_date    date,
   drug_type_concept_id      integer       NOT NULL,
   stop_reason               varchar(20),
   refills                   integer,
   quantity                  numeric,
   days_supply               integer,
   sig                       text,
   route_concept_id          integer,
   effective_drug_dose       numeric,
   dose_unit_concept_id      integer,
   lot_number                varchar(50),
   provider_id               integer,
   visit_occurrence_id       bigint,
   drug_source_value         varchar(50),
   drug_source_concept_id    integer,
   route_source_value        varchar(50),
   dose_unit_source_value    varchar(50),
   x_srcid                   integer,
   x_srcloadid               integer,
   x_srcfile                 varchar(20),
   x_createdate              date          DEFAULT statement_timestamp(),
   x_updatedate              date          DEFAULT statement_timestamp()
)
with (autovacuum_enabled=FALSE)
;

-- Column drug_exposure_id is associated with sequence drug_exposure_id_seq


CREATE unlogged TABLE measurement_temp
(
   measurement_id                 bigint       NOT NULL,
   person_id                      integer       NOT NULL,
   measurement_concept_id         integer       NOT NULL,
   measurement_date               date          NOT NULL,
   measurement_time               varchar(10),
   measurement_type_concept_id    integer       NOT NULL,
   operator_concept_id            integer,
   value_as_number                numeric,
   value_as_concept_id            integer,
   unit_concept_id                integer,
   range_low                      numeric,
   range_high                     numeric,
   provider_id                    integer,
   visit_occurrence_id            bigint,
   measurement_source_value       varchar(50),
   measurement_source_concept_id  integer,
   unit_source_value              varchar(50),
   value_source_value             varchar(50),
   x_srcid                        integer,
   x_srcloadid                    integer,
   x_srcfile                      varchar(20),
   x_createdate                   date          DEFAULT statement_timestamp(),
   x_updatedate                   date          DEFAULT statement_timestamp()
)
with (autovacuum_enabled=FALSE)
;

-- Column measurement_id is associated with sequence measurement_id_seq

CREATE unlogged TABLE observation_temp
(
   observation_id                 bigint       NOT NULL,
   person_id                      integer       NOT NULL,
   observation_concept_id         integer       NOT NULL,
   observation_date               date          NOT NULL,
   observation_time               varchar(10),
   observation_type_concept_id    integer       NOT NULL,
   value_as_number                numeric,
   value_as_string                varchar(60),
   value_as_concept_id            integer,
   qualifier_concept_id           integer,
   unit_concept_id                integer,
   provider_id                    integer,
   visit_occurrence_id            bigint,
   observation_source_value       varchar(50),
   observation_source_concept_id  integer,
   unit_source_value              varchar(50),
   qualifier_source_value         varchar(50),
   x_srcid                        integer,
   x_srcloadid                    integer,
   x_srcfile                      varchar(20),
   x_createdate                   date          DEFAULT statement_timestamp(),
   x_updatedate                   date          DEFAULT statement_timestamp()
)
with (autovacuum_enabled=FALSE)
;

CREATE TABLE procedure_occurrence_temp
(
   procedure_occurrence_id      bigint       NOT NULL,
   person_id                    integer       NOT NULL,
   procedure_concept_id         integer       NOT NULL,
   procedure_date               date          NOT NULL,
   procedure_type_concept_id    integer       NOT NULL,
   modifier_concept_id          integer,
   quantity                     integer,
   provider_id                  integer,
   visit_occurrence_id          bigint,
   procedure_source_value       varchar(50),
   procedure_source_concept_id  integer,
   qualifier_source_value       varchar(50),
   x_srcid                      integer,
   x_srcloadid                  integer,
   x_srcfile                    varchar(20),
   x_createdate                 date          DEFAULT statement_timestamp(),
   x_updatedate                 date          DEFAULT statement_timestamp()
)
with (autovacuum_enabled=FALSE)
;


-- create sequences
create sequence location_id_seq ;  
create sequence care_site_id_seq ;
create sequence cohort_id_seq ;
create sequence drug_exposure_id_seq;
create sequence visit_occurrence_id_seq;
create sequence condition_occurrence_id_seq;
create sequence procedure_occurrence_id_seq;
create sequence observation_id_seq;
create sequence obs_period_id_seq;
create sequence provider_id_seq;
create sequence measurement_id_seq;
create sequence device_exposure_id_seq;







