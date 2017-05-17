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
-- oracle
create schema if not exists etl;

CREATE TABLE etl.stage_care_site
(
   id                             serial         NOT NULL,
   care_site_name                 varchar(255),
   address_1              varchar(50),
   address_2              varchar(50),
   city                   varchar(50),
   state                  varchar(2),
   zip                    varchar(9),
   county                 varchar(20),
   location_source_value          varchar(50),
   care_site_source_value         varchar(50),
   place_of_service_source_value  varchar(50),
   load_id						int,
   loaded                        int  default 0
);


ALTER TABLE etl.stage_care_site
   ADD CONSTRAINT care_site_source_value_unique UNIQUE (care_site_source_value);

-- create sequence etl.stg_care_site_id;


CREATE TABLE etl.stage_condition_error
(
   id                           bigint        NOT NULL,
   condition_code_source_type   varchar(20),
   condition_source_value       varchar(20),
   condition_source_type_value  varchar(20),
   start_date                   timestamp,
   end_date                     timestamp,
   stop_reason                  varchar(20),
   visit_source_value           varchar(50),
   person_source_value          varchar(50),
   provider_source_value        varchar(50),
   load_id                      int,
   loaded                       int    DEFAULT 0
)
;


CREATE unlogged TABLE etl.stage_condition_temp
(
   id                           serial        NOT NULL,
   condition_code_source_type   varchar(20),
   condition_source_value       varchar(20),
   condition_source_type_value  varchar(20),
   start_date                   timestamp,
   end_date                     timestamp,
   stop_reason                  varchar(20),
   visit_source_value           varchar(50),
   person_source_value          varchar(50),
   provider_source_value        varchar(50),
   load_id                      int,
   loaded                       int    DEFAULT 0
)
with ( autovacuum_enabled=FALSE )
;
-- create sequence etl.stg_cond_id ;

CREATE TABLE etl.stage_condition
(
   id                           bigint        NOT NULL,
   condition_code_source_type   varchar(20),
   condition_source_value       varchar(20),
   condition_source_type_value  varchar(20),
   start_date                   timestamp,
   end_date                     timestamp,
   stop_reason                  varchar(20),
   visit_source_value           varchar(50),
   person_source_value          varchar(50),
   provider_source_value        varchar(50),
   load_id                      int,
   loaded                       int    DEFAULT 0
);


CREATE TABLE etl.stage_death
(
   id                           serial,
   person_source_value      varchar(50),
   dod                      timestamp,
   death_type_source_value  varchar(50),
   cause_source_value       varchar(50),
   load_id						int,
   loaded                   int    DEFAULT 0
);
-- create sequence etl.stg_death_seq;



CREATE unlogged  TABLE etl.stage_lab_temp
(
   id                             serial        NOT NULL,
   measurement_source_type        varchar(20),
   measurement_source_value       varchar(50),
   measurement_source_type_value  varchar(20),
   measurement_date               timestamp,
   operator_source_value          varchar(2),
   unit_source_value              varchar(50),
   value_source_value             varchar(50),
   value_as_number                varchar(50),
   value_as_string                varchar(50),
   range_low                      numeric,
   range_high                     numeric,
   visit_source_value             varchar(50),
   person_source_value            varchar(50),
   provider_source_value          varchar(50),
   load_id                        int,
   loaded                         int    DEFAULT 0
)
with ( autovacuum_enabled=FALSE );

-- create sequence etl.stg_lab_id ;

CREATE  TABLE etl.stage_lab_error
(
   id                             bigint        NOT NULL,
   measurement_source_type        varchar(20),
   measurement_source_value       varchar(50),
   measurement_source_type_value  varchar(20),
   measurement_date               timestamp,
   operator_source_value          varchar(2),
   unit_source_value              varchar(50),
   value_source_value             varchar(50),
   value_as_number                varchar(50),
   value_as_string                varchar(50),
   range_low                      numeric,
   range_high                     numeric,
   visit_source_value             varchar(50),
   person_source_value            varchar(50),
   provider_source_value          varchar(50),
   load_id                        int,
   loaded                         int    DEFAULT 0
);

CREATE  TABLE etl.stage_lab
(
   id                             bigint        NOT NULL,
   measurement_source_type        varchar(20),
   measurement_source_value       varchar(50),
   measurement_source_type_value  varchar(20),
   measurement_date               timestamp,
   operator_source_value          varchar(2),
   unit_source_value              varchar(50),
   value_source_value             varchar(50),
   value_as_number                varchar(50),
   value_as_string                varchar(50),
   range_low                      numeric,
   range_high                     numeric,
   visit_source_value             varchar(50),
   person_source_value            varchar(50),
   provider_source_value          varchar(50),
   load_id                        int,
   loaded                         int    DEFAULT 0
);


CREATE TABLE etl.stage_person
(
   id                           serial,
   person_source_value     varchar(50)   NOT NULL,
   gender                  varchar(1),
   year_of_birth           int,
   month_of_birth           int,
   day_of_birth             int,
   time_of_birth            int,
   race                    varchar(50),
   address_1               varchar(50),
   address_2               varchar(50),
   city                    varchar(50),
   state                   varchar(2),
   zip                     varchar(9),
   county                  varchar(20),
   ethnicity               varchar(8),
   ethnicity_source_value  varchar(50),
   gender_source_value     varchar(50),
   race_source_value       varchar(50),
   provider_source_value   varchar(50),
   care_site_source_value  varchar(50),
   location_source_value   varchar(50),
   load_id						int,
   loaded                    int    DEFAULT 0
);

ALTER TABLE etl.stage_person
   ADD CONSTRAINT person_source_value_unique UNIQUE (person_source_value);

-- create sequence etl.stg_person_id_seq;


CREATE unlogged  TABLE etl.stage_procedure_temp
(
   id                           serial        NOT NULL,
   procedure_code_source_type   varchar(20),
   procedure_source_value       varchar(50),
   procedure_source_type_value  varchar(20),
   code_modifier                varchar(50),
   procedure_date               timestamp,
   quantity                     int,
   stop_reason                  varchar(20),
   total_charge                 numeric,
   total_cost                   numeric,
   total_paid                   numeric,
   paid_by_payer                numeric,
   paid_by_patient              numeric,
   paid_patient_copay           numeric,
   paid_patient_coinsurance     numeric,
   paid_patient_deductible      numeric,
   paid_by_primary              numeric,
   visit_source_value           varchar(50),
   person_source_value          varchar(50),
   provider_source_value        varchar(50),
   load_id                      int,
   loaded                       int    DEFAULT 0
)
with ( autovacuum_enabled=FALSE );

-- create sequence etl.stg_proc_id ;

CREATE  TABLE etl.stage_procedure_error
(
   id                           bigint        NOT NULL,
   procedure_code_source_type   varchar(20),
   procedure_source_value       varchar(50),
   procedure_source_type_value  varchar(20),
   code_modifier                varchar(50),
   procedure_date               timestamp,
   quantity                     int,
   stop_reason                  varchar(20),
   total_charge                 numeric,
   total_cost                   numeric,
   total_paid                   numeric,
   paid_by_payer                numeric,
   paid_by_patient              numeric,
   paid_patient_copay           numeric,
   paid_patient_coinsurance     numeric,
   paid_patient_deductible      numeric,
   paid_by_primary              numeric,
   visit_source_value           varchar(50),
   person_source_value          varchar(50),
   provider_source_value        varchar(50),
   load_id                      int,
   loaded                       int    DEFAULT 0
);

CREATE  TABLE etl.stage_procedure
(
   id                           bigint        NOT NULL,
   procedure_code_source_type   varchar(20),
   procedure_source_value       varchar(50),
   procedure_source_type_value  varchar(20),
   code_modifier                varchar(50),
   procedure_date               timestamp,
   quantity                     int,
   stop_reason                  varchar(20),
   total_charge                 numeric,
   total_cost                   numeric,
   total_paid                   numeric,
   paid_by_payer                numeric,
   paid_by_patient              numeric,
   paid_patient_copay           numeric,
   paid_patient_coinsurance     numeric,
   paid_patient_deductible      numeric,
   paid_by_primary              numeric,
   visit_source_value           varchar(50),
   person_source_value          varchar(50),
   provider_source_value        varchar(50),
   load_id                      int,
   loaded                       int    DEFAULT 0
);


CREATE TABLE etl.stage_provider
(
   id                           serial,
   provider_name           varchar(50),
   npi                     varchar(20),
   dea                     varchar(20),
   specialty_source_value  varchar(50),
   care_site_source_value  varchar(50),
   location_source_value   varchar(50),
   gender                  varchar(1),
   year_of_birth           integer,
   address_1               varchar(50),
   address_2               varchar(50),
   city                    varchar(50),
   state                   varchar(2),
   zip                     varchar(9),
   county                  varchar(20),
   gender_source_value     varchar(50),
   provider_source_value   varchar(50)   NOT NULL,
   load_id						int,
   loaded                    int    DEFAULT 0
);

ALTER TABLE etl.stage_provider
   ADD CONSTRAINT provider_source_value_unique UNIQUE (provider_source_value);
-- create sequence etl.stg_provider_id_seq;

CREATE unlogged  TABLE etl.stage_rx_temp
(
   id                        serial        NOT NULL,
   drug_source_type          varchar(20),
   drug_source_value         varchar(50),
   drug_source_type_value    varchar(50),
   drug_start_date           timestamp,
   drug_end_date             timestamp,
   stop_reason               varchar(20),
   refills                   int,
   quantity                  float8,
   days_supply               int,
   dose_unit_source_value    varchar(50),
   effective_drug_dose       float8,
   total_charge              numeric,
   total_cost                numeric,
   total_paid                numeric,
   paid_by_payer             numeric,
   paid_by_patient           numeric,
   paid_patient_copay        numeric,
   paid_patient_coinsurance  numeric,
   paid_patient_deductible   numeric,
   paid_by_primary           numeric,
   paid_ingredient_cost      numeric,
   pait_dispensing_fee       numeric,
   route_source_value        varchar(50),
   visit_source_value        varchar(50),
   person_source_value       varchar(50),
   provider_source_value     varchar(50),
   load_id                   int,
   loaded                    int    DEFAULT 0
)
with ( autovacuum_enabled=FALSE );

-- create sequence etl.stg_rx_id ;

CREATE  TABLE etl.stage_rx_error
(
   id                        bigint        NOT NULL,
   drug_source_type          varchar(20),
   drug_source_value         varchar(50),
   drug_source_type_value    varchar(50),
   drug_start_date           timestamp,
   drug_end_date             timestamp,
   stop_reason               varchar(20),
   refills                   int,
   quantity                  float8,
   days_supply               int,
   dose_unit_source_value    varchar(50),
   effective_drug_dose       float8,
   total_charge              numeric,
   total_cost                numeric,
   total_paid                numeric,
   paid_by_payer             numeric,
   paid_by_patient           numeric,
   paid_patient_copay        numeric,
   paid_patient_coinsurance  numeric,
   paid_patient_deductible   numeric,
   paid_by_primary           numeric,
   paid_ingredient_cost      numeric,
   pait_dispensing_fee       numeric,
   route_source_value        varchar(50),
   visit_source_value        varchar(50),
   person_source_value       varchar(50),
   provider_source_value     varchar(50),
   load_id                   int,
   loaded                    int    DEFAULT 0
);

CREATE  TABLE etl.stage_rx
(
   id                        bigint        NOT NULL,
   drug_source_type          varchar(20),
   drug_source_value         varchar(50),
   drug_source_type_value    varchar(50),
   drug_start_date           timestamp,
   drug_end_date             timestamp,
   stop_reason               varchar(20),
   refills                   int,
   quantity                  float8,
   days_supply               int,
   dose_unit_source_value    varchar(50),
   effective_drug_dose       float8,
   total_charge              numeric,
   total_cost                numeric,
   total_paid                numeric,
   paid_by_payer             numeric,
   paid_by_patient           numeric,
   paid_patient_copay        numeric,
   paid_patient_coinsurance  numeric,
   paid_patient_deductible   numeric,
   paid_by_primary           numeric,
   paid_ingredient_cost      numeric,
   pait_dispensing_fee       numeric,
   route_source_value        varchar(50),
   visit_source_value        varchar(50),
   person_source_value       varchar(50),
   provider_source_value     varchar(50),
   load_id                   int,
   loaded                    int    DEFAULT 0
);

CREATE TABLE etl.stage_visit
(
   id                           serial,
   visit_source_value        varchar(50)   NOT NULL,
   visit_type                varchar(8),
   visit_source_type_value   varchar(5),
   visit_start_date          timestamp,
   visit_end_date            timestamp,
   total_charge              numeric,
   total_cost                numeric,
   total_paid                numeric,
   paid_by_payer             numeric,
   paid_by_patient           numeric,
   paid_patient_copay        numeric,
   paid_patient_coinsurance  numeric,
   paid_patient_deductible   numeric,
   paid_by_primary           numeric,
   person_source_value       varchar(50),
   provider_source_value     varchar(50),
   care_site_source_value    varchar(50),
   load_id						int,
   loaded                    int    DEFAULT 0
);

ALTER TABLE etl.stage_visit
   ADD CONSTRAINT visit_source_value_unique UNIQUE (visit_source_value);

-- create sequence etl.stg_visit_id_seq;

create table etl.load_info
(
	load_id	serial,
	load_name		varchar(100),
	load_description	varchar(1000),
	status			int
);
-- create sequence etl.stg_load_id_seq;

