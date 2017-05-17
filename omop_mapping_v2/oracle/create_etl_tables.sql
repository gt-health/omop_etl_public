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

CREATE TABLE etl.stage_care_site
(
   id                             number         NOT NULL,
   care_site_name                 varchar2(255),
   address_1              varchar2(50),
   address_2              varchar2(50),
   city                   varchar2(50),
   state                  varchar2(2),
   zip                    varchar2(9),
   county                 varchar2(20),
   location_source_value          varchar2(50),
   care_site_source_value         varchar2(50),
   place_of_service_source_value  varchar2(50),
   load_id						int,
   loaded                        number  default 0
);


ALTER TABLE etl.stage_care_site
   ADD CONSTRAINT care_site_source_value_unique UNIQUE (care_site_source_value);

create sequence etl.stg_care_site_id;


CREATE TABLE etl.stage_condition_error
(
   id                           number        NOT NULL,
   condition_code_source_type   varchar2(20),
   condition_source_value       varchar2(20),
   condition_source_type_value  varchar2(20),
   start_date                   timestamp,
   end_date                     timestamp,
   stop_reason                  varchar2(20),
   visit_source_value           varchar2(50),
   person_source_value          varchar2(50),
   provider_source_value        varchar2(50),
   load_id                      number,
   loaded                       number    DEFAULT 0
)
;


CREATE TABLE etl.stage_condition_temp
(
   id                           number        NOT NULL,
   condition_code_source_type   varchar2(20),
   condition_source_value       varchar2(20),
   condition_source_type_value  varchar2(20),
   start_date                   timestamp,
   end_date                     timestamp,
   stop_reason                  varchar2(20),
   visit_source_value           varchar2(50),
   person_source_value          varchar2(50),
   provider_source_value        varchar2(50),
   load_id                      number,
   loaded                       number    DEFAULT 0
)
nologging
;
create sequence etl.stg_cond_id ;

CREATE TABLE etl.stage_condition
(
   id                           number        NOT NULL,
   condition_code_source_type   varchar2(20),
   condition_source_value       varchar2(20),
   condition_source_type_value  varchar2(20),
   start_date                   timestamp,
   end_date                     timestamp,
   stop_reason                  varchar2(20),
   visit_source_value           varchar2(50),
   person_source_value          varchar2(50),
   provider_source_value        varchar2(50),
   load_id                      number,
   loaded                       number    DEFAULT 0
);


CREATE TABLE etl.stage_death
(
   id                           number,
   person_source_value      varchar2(50),
   dod                      timestamp,
   death_type_source_value  varchar2(50),
   cause_source_value       varchar2(50),
   load_id						int,
   loaded                   number    DEFAULT 0
);
create sequence etl.stg_death_seq;



CREATE  TABLE etl.stage_lab_temp
(
   id                             number        NOT NULL,
   measurement_source_type        varchar2(20),
   measurement_source_value       varchar2(50),
   measurement_source_type_value  varchar2(20),
   measurement_date               timestamp,
   operator_source_value          varchar2(2),
   unit_source_value              varchar2(50),
   value_source_value             varchar2(50),
   value_as_number                varchar2(50),
   value_as_string                varchar2(50),
   range_low                      number,
   range_high                     number,
   visit_source_value             varchar2(50),
   person_source_value            varchar2(50),
   provider_source_value          varchar2(50),
   load_id                        number,
   loaded                         number    DEFAULT 0
)
nologging;

create sequence etl.stg_lab_id ;

CREATE  TABLE etl.stage_lab_error
(
   id                             number        NOT NULL,
   measurement_source_type        varchar2(20),
   measurement_source_value       varchar2(50),
   measurement_source_type_value  varchar2(20),
   measurement_date               timestamp,
   operator_source_value          varchar2(2),
   unit_source_value              varchar2(50),
   value_source_value             varchar2(50),
   value_as_number                varchar2(50),
   value_as_string                varchar2(50),
   range_low                      number,
   range_high                     number,
   visit_source_value             varchar2(50),
   person_source_value            varchar2(50),
   provider_source_value          varchar2(50),
   load_id                        number,
   loaded                         number    DEFAULT 0
);

CREATE  TABLE etl.stage_lab
(
   id                             number        NOT NULL,
   measurement_source_type        varchar2(20),
   measurement_source_value       varchar2(50),
   measurement_source_type_value  varchar2(20),
   measurement_date               timestamp,
   operator_source_value          varchar2(2),
   unit_source_value              varchar2(50),
   value_source_value             varchar2(50),
   value_as_number                varchar2(50),
   value_as_string                varchar2(50),
   range_low                      number,
   range_high                     number,
   visit_source_value             varchar2(50),
   person_source_value            varchar2(50),
   provider_source_value          varchar2(50),
   load_id                        number,
   loaded                         number    DEFAULT 0
);
*/

CREATE TABLE etl.stage_person
(
   id                           number,
   person_source_value     varchar2(50)   NOT NULL,
   gender                  varchar2(1),
   year_of_birth           int,
   month_of_birth           int,
   day_of_birth             int,
   time_of_birth            int,
   race                    varchar2(50),
   address_1               varchar2(50),
   address_2               varchar2(50),
   city                    varchar2(50),
   state                   varchar2(2),
   zip                     varchar2(9),
   county                  varchar2(20),
   ethnicity               varchar2(8),
   ethnicity_source_value  varchar2(50),
   gender_source_value     varchar2(50),
   race_source_value       varchar2(50),
   provider_source_value   varchar2(50),
   care_site_source_value  varchar2(50),
   location_source_value   varchar2(50),
   load_id						int,
   loaded                    number    DEFAULT 0
);

ALTER TABLE etl.stage_person
   ADD CONSTRAINT person_source_value_unique UNIQUE (person_source_value);

create sequence etl.stg_person_id_seq;


CREATE  TABLE etl.stage_procedure_temp
(
   id                           number        NOT NULL,
   procedure_code_source_type   varchar2(20),
   procedure_source_value       varchar2(50),
   procedure_source_type_value  varchar2(20),
   code_modifier                varchar2(50),
   procedure_date               timestamp,
   quantity                     number,
   stop_reason                  varchar2(20),
   total_charge                 number,
   total_cost                   number,
   total_paid                   number,
   paid_by_payer                number,
   paid_by_patient              number,
   paid_patient_copay           number,
   paid_patient_coinsurance     number,
   paid_patient_deductible      number,
   paid_by_primary              number,
   visit_source_value           varchar2(50),
   person_source_value          varchar2(50),
   provider_source_value        varchar2(50),
   load_id                      number,
   loaded                       number    DEFAULT 0
)
nologging;

create sequence etl.stg_proc_id ;

CREATE  TABLE etl.stage_procedure_error
(
   id                           number        NOT NULL,
   procedure_code_source_type   varchar2(20),
   procedure_source_value       varchar2(50),
   procedure_source_type_value  varchar2(20),
   code_modifier                varchar2(50),
   procedure_date               timestamp,
   quantity                     number,
   stop_reason                  varchar2(20),
   total_charge                 number,
   total_cost                   number,
   total_paid                   number,
   paid_by_payer                number,
   paid_by_patient              number,
   paid_patient_copay           number,
   paid_patient_coinsurance     number,
   paid_patient_deductible      number,
   paid_by_primary              number,
   visit_source_value           varchar2(50),
   person_source_value          varchar2(50),
   provider_source_value        varchar2(50),
   load_id                      number,
   loaded                       number    DEFAULT 0
);

CREATE  TABLE etl.stage_procedure
(
   id                           number        NOT NULL,
   procedure_code_source_type   varchar2(20),
   procedure_source_value       varchar2(50),
   procedure_source_type_value  varchar2(20),
   code_modifier                varchar2(50),
   procedure_date               timestamp,
   quantity                     number,
   stop_reason                  varchar2(20),
   total_charge                 number,
   total_cost                   number,
   total_paid                   number,
   paid_by_payer                number,
   paid_by_patient              number,
   paid_patient_copay           number,
   paid_patient_coinsurance     number,
   paid_patient_deductible      number,
   paid_by_primary              number,
   visit_source_value           varchar2(50),
   person_source_value          varchar2(50),
   provider_source_value        varchar2(50),
   load_id                      number,
   loaded                       number    DEFAULT 0
);


CREATE TABLE etl.stage_provider
(
   id                           number,
   provider_name           varchar2(50),
   npi                     varchar2(20),
   dea                     varchar2(20),
   specialty_source_value  varchar2(50),
   care_site_source_value  varchar2(50),
   location_source_value   varchar2(50),
   gender                  varchar2(1),
   year_of_birth           integer,
   address_1               varchar2(50),
   address_2               varchar2(50),
   city                    varchar2(50),
   state                   varchar2(2),
   zip                     varchar2(9),
   county                  varchar2(20),
   gender_source_value     varchar2(50),
   provider_source_value   varchar2(50)   NOT NULL,
   load_id						int,
   loaded                    number    DEFAULT 0
);

ALTER TABLE etl.stage_provider
   ADD CONSTRAINT provider_source_value_unique UNIQUE (provider_source_value);
create sequence etl.stg_provider_id_seq;

CREATE  TABLE etl.stage_rx_temp
(
   id                        number        NOT NULL,
   drug_source_type          varchar2(20),
   drug_source_value         varchar2(50),
   drug_source_type_value    varchar2(50),
   drug_start_date           timestamp,
   drug_end_date             timestamp,
   stop_reason               varchar2(20),
   refills                   number,
   quantity                  binary_double,
   days_supply               number,
   dose_unit_source_value    varchar2(50),
   effective_drug_dose       binary_double,
   total_charge              number,
   total_cost                number,
   total_paid                number,
   paid_by_payer             number,
   paid_by_patient           number,
   paid_patient_copay        number,
   paid_patient_coinsurance  number,
   paid_patient_deductible   number,
   paid_by_primary           number,
   paid_ingredient_cost      number,
   pait_dispensing_fee       number,
   route_source_value        varchar2(50),
   visit_source_value        varchar2(50),
   person_source_value       varchar2(50),
   provider_source_value     varchar2(50),
   load_id                   number,
   loaded                    number    DEFAULT 0
)
nologging;

create sequence etl.stg_rx_id ;

CREATE  TABLE etl.stage_rx_error
(
   id                        number        NOT NULL,
   drug_source_type          varchar2(20),
   drug_source_value         varchar2(50),
   drug_source_type_value    varchar2(50),
   drug_start_date           timestamp,
   drug_end_date             timestamp,
   stop_reason               varchar2(20),
   refills                   number,
   quantity                  binary_double,
   days_supply               number,
   dose_unit_source_value    varchar2(50),
   effective_drug_dose       binary_double,
   total_charge              number,
   total_cost                number,
   total_paid                number,
   paid_by_payer             number,
   paid_by_patient           number,
   paid_patient_copay        number,
   paid_patient_coinsurance  number,
   paid_patient_deductible   number,
   paid_by_primary           number,
   paid_ingredient_cost      number,
   pait_dispensing_fee       number,
   route_source_value        varchar2(50),
   visit_source_value        varchar2(50),
   person_source_value       varchar2(50),
   provider_source_value     varchar2(50),
   load_id                   number,
   loaded                    number    DEFAULT 0
);

CREATE  TABLE etl.stage_rx
(
   id                        number        NOT NULL,
   drug_source_type          varchar2(20),
   drug_source_value         varchar2(50),
   drug_source_type_value    varchar2(50),
   drug_start_date           timestamp,
   drug_end_date             timestamp,
   stop_reason               varchar2(20),
   refills                   number,
   quantity                  binary_double,
   days_supply               number,
   dose_unit_source_value    varchar2(50),
   effective_drug_dose       binary_double,
   total_charge              number,
   total_cost                number,
   total_paid                number,
   paid_by_payer             number,
   paid_by_patient           number,
   paid_patient_copay        number,
   paid_patient_coinsurance  number,
   paid_patient_deductible   number,
   paid_by_primary           number,
   paid_ingredient_cost      number,
   pait_dispensing_fee       number,
   route_source_value        varchar2(50),
   visit_source_value        varchar2(50),
   person_source_value       varchar2(50),
   provider_source_value     varchar2(50),
   load_id                   number,
   loaded                    number    DEFAULT 0
);

CREATE TABLE etl.stage_visit
(
   id                           number,
   visit_source_value        varchar2(50)   NOT NULL,
   visit_type                varchar2(8),
   visit_source_type_value   varchar2(5),
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
   person_source_value       varchar2(50),
   provider_source_value     varchar2(50),
   care_site_source_value    varchar2(50),
   load_id						int,
   loaded                    number    DEFAULT 0
);

ALTER TABLE etl.stage_visit
   ADD CONSTRAINT visit_source_value_unique UNIQUE (visit_source_value);

create sequence etl.stg_visit_id_seq;

create table etl.load_info
(
	load_id	number,
	load_name		varchar2(100),
	load_description	varchar2(1000),
	status			int
);
create sequence etl.stg_load_id_seq;

