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
set search_path = omop;


CREATE unlogged TABLE omop.condition_occurrence_temp
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
-- Column condition_occurrence_id is associated with sequence omop.condition_occurrence_id_seq


CREATE unlogged TABLE omop.device_exposure_temp
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


CREATE unlogged TABLE omop.drug_exposure_temp
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

-- Column drug_exposure_id is associated with sequence omop.drug_exposure_id_seq


CREATE unlogged TABLE omop.measurement_temp
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

-- Column measurement_id is associated with sequence omop.measurement_id_seq

CREATE unlogged TABLE omop.observation_temp
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

CREATE TABLE omop.procedure_occurrence_temp
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

-- Column procedure_occurrence_id is associated with sequence omop.procedure_occurrence_id_seq




