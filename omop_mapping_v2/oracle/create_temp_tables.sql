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
-- ORACLE version

-- these are not TRUE temporary tables.  They are meant to be truncated as needed by the procedure controlling the process.


-- ----------------  OMOP    -----------------------------

-- create OMOP temporary tables.  these are used to stage the inserts before finalizing to the omop tables.
-- they add the sequence number for the final tables in their inserts.  This will cause holes in the dataset after the QA process runs on temp tables.


CREATE  TABLE omop.condition_occurrence_temp
(
   condition_occurrence_id      number       NOT NULL,
   person_id                    number       NOT NULL,
   condition_concept_id         number       NOT NULL,
   condition_start_date         date          NOT NULL,
   condition_end_date           date,
   condition_type_concept_id    number       NOT NULL,
   stop_reason                  varchar2(20),
   provider_id                  number,
   visit_occurrence_id          number,
   condition_source_value       varchar2(50),
   condition_source_concept_id  number,
   x_srcid                      number,
   x_srcloadid                  number,
   x_srcfile                    varchar2(20),
   x_createdate                 date          DEFAULT sysdate,
   x_updatedate                 date          DEFAULT sysdate
)
nologging;

-- Column condition_occurrence_id is associated with sequence omop.condition_occurrence_id_seq

COMMIT;

CREATE  TABLE omop.drug_exposure_temp
(
   drug_exposure_id          number       NOT NULL,
   person_id                 number       NOT NULL,
   drug_concept_id           number       NOT NULL,
   drug_exposure_start_date  date          NOT NULL,
   drug_exposure_end_date    date,
   drug_type_concept_id      number       NOT NULL,
   stop_reason               varchar2(20),
   refills                   number,
   quantity                  number,
   days_supply               number,
   sig                       clob,
   route_concept_id          number,
   effective_drug_dose       number,
   dose_unit_concept_id      number,
   lot_number                varchar2(50),
   provider_id               number,
   visit_occurrence_id       number,
   drug_source_value         varchar2(50),
   drug_source_concept_id    number,
   route_source_value        varchar2(50),
   dose_unit_source_value    varchar2(50),
   x_srcid                   number,
   x_srcloadid               number,
   x_srcfile                 varchar2(20),
   x_createdate              date          DEFAULT sysdate,
   x_updatedate              date          DEFAULT sysdate
)
nologging;


-- Column drug_exposure_id is associated with sequence omop.drug_exposure_id_seq


CREATE  TABLE omop.measurement_temp
(
   measurement_id                 number       NOT NULL,
   person_id                      number       NOT NULL,
   measurement_concept_id         number       NOT NULL,
   measurement_date               date          NOT NULL,
   measurement_time               varchar2(10),
   measurement_type_concept_id    number       NOT NULL,
   operator_concept_id            number,
   value_as_number                number,
   value_as_concept_id            number,
   unit_concept_id                number,
   range_low                      number,
   range_high                     number,
   provider_id                    number,
   visit_occurrence_id            number,
   measurement_source_value       varchar2(50),
   measurement_source_concept_id  number,
   unit_source_value              varchar2(50),
   value_source_value             varchar2(50),
   x_srcid                        number,
   x_srcloadid                    number,
   x_srcfile                      varchar2(20),
   x_createdate                   date          DEFAULT sysdate,
   x_updatedate                   date          DEFAULT sysdate
)
nologging;

-- Column measurement_id is associated with sequence omop.measurement_id_seq

CREATE  TABLE omop.observation_temp
(
   observation_id                 number       NOT NULL,
   person_id                      number       NOT NULL,
   observation_concept_id         number       NOT NULL,
   observation_date               date          NOT NULL,
   observation_time               varchar2(10),
   observation_type_concept_id    number       NOT NULL,
   value_as_number                number,
   value_as_string                varchar2(60),
   value_as_concept_id            number,
   qualifier_concept_id           number,
   unit_concept_id                number,
   provider_id                    number,
   visit_occurrence_id            number,
   observation_source_value       varchar2(50),
   observation_source_concept_id  number,
   unit_source_value              varchar2(50),
   qualifier_source_value         varchar2(50),
   x_srcid                        number,
   x_srcloadid                    number,
   x_srcfile                      varchar2(20),
   x_createdate                   date          DEFAULT sysdate,
   x_updatedate                   date          DEFAULT sysdate
)
nologging;

CREATE TABLE omop.procedure_occurrence_temp
(
   procedure_occurrence_id      number       NOT NULL,
   person_id                    number       NOT NULL,
   procedure_concept_id         number       NOT NULL,
   procedure_date               date          NOT NULL,
   procedure_type_concept_id    number       NOT NULL,
   modifier_concept_id          number,
   quantity                     number,
   provider_id                  number,
   visit_occurrence_id          number,
   procedure_source_value       varchar2(50),
   procedure_source_concept_id  number,
   qualifier_source_value       varchar2(50),
   x_srcid                      number,
   x_srcloadid                  number,
   x_srcfile                    varchar2(20),
   x_createdate                 date          DEFAULT sysdate,
   x_updatedate                 date          DEFAULT sysdate
)
nologging;

 
  CREATE TABLE omop.device_exposure_temp 
  (	
      device_exposure_id number not null enable, 
      person_id number not null enable, 
      device_concept_id number not null enable, 
      device_exposure_start_date date not null enable, 
      device_exposure_end_date date, 
      device_type_concept_id number not null enable, 
      unique_device_id varchar2(50 byte), 
      quantity number, 
      provider_id number, 
      visit_occurrence_id number, 
      device_source_value varchar2(100 byte), 
      device_source_concept_id number, 
      x_srcid number, 
      x_srcloadid number, 
      x_srcfile varchar2(20 byte), 
      x_createdate date default sysdate, 
      x_updatedate date default sysdate
  ) ;

-- Column procedure_occurrence_id is associated with sequence omop.procedure_occurrence_id_seq

COMMIT;



