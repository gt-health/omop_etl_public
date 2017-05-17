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
  
  select count(*) -- into rowcnt
  from etl.load_info
  where load_id = 2;
  
  if rowcnt <> 1 then
    raise ;
  end if;


/*  
  select etl.logm('etl.run_etl_load', 'starting etl load for load_id = ', 2 );

-- ETL - OMOP_temp procedures
  select etl.logm('etl.run_etl_load', 'starting generic_create_locations for load_id = ', 2 );
  select omop.generic_create_locations(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_locations for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting generic_create_caresites for load_id = ', 2 );
  select omop.generic_create_caresites(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_caresites for load_id = ', 2 );


select * from etl.logmessage order by msg_id desc;

  select etl.logm('etl.run_etl_load', 'starting generic_create_providers for load_id = ', 2 );
  select omop.generic_create_providers(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_providers for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting generic_create_person for load_id = ', 2 );
  select omop.generic_create_person(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_person for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting generic_create_deaths for load_id = ', 2 );
  select omop.generic_create_deaths(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_deaths for load_id = ', 2 );

truncate table omop.condition_occurrence_temp;
truncate table omop.procedure_occurrence_temp;
truncate table omop.drug_exposure_temp;
truncate table omop.device_exposure_temp;
truncate table omop.observation_temp;
truncate table omop.measurement_temp;

drop index omop.idx_visit_src_val;


create index idx_caresite_sv on omop.care_site(care_site_source_value);

  select etl.logm('etl.run_etl_load', 'starting generic_create_visits for load_id = ', 2 );
  select omop.generic_create_visits(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_visits for load_id = ', 2 );

set default_tablespace = etl_omop;

-- create index idx_visit_src_val on omop.visit_occurrence( visit_source_value );

  select etl.logm('etl.run_etl_load', 'starting generic_create_conditions for load_id = ', 2 );
  select omop.generic_create_conditions(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_conditions for load_id = ', 2 );


select * from etl.logmessage order by msg_id desc;

  select etl.logm('etl.run_etl_load', 'starting generic_create_procedures for load_id = ', 2 );
  select omop.generic_create_procedures(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_procedures for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting generic_create_measurements for load_id = ', 2 );
  select omop.generic_create_measurements(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_measurements for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting generic_create_drug_exposures for load_id = ', 2 );
  select omop.generic_create_drug_exposures(2);
  select etl.logm('etl.run_etl_load', 'finished generic_create_drug_exposures for load_id = ', 2 );
  */
  
select * from etl.logmessage order by msg_id desc;

select * from omop.drug_exposure_temp;
select * from etl.stage_rx;

  -- QA procedures
  select etl.logm('etl.run_etl_load', 'starting qa_condition_load for load_id = ', 2 );
  select omop.qa_condition_load(2);
  select etl.logm('etl.run_etl_load', 'finished qa_condition_load for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting qa_procedure_load for load_id = ', 2 );
  select omop.qa_procedure_load(2);
  select etl.logm('etl.run_etl_load', 'finished qa_procedure_load for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting qa_lab_load for load_id = ', 2 );
  select omop.qa_lab_load(2);
  select etl.logm('etl.run_etl_load', 'finished qa_lab_load for load_id = ', 2 );

  select etl.logm('etl.run_etl_load', 'starting qa_rx_load for load_id = ', 2 );
  select omop.qa_rx_load(2);
  select etl.logm('etl.run_etl_load', 'finished qa_rx_load for load_id = ', 2 );


  -- write to final omop tables;
  select etl.logm('etl.run_etl_load', 'starting persist_omop_to_perm for load_id = ', 2 );
  select omop.persist_omop_to_perm(2);
  select etl.logm('etl.run_etl_load', 'finished persist_omop_to_perm for load_id = ', 2 );
