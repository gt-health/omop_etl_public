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
create or replace function etl.run_etl_load( p_loadid integer )
returns void
language plpgsql
as
$$

declare
  v_loadid integer;
  -- e_invalid_loadid exception;
  rowcnt integer;
  v_code integer;
  v_errm  varchar(200);
  
begin
  
  v_loadid := p_loadid;
  
  select count(*) into rowcnt
  from etl.load_info
  where load_id = v_loadid;
  
  if rowcnt <> 1 then
    raise ;
  end if;
  
  perform etl.logm('etl.run_etl_load', 'starting etl load for load_id = ', v_loadid );

-- ETL - OMOP_temp procedures
  perform etl.logm('etl.run_etl_load', 'starting generic_create_locations for load_id = ', v_loadid );
  perform omop.generic_create_locations(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_locations for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_caresites for load_id = ', v_loadid );
  perform omop.generic_create_caresites(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_caresites for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_providers for load_id = ', v_loadid );
  perform omop.generic_create_providers(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_providers for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_person for load_id = ', v_loadid );
  perform omop.generic_create_person(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_person for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_deaths for load_id = ', v_loadid );
  perform omop.generic_create_deaths(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_deaths for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_visits for load_id = ', v_loadid );
  perform omop.generic_create_visits(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_visits for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_conditions for load_id = ', v_loadid );
  perform omop.generic_create_conditions(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_conditions for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_procedures for load_id = ', v_loadid );
  perform omop.generic_create_procedures(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_procedures for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_measurements for load_id = ', v_loadid );
  perform omop.generic_create_measurements(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_measurements for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting generic_create_drug_exposures for load_id = ', v_loadid );
  perform omop.generic_create_drug_exposures(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished generic_create_drug_exposures for load_id = ', v_loadid );
  
  -- QA procedures
  perform etl.logm('etl.run_etl_load', 'starting qa_condition_load for load_id = ', v_loadid );
  perform omop.qa_condition_load(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished qa_condition_load for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting qa_procedure_load for load_id = ', v_loadid );
  perform omop.qa_procedure_load(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished qa_procedure_load for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting qa_lab_load for load_id = ', v_loadid );
  perform omop.qa_lab_load(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished qa_lab_load for load_id = ', v_loadid );

  perform etl.logm('etl.run_etl_load', 'starting qa_rx_load for load_id = ', v_loadid );
  perform omop.qa_rx_load(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished qa_rx_load for load_id = ', v_loadid );


  -- write to final omop tables;
  perform etl.logm('etl.run_etl_load', 'starting persist_omop_to_perm for load_id = ', v_loadid );
  perform omop.persist_omop_to_perm(v_loadid);
  perform etl.logm('etl.run_etl_load', 'finished persist_omop_to_perm for load_id = ', v_loadid );
  
  commit;
  
  perform etl.logm('etl.run_etl_load', 'starting etl load for load_id = ', v_loadid );
  
exception
  -- when e_invalid_loadid
  -- then
    -- etl.logm( 'etl.run_etl_load', 'ERROR: invalid load_id = ', p_loadid );
    -- raise;
  when others then
    perform etl.logm( 'etl.run_etl_load', 'ERROR: invalid load_id = ', p_loadid );
    --v_code := SQLCODE;
    --v_errm := substr(sqlerrm,1,200);
    --etl.logm( 'etl.run_etl_load ROLLBACK', substr('ERROR: '||v_code||', '|| v_errm, 1, 200), v_loadid );
    raise;
end;
 $$
