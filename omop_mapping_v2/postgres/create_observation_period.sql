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
CREATE OR REPLACE FUNCTION ims.create_proto_obs_period(start_person_id integer, end_person_id integer)
  RETURNS void
  LANGUAGE plpgsql
AS
$body$
declare
        rowcnt  integer;

    begin
    
        perform etl.logm('create_proto_obs_period', 'start' , 'start_person: '||start_person_id||' end_person_id: '|| end_person_id );
/*
create table ims.proto_observation_period
(
    person_id bigint,
    min_date    date,
    max_date    date,
    source_table      varchar(100)
 );
 */
         insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( visit_start_date ) min_date,
            max( visit_start_date ) max_date,
            'visit_occurrence' as source_table
        from omop_v5.visit_occurrence p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from vis_occ into proto_observation_period' , rowcnt ); 
        


        insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( procedure_date ) min_date,
            max( procedure_date ) max_date,
            'procedure_occurrence' as source_table
        from omop_v5.procedure_occurrence p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from proc_occ into proto_observation_period' , rowcnt ); 
        
        insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( condition_start_date ) min_date,
            max( condition_start_date ) max_date,
            'condition_occurrence' as source_table
        from omop_v5.condition_occurrence p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from cond_occ into proto_observation_period' , rowcnt ); 

        insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( drug_exposure_start_date ) min_date,
            max( drug_exposure_start_date ) max_date,
            'drug_exposure' as source_table
        from omop_v5.drug_exposure p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from drug_exp into proto_observation_period' , rowcnt ); 

        insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( device_exposure_start_date ) min_date,
            max( device_exposure_start_date ) max_date,
            'device_exposure' as source_table
        from omop_v5.device_exposure p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from device_exp into proto_observation_period' , rowcnt ); 

        insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( observation_date ) min_date,
            max( observation_date ) max_date,
            'observation' as source_table
        from omop_v5.observation p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from obs into proto_observation_period' , rowcnt );  
 
        insert into ims.proto_observation_period
        ( person_id, min_date, max_date, source_table )
        select 
            person_id,
            min( measurement_date ) min_date,
            max( measurement_date ) max_date,
            'measurement' as source_table
        from omop_v5.measurement p
        where person_id between start_person_id and end_person_id
        group by person_id
        ;

        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('create_proto_obs_period', 'insert from meas into proto_observation_period' , rowcnt );         
       
        perform etl.logm('create_drug_era', 'create_drug_era' , 'FINISH' );


    end;
$body$
  VOLATILE
  COST 100;

COMMIT;
