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
create or replace
function      omop.generic_create_deaths( p_loadid integer )
returns void
language plpgsql
    as
$$

declare
        rowcnt  integer;
        v_loadid integer;

    begin

        v_loadid := p_loadid;
        perform etl.logm('generic_create_deaths', 'generic_create_deaths loadid:'||v_loadid , 'START' );
        
          INSERT INTO omop.death
          (
            person_id,
            death_date,
            death_type_concept_id,
            cause_source_value,
            cause_source_concept_id,
            x_srcid,
            x_srcfile
          )
          select
            p.person_id as person_id,
            d.dod as death_date,
            case d.death_type_source_value
              when 'SSN' then 242
              when 'ETL_DISCHARGE' then 44818516
              else 0
            end as death_type_concept_id,
            null as cause_source_value,   -- TODO
            null as cause_source_concept_id   -- TODO
            , d.id as x_srcid
            , 'STAGE_DEATH' as x_srcfile
          from etl.stage_death d
          join omop.person p on d.person_source_value = p.person_source_value
          where load_id = v_loadid;

          get diagnostics rowcnt = ROW_COUNT;

          perform etl.logm('generic_create_deaths', 'insert into death' , rowcnt );

          perform etl.logm('generic_create_deaths', 'generic_create_deaths' , 'FINISH' );


    end;
$$
