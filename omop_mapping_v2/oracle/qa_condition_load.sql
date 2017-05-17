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
procedure      omop.qa_condition_load(v_loadid int)

AS

	logmsg  varchar2(200);
  rownum integer;

begin

-- used loaded column to track the "status" of the mapping for a records from stage
-- 0 = not loaded, 
-- 1 = loaded, but not matched concept, 
-- 2 = loaded matched source concept, 
-- 3 = loaded matched target concept


        etl.logm('qa_condition_load', 'process v_loadid: '||v_loadid , 'START' ); 

        etl.logm('qa_condition_load', 'create Index ' , 'START' ); 
		    
        execute immediate 'drop index if exists etl.idx_stg_cond_temp_1' ;

        execute immediate 'create index idx_stg_cond_temp_1 on etl.stage_condition_temp( id, loaded )';

        etl.logm('qa_condition_load', 'update from conditions' , 'START' ); 
        
        
      -- look for unmapped records that may have been mapped in other tables.
      -- if they are mapped in other tables, then they ( concept_id = 0 ) 
      -- do not need to be loaded in condition_occurrence
      
        -- check condition_occurrence
        
        merge into etl.stage_condition_temp sc
        using
        (
         select distinct loaded, id 
          from
          (
            select
              max(
                case
                  when nullif(condition_concept_id, 0 ) is not null
                    then 3
                  when nullif(condition_source_concept_id, 0 ) is not null
                    then 2
                else 1
                end 
              ) as loaded,
              x_srcid as id
            from omop.condition_occurrence_temp t
            where x_srcloadid = v_loadid
            and x_srcfile = 'STAGE_CONDITION'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded
        ;
          

        etl.logm('qa_condition_load', 'update from conditions' , 'FINISH' ); 
        
        etl.logm('qa_condition_load', 'update from measurements' , 'START' );  

        -- check measurement

        merge into etl.stage_condition_temp sc
        using
        (
         select distinct loaded, id 
          from
          (
            select
              max(
                case
                  when nullif(measurement_concept_id, 0 ) is not null
                    then 3
                  when nullif(measurement_source_concept_id, 0 ) is not null
                    then 2
                else 1
                end 
              ) as loaded,
              x_srcid as id
            from omop.measurement_temp t
            where x_srcloadid = v_loadid
            and x_srcfile = 'STAGE_CONDITION'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded;
 
        etl.logm('qa_condition_load', 'update from measurements' , 'FINISH' ); 
        
        etl.logm('qa_condition_load', 'update from observations' , 'START' ); 
                
        -- check observation
        
        merge into etl.stage_condition_temp sc
        using
        (
         select distinct loaded, id 
          from
          (
            select
              max(
                case
                  when nullif(observation_concept_id, 0 ) is not null
                    then 3
                  when nullif(observation_source_concept_id, 0 ) is not null
                    then 2
                else 1
                end 
              ) as loaded,
              x_srcid as id
            from omop.observation_temp t
            where x_srcloadid = v_loadid
            and x_srcfile = 'STAGE_CONDITION'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded;
 
        etl.logm('qa_condition_load', 'update from observations' , 'FINISH' );  
        
        etl.logm('qa_condition_load', 'update from procedures' , 'START' );  
       
        -- check procedure
              
        merge into etl.stage_condition_temp sc
        using
        (
         select distinct loaded, id 
          from
          (
            select
              max(
                case
                  when nullif(procedure_concept_id, 0 ) is not null
                    then 3
                  when nullif(procedure_source_concept_id, 0 ) is not null
                    then 2
                else 1
                end 
              ) as loaded,
              x_srcid as id
            from omop.procedure_occurrence_temp t
            where x_srcloadid = v_loadid
            and x_srcfile = 'STAGE_CONDITION'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded;

        etl.logm('qa_condition_load', 'update from procedures' , 'FINISH' );  

        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg from etl.stage_condition_temp
        where loaded = 3 and load_id = v_loadid;
            etl.logm('qa_condition_load', 'QA check count: ' , logmsg ); 
    
        select 'Loadid: '||v_loadid||', loaded = 2:' || count(*) into logmsg from etl.stage_condition_temp
        where loaded = 2 and load_id = v_loadid;
            etl.logm('qa_condition_load', 'QA check count: ' , logmsg ); 
    
        select 'Loadid: '||v_loadid||', loaded = 1:' || count(*) into logmsg from etl.stage_condition_temp
        where loaded = 1 and load_id = v_loadid;
            etl.logm('qa_condition_load', 'QA check count: ' , logmsg ); 
    
        select 'Loadid: '||v_loadid||', loaded = 0:' || count(*) into logmsg from etl.stage_condition_temp
        where loaded = 0 and load_id = v_loadid;
            etl.logm('qa_condition_load', 'QA check count: ' , logmsg ); 
    
        -- copy records that did not make it over to error
        insert into etl.stage_condition_error
        (
          id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        )
        select id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        from etl.stage_condition_temp
        where loaded = 0
        and load_id = v_loadid;
        
        etl.logm('qa_condition_load', 'Error records moved to etl.stage_condition_error: ' , sql%ROWCOUNT );
    
        -- remove unmapped records that were mapped in other tables
        delete from omop.condition_occurrence_temp co
        where co.x_srcid in
        (
            select id 
            from etl.stage_condition_temp c
            join omop.condition_occurrence_temp co2 on ( c.id = co2.x_srcid and c.load_id = x_srcloadid )
            where c.loaded >1
            and co2.condition_source_concept_id = 0
            and x_srcfile = 'STAGE_CONDITION'
            and c.load_id = v_loadid
        ) 
        and co.x_srcfile = 'STAGE_CONDITION'
        and co.x_srcloadid = v_loadid
        ;

        etl.logm('qa_condition_load', 'unmapped records removed that were mapped in other tables: ' , sql%ROWCOUNT );

        
        -- move to etl stage permanent home
        insert into etl.stage_condition
        (
          id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        )
        select id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        from etl.stage_condition_temp 
        where load_id = v_loadid;
        
        etl.logm('qa_condition_load', 'moved from etl.stage_condition_temp to permanent: ' , sql%ROWCOUNT );
        
        delete from etl.stage_condition_temp 
        where load_id = v_loadid;
        
        etl.logm('qa_condition_load', 'deleted from etl.stage_condition_temp: ' , sql%ROWCOUNT );

        commit;
        etl.logm('qa_condition_load', 'process' , 'FINISH' ); 

end;
/

 
