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
procedure      omop.qa_lab_load(v_loadid int)

AS

	logmsg  varchar2(200);
  rownum integer;

begin

-- used loaded column to track the "status" of the mapping for a records from stage
-- 0 = not loaded, 
-- 1 = loaded, but not matched concept, 
-- 2 = loaded matched source concept, 
-- 3 = loaded matched target concept
-- create unique index etl_stage_cond_pk on etl.STAGE_LAB( id );

        etl.logm('qa_lab_load', 'process v_loadid: '||v_loadid , 'START' ); 

        etl.logm('qa_lab_load', 'create Index ' , 'START' ); 
		    
        execute immediate 'drop index if exists etl.idx_stg_lab_temp_1' ;

        execute immediate 'create index idx_stg_lab_temp_1 on etl.stage_lab_temp( id, loaded )';

        etl.logm('qa_lab_load', 'update from lab' , 'START' ); 
        
        
      -- look for unmapped records that may have been mapped in other tables.
      -- if they are mapped in other tables, then they ( concept_id = 0 ) 
      -- do not need to be loaded in condition_occurrence
      
/* not currently in use
        -- check condition_occurrence
        
        merge into etl.stage_lab_temp sc
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
            and x_srcfile = 'STAGE_LAB'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded
        ;
          

        etl.logm('qa_lab_load', 'update from conditions' , 'FINISH' ); 
*/

        etl.logm('qa_lab_load', 'update from measurements' , 'START' );  

        -- check measurement

        merge into etl.stage_lab_temp sc
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
            and x_srcfile = 'STAGE_LAB'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded;
 
        etl.logm('qa_lab_load', 'update from measurements' , 'FINISH' ); 
        
        etl.logm('qa_lab_load', 'update from observations' , 'START' ); 
                
        -- check observation
        
        merge into etl.stage_lab_temp sc
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
            and x_srcfile = 'STAGE_LAB'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded;
 
        etl.logm('qa_lab_load', 'update from observations' , 'FINISH' );  
        
        etl.logm('qa_lab_load', 'update from procedures' , 'START' );  

/*  not currrently in use       
        -- check procedure
              
        merge into etl.stage_lab_temp sc
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
            and x_srcfile = 'STAGE_LAB'
            group by x_srcid
            ) b
        ) a
        on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
        when matched then 
          update set sc.loaded = a.loaded;

        etl.logm('qa_lab_load', 'update from procedures' , 'FINISH' );  
*/


        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg from etl.stage_lab_temp
        where loaded = 3 and load_id = v_loadid;
            etl.logm('qa_lab_load', 'QA check count: ' , logmsg ); 
    
        select 'Loadid: '||v_loadid||', loaded = 2:' || count(*) into logmsg from etl.stage_lab_temp
        where loaded = 2 and load_id = v_loadid;
            etl.logm('qa_lab_load', 'QA check count: ' , logmsg ); 
    
        select 'Loadid: '||v_loadid||', loaded = 1:' || count(*) into logmsg from etl.stage_lab_temp
        where loaded = 1 and load_id = v_loadid;
            etl.logm('qa_lab_load', 'QA check count: ' , logmsg ); 
    
        select 'Loadid: '||v_loadid||', loaded = 0:' || count(*) into logmsg from etl.stage_lab_temp
        where loaded = 0 and load_id = v_loadid;
            etl.logm('qa_lab_load', 'QA check count: ' , logmsg ); 
    
        -- copy records that did not make it over to error
        insert into etl.stage_lab_error
        (
            id,
            measurement_source_type,
            measurement_source_value,
            measurement_source_type_value,
            measurement_date,
            operator_source_value,
            unit_source_value,
            value_source_value,
            value_as_number,
            value_as_string,
            range_low,
            range_high,
            visit_source_value,
            person_source_value,
            provider_source_value,
            load_id,
            loaded
        )
        select id,
          measurement_source_type,
          measurement_source_value,
          measurement_source_type_value,
          measurement_date,
          operator_source_value,
          unit_source_value,
          value_source_value,
          value_as_number,
          value_as_string,
          range_low,
          range_high,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        from etl.stage_lab_temp 
        where loaded = 0
        and load_id = v_loadid;
        
        etl.logm('qa_lab_load', 'Error records moved to etl.STAGE_LAB_error: ' , sql%ROWCOUNT );
    
        -- remove unmapped records that were mapped in other tables
        delete from omop.measurement_temp me
        where me.x_srcid in
        (
            select id 
            from etl.stage_lab_temp l
            join omop.measurement_temp me2 on ( l.id = me2.x_srcid and l.load_id = me2.x_srcloadid )
            where l.loaded >1
            and me2.measurement_source_concept_id = 0
            and me2.x_srcfile = 'STAGE_LAB'
            and l.load_id = v_loadid
        ) 
        and me.x_srcfile = 'STAGE_LAB'
        and me.x_srcloadid = v_loadid
        ;

        etl.logm('qa_lab_load', 'unmapped records removed that were mapped in other tables: ' , sql%ROWCOUNT );

        insert into etl.stage_lab_error
        (
            id,
            measurement_source_type,
            measurement_source_value,
            measurement_source_type_value,
            measurement_date,
            operator_source_value,
            unit_source_value,
            value_source_value,
            value_as_number,
            value_as_string,
            range_low,
            range_high,
            visit_source_value,
            person_source_value,
            provider_source_value,
            load_id,
            loaded
        )
        select id,
          measurement_source_type,
          measurement_source_value,
          measurement_source_type_value,
          measurement_date,
          operator_source_value,
          unit_source_value,
          value_source_value,
          value_as_number,
          value_as_string,
          range_low,
          range_high,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        from etl.stage_lab_temp 
        where load_id = v_loadid;
        
        etl.logm('qa_lab_load', 'moved from etl.stage_lab_temp to permanent: ' , sql%ROWCOUNT );

        delete from etl.stage_lab_temp
        where load_id = v_loadid;
        etl.logm('qa_lab_load', 'deleted from etl.stage_lab_temp: ' , sql%ROWCOUNT );
          
        commit;
        etl.logm('qa_lab_load', 'process' , 'FINISH' ); 

end;
/

 
