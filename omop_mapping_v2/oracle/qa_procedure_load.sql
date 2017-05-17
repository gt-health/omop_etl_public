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
create or replace procedure omop.qa_procedure_load(v_loadid number)

AS

	logmsg  varchar2(200);

begin

-- used loaded column to track the "status" of the mapping for a records from stage
-- 0 = not loaded, 
-- 1 = loaded, but not matched concept, 
-- 2 = loaded matched source concept, 
-- 3 = loaded matched target concept

    etl.logm('qa_procedure_load', 'process v_loadid: '||v_loadid , 'START' ); 
    
    etl.logm('qa_procedure_load', 'create Index ' , 'START' ); 
    
    execute immediate 'drop index if exists etl.idx_stg_proc_temp_1';
    execute immediate 'create index idx_stg_proc_temp_1 on etl.stage_procedure_temp( id, loaded )';

    etl.logm('qa_procedure_load', 'update from procedures' , 'START' ); 

        
      -- look for unmapped records that may have been mapped in other tables.
      -- if they are mapped in other tables, then they ( concept_id = 0 ) 
      -- do not need to be loaded in condition_occurrence
      
    -- check procedure_occurrence
    merge into etl.stage_procedure_temp sc
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
        and x_srcfile = 'STAGE_PROCEDURE'
        group by x_srcid
        ) b
    ) a
    on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
    when matched then 
      update set sc.loaded = a.loaded;

     etl.logm('qa_procedure_load', 'update from procedures' , 'FINISH' ); 
    
     etl.logm('qa_procedure_load', 'update from measurements' , 'START' );  
    -- check measurement

    merge into etl.stage_procedure_temp sc
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
        and x_srcfile = 'STAGE_PROCEDURE'
        group by x_srcid
        ) b
    ) a
    on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
    when matched then 
      update set sc.loaded = a.loaded;

     etl.logm('qa_procedure_load', 'update from measurements' , 'FINISH' ); 
    
     etl.logm('qa_procedure_load', 'update from observations' , 'START' ); 
            
    -- check observation
    merge into etl.stage_procedure_temp sc
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
        and x_srcfile = 'STAGE_PROCEDURE'
        group by x_srcid
        ) b
    ) a
    on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
    when matched then 
      update set sc.loaded = a.loaded;

     etl.logm('qa_procedure_load', 'update from observations' , 'FINISH' );  
    

     etl.logm('qa_procedure_load', 'update from drug_exposure' , 'START' );

    -- check drug
    merge into etl.stage_procedure_temp sc
    using
    (
     select distinct loaded, id 
      from
      (
        select
          max(
            case
              when nullif(drug_concept_id, 0 ) is not null
                then 3
              when nullif(drug_source_concept_id, 0 ) is not null
                then 2
            else 1
            end 
          ) as loaded,
          x_srcid as id
        from omop.drug_exposure_temp t
        where x_srcloadid = v_loadid
        and x_srcfile = 'STAGE_PROCEDURE'
        group by x_srcid
        ) b
    ) a
    on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
    when matched then 
      update set sc.loaded = a.loaded;


    etl.logm('qa_procedure_load', 'update from drug_exposure' , 'FINISH' );

    etl.logm('qa_procedure_load', 'update from device_exposure' , 'START' );

    -- check device
    merge into etl.stage_procedure_temp sc
    using
    (
     select distinct loaded, id 
      from
      (
        select
          max(
            case
              when nullif(device_concept_id, 0 ) is not null
                then 3
              when nullif(device_source_concept_id, 0 ) is not null
                then 2
            else 1
            end 
          ) as loaded,
          x_srcid as id
        from omop.device_exposure_temp t
        where x_srcloadid = v_loadid
        and x_srcfile = 'STAGE_PROCEDURE'
        group by x_srcid
        ) b
    ) a
    on ( a.id = sc.id and sc.load_id = v_loadid and sc.loaded < a.loaded )
    when matched then 
      update set sc.loaded = a.loaded;
      

    etl.logm('qa_procedure_load', 'update from device_exposure' , 'FINISH' );

    etl.logm('qa_procedure_load', 'update from procedures' , 'FINISH' );  

    select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg from etl.stage_procedure_temp
    where loaded = 3;
         etl.logm('qa_procedure_load', 'QA check count: ' , logmsg ); 
    
    select 'Loadid: '||v_loadid||', loaded = 2:' || count(*) into logmsg from etl.stage_procedure_temp
    where loaded = 2;
         etl.logm('qa_procedure_load', 'QA check count: ' , logmsg ); 
    
    select 'Loadid: '||v_loadid||', loaded = 1:' || count(*) into logmsg from etl.stage_procedure_temp
    where loaded = 1;
         etl.logm('qa_procedure_load', 'QA check count: ' , logmsg ); 
    
    select 'Loadid: '||v_loadid||', loaded = 0:' || count(*) into logmsg from etl.stage_procedure_temp
    where loaded = 0;
         etl.logm('qa_procedure_load', 'QA check count: ' , logmsg ); 


    -- copy records that did not make it over to error
    insert into etl.stage_procedure_error
    (
        id,
        procedure_code_source_type,
        procedure_source_value,
        procedure_source_type_value,
        code_modifier,
        procedure_date,
        quantity,
        stop_reason,
        total_charge,
        total_cost,
        total_paid,
        paid_by_payer,
        paid_by_patient,
        paid_patient_copay,
        paid_patient_coinsurance,
        paid_patient_deductible,
        paid_by_primary,
        visit_source_value,
        person_source_value,
        provider_source_value,
        load_id,
        loaded
    )
    select 
      id,
      procedure_code_source_type,
      procedure_source_value,
      procedure_source_type_value,
      code_modifier,
      procedure_date,
      quantity,
      stop_reason,
      total_charge,
      total_cost,
      total_paid,
      paid_by_payer,
      paid_by_patient,
      paid_patient_copay,
      paid_patient_coinsurance,
      paid_patient_deductible,
      paid_by_primary,
      visit_source_value,
      person_source_value,
      provider_source_value,
      load_id,
      loaded
    from etl.stage_procedure_temp 
    where loaded = 0
    and load_id = v_loadid;
    
    etl.logm('qa_procedure_load', 'Error records moved to etl.stage_condition_error: ' , sql%ROWCOUNT );

    -- remove unmapped records that were mapped in other tables
    delete from omop.procedure_occurrence_temp co
    where co.x_srcid in
    (
        select id 
        from etl.stage_procedure_temp c
        join omop.procedure_occurrence_temp co2 on ( c.id = co2.x_srcid and c.load_id = x_srcloadid )
        where c.loaded >1
        and co2.procedure_source_concept_id = 0
        and x_srcfile = 'STAGE_PROCEDURE'
        and c.load_id = v_loadid
    ) 
    and co.x_srcfile = 'STAGE_PROCEDURE'
    and co.x_srcloadid = v_loadid
    ;

    etl.logm('qa_procedure_load', 'unmapped records removed that were mapped in other tables: ' , sql%ROWCOUNT );
    
    
    -- move to etl stage permanent home
   insert into etl.stage_procedure_error
    (
        id,
        procedure_code_source_type,
        procedure_source_value,
        procedure_source_type_value,
        code_modifier,
        procedure_date,
        quantity,
        stop_reason,
        total_charge,
        total_cost,
        total_paid,
        paid_by_payer,
        paid_by_patient,
        paid_patient_copay,
        paid_patient_coinsurance,
        paid_patient_deductible,
        paid_by_primary,
        visit_source_value,
        person_source_value,
        provider_source_value,
        load_id,
        loaded
    )
    select 
      id,
      procedure_code_source_type,
      procedure_source_value,
      procedure_source_type_value,
      code_modifier,
      procedure_date,
      quantity,
      stop_reason,
      total_charge,
      total_cost,
      total_paid,
      paid_by_payer,
      paid_by_patient,
      paid_patient_copay,
      paid_patient_coinsurance,
      paid_patient_deductible,
      paid_by_primary,
      visit_source_value,
      person_source_value,
      provider_source_value,
      load_id,
      loaded
    from etl.stage_procedure_temp 
    where load_id = v_loadid;
    
    etl.logm('qa_procedure_load', 'moved from etl.stage_procedure_temp to permanent: ' , sql%ROWCOUNT );
	
    delete from etl.stage_procedure_temp 
    where load_id = v_loadid;
    
    etl.logm('qa_procedure_load', 'deleted from etl.stage_procedure_temp: ' , sql%ROWCOUNT );

    commit;
    etl.logm('qa_procedure_load', 'process' , 'FINISH' ); 

end;

 
