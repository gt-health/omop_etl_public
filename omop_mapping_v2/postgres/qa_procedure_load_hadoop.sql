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
create or replace function omop.qa_procedure_load(p_loadid integer)
  RETURNS void
  LANGUAGE plpgsql
AS
$$

  declare
    v_loadid integer;
    rowcnt   integer;
    logmsg   varchar(200);

  begin

-- -- 0 = not loaded, 1 = loaded, but not matched concept, 2 = loaded matched source concept,
--  3 = loaded matched target concept
-- create unique index etl_stage_cond_pk on etl.stage_procedure( id );

    v_loadid := p_loadid;

    perform etl.logm('qa_procedure_load', 'process loadid: '||v_loadid , 'START' ); 

    drop index if exists etl.etl_stage_proc_id;
    create index etl_stage_proc_id on etl.stage_procedure_temp( id, load_id );

    perform etl.logm('qa_procedure_load', 'update from procedures' , 'START' ); 
    

    -- create new table for qa


    create table etl.qa_stage_procedure
    (
      id bigint,
      load_id int,
      loaded int
    );



        -- check procedure_occurrence
        insert into etl.qa_stage_procedure 
        ( id, load_id, loaded )
        select 
            pot.x_srcid as id,
            pot.x_srcloadid as loadid,
            case
              when nullif(pot.procedure_concept_id, 0 ) is not null
                then 3
              when nullif(pot.procedure_source_concept_id, 0 ) is not null
                then 2
              else 1
            end as loaded
        from omop.procedure_occurrence_temp pot
        join etl.stage_procedure_temp spt on pot.x_srcid = spt.id and spt.load_id = pot.x_srcloadid
        where pot.x_srcfile = 'STAGE_PROCEDURE'
        and pot.x_srcloadid = v_loadid
        ;


        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('qa_procedure_load', 'update from procedures: ' , rowcnt ); 
        perform etl.logm('qa_procedure_load', 'update from procedures' , 'FINISH' ); 
        
        perform etl.logm('qa_procedure_load', 'update from measurements' , 'START' );  
        -- check measurement


        insert into etl.qa_stage_procedure 
        ( id, load_id, loaded )
        select 
            pot.x_srcid as id,
            pot.x_srcloadid as loadid,
            case
              when nullif(measurement_concept_id, 0 ) is not null
                  then 3
              when nullif(measurement_source_concept_id, 0 ) is not null
                then 2
              else 1
            end as loaded
        from omop.measurement_temp pot
        join etl.stage_procedure_temp spt on pot.x_srcid = spt.id and spt.load_id = pot.x_srcloadid
        where pot.x_srcfile = 'STAGE_PROCEDURE'
        and pot.x_srcloadid = v_loadid
        ;

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_procedure_load', 'update from measurements: ' , rowcnt ); 
        perform etl.logm('qa_procedure_load', 'update from measurements' , 'FINISH' ); 
        
        perform etl.logm('qa_procedure_load', 'update from observations' , 'START' ); 
                
        -- check observation
        insert into etl.qa_stage_procedure 
        ( id, load_id, loaded )
        select 
            pot.x_srcid as id,
            pot.x_srcloadid as loadid,
            case
              when nullif(observation_concept_id, 0 ) is not null
                  then 3
              when nullif(observation_source_concept_id, 0 ) is not null
                then 2
              else 1
            end as loaded
        from omop.observation_temp pot
        join etl.stage_procedure_temp spt on pot.x_srcid = spt.id and spt.load_id = pot.x_srcloadid
        where pot.x_srcfile = 'STAGE_PROCEDURE'
        and pot.x_srcloadid = v_loadid
        ; 
        get diagnostics rowcnt = ROW_COUNT;

        perform etl.logm('qa_procedure_load', 'update from observations: ' , rowcnt);  
        perform etl.logm('qa_procedure_load', 'update from observations' , 'FINISH' );  
        
        perform etl.logm('qa_procedure_load', 'update from condition' , 'START' );  
        
        -- check condition
        insert into etl.qa_stage_procedure 
        ( id, load_id, loaded )
        select 
            pot.x_srcid as id,
            pot.x_srcloadid as loadid,
            case
              when nullif(condition_concept_id, 0 ) is not null
                  then 3
              when nullif(condition_source_concept_id, 0 ) is not null
                then 2
              else 1
            end as loaded
        from omop.condition_occurrence_temp pot
        join etl.stage_procedure_temp spt on pot.x_srcid = spt.id and spt.load_id = pot.x_srcloadid
        where pot.x_srcfile = 'STAGE_PROCEDURE'
        and pot.x_srcloadid = v_loadid
        ;         

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_procedure_load', 'update from condition: ' , rowcnt );  
        perform etl.logm('qa_procedure_load', 'update from condition' , 'FINISH' );  

        perform etl.logm('qa_procedure_load', 'update from drug_exposure' , 'START' );

        -- check drug exposure
        insert into etl.qa_stage_procedure 
        ( id, load_id, loaded )
        select 
            pot.x_srcid as id,
            pot.x_srcloadid as loadid,
            case
              when nullif(drug_concept_id, 0 ) is not null
                  then 3
              when nullif(drug_source_concept_id, 0 ) is not null
                then 2
              else 1
            end as loaded
        from omop.drug_exposure_temp pot
        join etl.stage_procedure_temp spt on pot.x_srcid = spt.id and spt.load_id = pot.x_srcloadid
        where pot.x_srcfile = 'STAGE_PROCEDURE'
        and pot.x_srcloadid = v_loadid
        ;

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_procedure_load', 'update from drug_exposure: ' , rowcnt );
        perform etl.logm('qa_procedure_load', 'update from drug_exposure' , 'FINISH' );

		    perform etl.logm('qa_procedure_load', 'update from device_exposure' , 'START' );

        -- check device exposure
        insert into etl.qa_stage_procedure 
        ( id, load_id, loaded )
        select 
            pot.x_srcid as id,
            pot.x_srcloadid as loadid,
            case
              when nullif(device_concept_id, 0 ) is not null
                  then 3
              when nullif(device_source_concept_id, 0 ) is not null
                then 2
              else 1
            end as loaded
        from omop.device_exposure_temp pot
        join etl.stage_procedure_temp spt on pot.x_srcid = spt.id and spt.load_id = pot.x_srcloadid
        where pot.x_srcfile = 'STAGE_PROCEDURE'
        and pot.x_srcloadid = v_loadid
        ;

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_procedure_load', 'update from device_exposure: ' , rowcnt );
        perform etl.logm('qa_procedure_load', 'update from device_exposure' , 'FINISH' );

-- create index for qa_stage_procedure
        drop index qastproc_idx if exists;
        create index qastproc_idx on etl.qa_stage_procedure( load_id, id );

        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg
        from 
        (
          select max(loaded) as loaded, id, load_id 
          from etl.qa_stage_procedure 
          where load_id = v_loadid
          group by id, load_id
        ) a
        where loaded = 3
        ;
        perform etl.logm('qa_procedure_load', 'QA check count: ' , logmsg );

        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg
        from 
        (
          select max(loaded) as loaded, id, load_id 
          from etl.qa_stage_procedure 
          where load_id = v_loadid
          group by id, load_id
        ) a
        where loaded = 2
        ;
        perform etl.logm('qa_procedure_load', 'QA check count: ' , logmsg );

        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg
        from 
        (
          select max(loaded) as loaded, id, load_id 
          from etl.qa_stage_procedure 
          where load_id = v_loadid
          group by id, load_id
        ) a
        where loaded = 1
        ;
        perform etl.logm('qa_procedure_load', 'QA check count: ' , logmsg );

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
      spt.id,
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
      spt.load_id,
      spt.loaded
    from etl.stage_procedure_temp spt
    left join etl.qa_stage_procedure qsc on sct.id = qsc.id and sct.load_id = qsc.load_id
    where qsc.id is null 
    and load_id = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_procedure_load', 'Error records moved to etl.stage_condition_error: ' , rowcnt );

    -- remove unmapped records that were mapped in other tables
    -- postgres performance improvement
    delete from omop.procedure_occurrence_temp po
    using etl.qa_stage_procedure sl
    where po.procedure_source_concept_id = 0
    and po.x_srcfile = 'STAGE_PROCEDURE'
    and po.x_srcid = sl.id
    and sl.loaded <> 1
    ;

/*
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
*/

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_procedure_load', 'unmapped records removed that were mapped in other tables: ' , rowcnt );


    -- move to etl stage permanent home
   insert into etl.stage_procedure
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
      sct.id,
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
      sct.load_id,
      qsa.loaded
    from etl.stage_procedure_temp sct
    join
    ( select max(loaded) as loaded, id, load_id 
        from etl.qa_stage_procedure 
        where load_id = v_loadid
        group by id, load_id
    ) qsc on sct.id = qsc.id and sct.load_id = qsc.load_id
    where load_id = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_procedure_load', 'moved from etl.stage_procedure_temp to permanent: ' , rowcnt );

    delete from etl.stage_procedure_temp
    where load_id = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_procedure_load', 'deleted from etl.stage_procedure_temp: ' , rowcnt );

    perform etl.logm('qa_procedure_load', 'process' , 'FINISH' ); 

  end;
$$
 
