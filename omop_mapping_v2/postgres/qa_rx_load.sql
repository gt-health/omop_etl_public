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
create or replace function omop.qa_rx_load( p_loadid integer )
  RETURNS void
  LANGUAGE plpgsql
AS
$$

  declare
	rowcnt integer;
    v_loadid integer;
    logmsg varchar(200);

  begin

-- -- 0 = not loaded, 1 = loaded, but not matched concept, 2 = loaded matched source concept, 3 = loaded matched target concept
-- create unique index etl_stage_cond_pk on etl.stage_rx( id );

        v_loadid := p_loadid;
        perform etl.logm('qa_rx_load', 'process loadid:'||v_loadid , 'START' ); 

        drop index if exists etl.etl_stage_rx_id;
        create index etl_stage_rx_id on etl.stage_rx_temp( id, load_id );
        
        perform etl.logm('qa_rx_load', 'update from procedures' , 'START' ); 
        
        -- check procedure_occurrence
        update etl.stage_rx_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(procedure_concept_id, 0 ) is not null
                  then 3
                when nullif(procedure_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
              x_srcloadid as loadid
            from omop.procedure_occurrence_temp
            where x_srcfile = 'STAGE_RX'
            and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
            and load_id = b.loadid
         ;

		get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_rx_load', 'update from procedures: ' , rowcnt ); 
        perform etl.logm('qa_rx_load', 'update from procedures' , 'FINISH' ); 
        
        perform etl.logm('qa_rx_load', 'update from measurements' , 'START' );  
        -- check measurement

        update etl.stage_rx_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(measurement_concept_id, 0 ) is not null
                  then 3
                when nullif(measurement_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
              x_srcloadid as loadid
            from omop.measurement_temp
            where x_srcfile = 'STAGE_RX'
			and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded  < b.loaded
			and load_id = b.loadid
         ;
 
		get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_rx_load', 'update from measurements: ' , rowcnt ); 
        perform etl.logm('qa_rx_load', 'update from measurements' , 'FINISH' ); 
        
        perform etl.logm('qa_rx_load', 'update from observations' , 'START' ); 
                
        -- check observation
        update etl.stage_rx_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(observation_concept_id, 0 ) is not null
                  then 3
                when nullif(observation_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.observation_temp
            where x_srcfile = 'STAGE_RX'
			and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded  < b.loaded
			and load_id = b.loadid
         ;
 
		get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_rx_load', 'update from observations' , 'FINISH' );  
        
        perform etl.logm('qa_rx_load', 'update from condition' , 'START' );  
        -- check procedure
        update etl.stage_rx_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(condition_concept_id, 0 ) is not null
                  then 3
                when nullif(condition_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.condition_occurrence_temp
            where x_srcfile = 'STAGE_RX'
			and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
			and load_id = b.loadid
         ;

		get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_rx_load', 'update from condition: ' , rowcnt );  
        perform etl.logm('qa_rx_load', 'update from condition' , 'FINISH' );  

        perform etl.logm('qa_rx_load', 'update from rx' , 'START' );  

        -- check procedure
        update etl.stage_rx_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(drug_concept_id, 0 ) is not null
                  then 3
                when nullif(drug_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.drug_exposure_temp
            where x_srcfile = 'STAGE_RX'
			and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
			and load_id = b.loadid
         ;

		get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_rx_load', 'update from rx: ' , rowcnt );  
        perform etl.logm('qa_rx_load', 'update from rx' , 'FINISH' );  
        
        perform etl.logm('qa_rx_load', 'update from device' , 'START' );

        -- check procedure
        update etl.stage_rx_temp sc
        set loaded = b.loaded
        from
        (
           select
              case
                when nullif(device_concept_id, 0 ) is not null
                  then 3
                when nullif(device_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.device_exposure_temp
            where x_srcfile = 'STAGE_RX'
			and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded  < b.loaded
			and load_id = b.loadid
         ;

		get diagnostics rowcnt = ROW_COUNt;
        perform etl.logm('qa_rx_load', 'update from device: ' , rowcnt );
        perform etl.logm('qa_rx_load', 'update from device' , 'FINISH' );


    perform etl.logm('qa_rx_load', 'update from device_exposure' , 'FINISH' );

    select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg from etl.stage_rx_temp
    where loaded = 3;
    perform etl.logm('qa_rx_load', 'QA check count: ' , logmsg );

    select 'Loadid: '||v_loadid||', loaded = 2:' || count(*) into logmsg from etl.stage_rx_temp
    where loaded = 2;
    perform etl.logm('qa_rx_load', 'QA check count: ' , logmsg );

    select 'Loadid: '||v_loadid||', loaded = 1:' || count(*) into logmsg from etl.stage_rx_temp
    where loaded = 1;
    perform etl.logm('qa_rx_load', 'QA check count: ' , logmsg );

    select 'Loadid: '||v_loadid||', loaded = 0:' || count(*) into logmsg from etl.stage_rx_temp
    where loaded = 0;
    perform etl.logm('qa_rx_load', 'QA check count: ' , logmsg );


    -- copy records that did not make it over to error

    insert into etl.stage_rx_error
    (
        id,
        drug_source_type,
        drug_source_value,
        drug_source_type_value,
        drug_start_date,
        drug_end_date,
        stop_reason,
        refills,
        quantity,
        days_supply,
        dose_unit_source_value,
        effective_drug_dose,
        total_charge,
        total_cost,
        total_paid,
        paid_by_payer,
        paid_by_patient,
        paid_patient_copay,
        paid_patient_coinsurance,
        paid_patient_deductible,
        paid_by_primary,
        paid_ingredient_cost,
        pait_dispensing_fee,
        route_source_value,
        visit_source_value,
        person_source_value,
        provider_source_value,
        load_id,
        loaded
    )
   select
      id,
      drug_source_type,
      drug_source_value,
      drug_source_type_value,
      drug_start_date,
      drug_end_date,
      stop_reason,
      refills,
      quantity,
      days_supply,
      dose_unit_source_value,
      effective_drug_dose,
      total_charge,
      total_cost,
      total_paid,
      paid_by_payer,
      paid_by_patient,
      paid_patient_copay,
      paid_patient_coinsurance,
      paid_patient_deductible,
      paid_by_primary,
      paid_ingredient_cost,
      pait_dispensing_fee,
      route_source_value,
      visit_source_value,
      person_source_value,
      provider_source_value,
      load_id,
      loaded
    from etl.stage_rx_temp
    where loaded = 0
    and load_id = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_rx_load', 'Error records moved to etl.stage_condition_error: ' , rowcnt );

    -- remove unmapped records that were mapped in other tables
        -- postgres performance improvement
        delete from omop.drug_exposure_temp co
        using etl.stage_rx_temp sl
        where co.drug_source_concept_id = 0
        and co.x_srcfile = 'STAGE_RX'
        and co.x_srcid = sl.id
        and sl.loaded <> 1
        ;
        /*  generic subselect method for other DBs.

    delete from omop.drug_exposure_temp de
    where de.x_srcid in
    (
        select id
        from etl.stage_rx_temp rx
        join omop.drug_exposure_temp de2 on ( rx.id = de2.x_srcid and rx.load_id = de2.x_srcloadid )
        where rx.loaded >1
        and de2.drug_source_concept_id = 0
        and de2.x_srcfile = 'STAGE_RX'
        and rx.load_id = v_loadid
    )
    and de.x_srcfile = 'STAGE_RX'
    and de.x_srcloadid = v_loadid
    ;
*/

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_rx_load', 'unmapped records removed that were mapped in other tables: ' , rowcnt );


    -- move to etl stage permanent home
    insert into etl.stage_rx
    (
        id,
        drug_source_type,
        drug_source_value,
        drug_source_type_value,
        drug_start_date,
        drug_end_date,
        stop_reason,
        refills,
        quantity,
        days_supply,
        dose_unit_source_value,
        effective_drug_dose,
        total_charge,
        total_cost,
        total_paid,
        paid_by_payer,
        paid_by_patient,
        paid_patient_copay,
        paid_patient_coinsurance,
        paid_patient_deductible,
        paid_by_primary,
        paid_ingredient_cost,
        pait_dispensing_fee,
        route_source_value,
        visit_source_value,
        person_source_value,
        provider_source_value,
        load_id,
        loaded
    )
    select
      id,
      drug_source_type,
      drug_source_value,
      drug_source_type_value,
      drug_start_date,
      drug_end_date,
      stop_reason,
      refills,
      quantity,
      days_supply,
      dose_unit_source_value,
      effective_drug_dose,
      total_charge,
      total_cost,
      total_paid,
      paid_by_payer,
      paid_by_patient,
      paid_patient_copay,
      paid_patient_coinsurance,
      paid_patient_deductible,
      paid_by_primary,
      paid_ingredient_cost,
      pait_dispensing_fee,
      route_source_value,
      visit_source_value,
      person_source_value,
      provider_source_value,
      load_id,
      loaded
    from etl.stage_rx_temp
    where load_id = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_rx_load', 'moved from etl.stage_rx_temp to permanent: ' , rowcnt );

    delete from etl.stage_rx_temp
    where load_id = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('qa_rx_load', 'deleted from etl.stage_rx_temp: ' , rowcnt );


    perform etl.logm('qa_rx_load', 'process' , 'FINISH' ); 

  end;
$$
 
