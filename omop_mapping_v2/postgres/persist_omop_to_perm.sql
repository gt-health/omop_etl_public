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
function      omop.persist_omop_to_perm(p_loadid int)
returns void
language plpgsql
AS
$$

declare
  v_loadid  integer;
  logmsg  varchar(200);
  rowcnt integer;

begin

        v_loadid := p_loadid;
        perform etl.logm('persist_omop_to_perm', 'persist_omop_to_perm: load_id: '||v_loadid , 'FINISHED' );      

        -- move condition_temp to omop permanent home
        insert into omop.condition_occurrence
        (
          condition_occurrence_id,
          person_id,
          condition_concept_id,
          condition_start_date,
          condition_end_date,
          condition_type_concept_id,
          stop_reason,
          provider_id,
          visit_occurrence_id,
          condition_source_value,
          condition_source_concept_id,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        )
         select 
          condition_occurrence_id,
          person_id,
          condition_concept_id,
          condition_start_date,
          condition_end_date,
          condition_type_concept_id,
          stop_reason,
          provider_id,
          visit_occurrence_id,
          condition_source_value,
          condition_source_concept_id,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        from omop.condition_occurrence_temp 
        where x_srcloadid = v_loadid;
        
    get diagnostics rowcnt = ROW_COUNT;
         perform etl.logm('persist_omop_to_perm', 'moved from omop.condition_occurrence_temp to permanent: ' , rowcnt );

        -- move procedure_temp to omop permanent home
        insert into omop.procedure_occurrence
        (
          procedure_occurrence_id,
          person_id,
          procedure_concept_id,
          procedure_date,
          procedure_type_concept_id,
          modifier_concept_id,
          quantity,
          provider_id,
          visit_occurrence_id,
          procedure_source_value,
          procedure_source_concept_id,
          qualifier_source_value,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        )
        select 
          procedure_occurrence_id,
          person_id,
          procedure_concept_id,
          procedure_date,
          procedure_type_concept_id,
          modifier_concept_id,
          quantity,
          provider_id,
          visit_occurrence_id,
          procedure_source_value,
          procedure_source_concept_id,
          qualifier_source_value,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        from omop.procedure_occurrence_temp
        where x_srcloadid = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
         perform etl.logm('persist_omop_to_perm', 'moved from omop.procedre_occurrence_temp to permanent: ' , rowcnt );
        
        
        -- move measurement_temp to omop permanent home
        insert into omop.measurement
        (
          measurement_id,
          person_id,
          measurement_concept_id,
          measurement_date,
          measurement_time,
          measurement_type_concept_id,
          operator_concept_id,
          value_as_number,
          value_as_concept_id,
          unit_concept_id,
          range_low,
          range_high,
          provider_id,
          visit_occurrence_id,
          measurement_source_value,
          measurement_source_concept_id,
          unit_source_value,
          value_source_value,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        )
        select 
          measurement_id,
          person_id,
          measurement_concept_id,
          measurement_date,
          measurement_time,
          measurement_type_concept_id,
          operator_concept_id,
          value_as_number,
          value_as_concept_id,
          unit_concept_id,
          range_low,
          range_high,
          provider_id,
          visit_occurrence_id,
          measurement_source_value,
          measurement_source_concept_id,
          unit_source_value,
          value_source_value,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        from omop.measurement_temp 
        where x_srcloadid = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
         perform etl.logm('persist_omop_to_perm', 'moved from omop.measurement_temp to permanent: ' , rowcnt );
        
        -- move observation_temp to omop permanent home
        insert into omop.observation
        (
          observation_id,
          person_id,
          observation_concept_id,
          observation_date,
          observation_time,
          observation_type_concept_id,
          value_as_number,
          value_as_string,
          value_as_concept_id,
          qualifier_concept_id,
          unit_concept_id,
          provider_id,
          visit_occurrence_id,
          observation_source_value,
          observation_source_concept_id,
          unit_source_value,
          qualifier_source_value,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        )
        select 
          observation_id,
          person_id,
          observation_concept_id,
          observation_date,
          observation_time,
          observation_type_concept_id,
          value_as_number,
          value_as_string,
          value_as_concept_id,
          qualifier_concept_id,
          unit_concept_id,
          provider_id,
          visit_occurrence_id,
          observation_source_value,
          observation_source_concept_id,
          unit_source_value,
          qualifier_source_value,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        from omop.observation_temp
        where x_srcloadid = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
         perform etl.logm('persist_omop_to_perm', 'moved from omop.observation_temp to permanent: ' , rowcnt );
        
         -- move drug_exposure_temp to omop permanent home
        insert into omop.drug_exposure
        (
            drug_exposure_id,
            person_id,
            drug_concept_id,
            drug_exposure_start_date,
            drug_exposure_end_date,
            drug_type_concept_id,
            stop_reason,
            refills,
            quantity,
            days_supply,
            sig,
            route_concept_id,
            effective_drug_dose,
            dose_unit_concept_id,
            lot_number,
            provider_id,
            visit_occurrence_id,
            drug_source_value,
            drug_source_concept_id,
            route_source_value,
            dose_unit_source_value,
            x_srcid,
            x_srcloadid,
            x_srcfile,
            x_createdate,
            x_updatedate
          )
          select drug_exposure_id,
            person_id,
            drug_concept_id,
            drug_exposure_start_date,
            drug_exposure_end_date,
            drug_type_concept_id,
            stop_reason,
            refills,
            quantity,
            days_supply,
            sig,
            route_concept_id,
            effective_drug_dose,
            dose_unit_concept_id,
            lot_number,
            provider_id,
            visit_occurrence_id,
            drug_source_value,
            drug_source_concept_id,
            route_source_value,
            dose_unit_source_value,
            x_srcid,
            x_srcloadid,
            x_srcfile,
            x_createdate,
            x_updatedate
          from omop.drug_exposure_temp 
          where x_srcloadid = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
         perform etl.logm('persist_omop_to_perm', 'moved from omop.drug_exposure_temp to permanent: ' , rowcnt );
        
        -- move device_exposure_temp to omop permanent home
        insert into omop.device_exposure
        (
          device_exposure_id,
          person_id,
          device_concept_id,
          device_exposure_start_date,
          device_exposure_end_date,
          device_type_concept_id,
          unique_device_id,
          quantity,
          provider_id,
          visit_occurrence_id,
          device_source_value,
          device_source_concept_id,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        )
        select 
          device_exposure_id,
          person_id,
          device_concept_id,
          device_exposure_start_date,
          device_exposure_end_date,
          device_type_concept_id,
          unique_device_id,
          quantity,
          provider_id,
          visit_occurrence_id,
          device_source_value,
          device_source_concept_id,
          x_srcid,
          x_srcloadid,
          x_srcfile,
          x_createdate,
          x_updatedate
        from omop.device_exposure_temp
        where x_srcloadid = v_loadid;

    get diagnostics rowcnt = ROW_COUNT;
         perform etl.logm('persist_omop_to_perm', 'moved from omop.device_exposure_temp to permanent: ' , rowcnt );      

         perform etl.logm('persist_omop_to_perm', 'persist_omop_to_perm: ' , 'FINISHED' );      
        
end;
$$
