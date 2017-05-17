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


CREATE OR REPLACE FUNCTION etl.mimic_stage_insert()
  RETURNS void
  LANGUAGE plpgsql
AS
$body$
declare
        rowcnt  integer;

begin
      

      perform etl.logm('MIMIC_STAGE_INSERT', 'mimic_stage_insert' , 'START' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Person' , 'START' );

      INSERT INTO etl.stage_person
      (
        person_source_value,
        gender,
        year_of_birth,
        month_of_birth,
        day_of_birth,
        gender_source_value,
        ethnicity_source_value,
        ethnicity,
        race_source_value,
        race
      )
      select 
        a.person_source_value as person_source_value,
        a.gender as gender,
        extract('year' from a.dob ) as year_of_birth,
        extract( 'month' from a.dob ) as month_of_birth,
        extract( 'day' from a.dob ) as day_of_birth,
        a.gender as gender_source_value,
        a.ethnicity_source_value as  ethnicity_source_value,
        case 
          when position('HISPANIC' in ethnicity_source_value) > 0 then 'Hispanic'
          else null
        end as ethnicity,
        a.ethnicity_source_value as race_source_value,
        case 
          when position('WHITE' in ethnicity_source_value) > 0 then 'White' 
          when position('BLACK' in ethnicity_source_value) > 0 then 'Black'
          when position('ASIAN' in ethnicity_source_value) > 0 then 'Asian'
          when position('ALASKA' in ethnicity_source_value) > 0 then 'American Indian or Alaska Native'
          when position('HAWAII' in ethnicity_source_value) > 0 then 'Native Hawaiian or Other Pacific Islander'
          when position('HISPANIC' in ethnicity_source_value) > 0 then 'White'
          when position('MULTI' in ethnicity_source_value) > 0 then 'Other'
          else null
        end as race
        from 
        (
          select 
            p.subject_id as person_source_value,
            p.gender as gender,
            p.dob as dob,
            p.gender as gender_source_value,
            min(substr(a.ethnicity, 1, 50 ) ) as  ethnicity_source_value 
          from mimiciii.patients p
          left join mimiciii.admissions a on p.subject_id = a.subject_id
          group by p.subject_id, p.gender, p.dob
        ) a
      ;

        
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Person' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Person' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Death' , 'START' );

      INSERT INTO etl.stage_death
      (
        person_source_value,
        dod,
        death_type_source_value
      )
      select 
        p.subject_id as person_source_value,
        p.dod,
        case 
          when p.dod_hosp is null 
            then 'SSN' 
          else 'ETL_DISCHARGE'
        end as death_type_source_value  --  death_type_source_value   'SSN', 'ETL_DISCHARGE'
      from mimiciii.patients p
      where dod is not null;
        
        
        
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Death' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Death' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Provider' , 'START' );


      INSERT INTO etl.stage_provider
      (

        specialty_source_value,
        provider_source_value
      )
      select
        coalesce( nullif(description, ''), label ) as specialty_source_value,
        cgid as provider_source_value
      from mimiciii.caregivers;

        
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Provider' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Provider' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Visit' , 'START' );


      INSERT INTO etl.stage_visit
      (
        visit_source_value,
        visit_type,
        visit_source_type_value,
        visit_start_date,
        visit_end_date,
        person_source_value
      )
      select
        hadm_id as visit_source_value,
        case admission_type 
          when 'EMERGENCY' then 'ER'
          else 'IP'
        end as visit_type,  -- ELECTIVE, URGENT, NEWBORN, EMERGENCY.   all are admissions/inpatient
        'EHR' as visit_source_type_value,
        admittime as visit_start_date,
        dischtime as visit_end_date,
        subject_id as person_source_value
      from mimiciii.admissions;

        
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Visit' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Visit' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Condition from diagnosis_icd' , 'START' );

      -- from diagnosis_icd
      INSERT INTO etl.stage_condition
      (
        condition_code_source_type,
        condition_source_value,
        condition_source_type_value,
        start_date,
        end_date,
        visit_source_value,
        person_source_value
      )
      select
        'ICD9CM' as condition_code_source_type,
        d.icd9_code as condition_source_value,
        case 
        	when coalesce(d.seq_num, 16) > 15
        		then 38000198
        	else d.seq_num + 38000182 
      	end as condition_source_type_value,
        a.admittime as start_date,
        a.dischtime as end_date,
        d.hadm_id as visit_source_value,
        d.subject_id as person_source_value
      from mimiciii.diagnoses_icd d
      join mimiciii.admissions a on d.hadm_id = a.hadm_id;
      
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Condition from diagnosis_icd' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Condition from diagnosis_icd' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Procedure from procedures_icd' , 'START' );

      -- from procedures_icd
      INSERT INTO etl.stage_procedure
      (
        procedure_code_source_type,
        procedure_source_value,
        procedure_source_type_value,
        procedure_date,
        visit_source_value,
        person_source_value
      )
      select
        null as procedure_code_source_type,
        d.icd9_code as procedure_source_value,
        case 
        	when coalesce(d.seq_num, 16) > 15
        		then 38000265
        	else d.seq_num + 38000248 
        end as procedure_source_type_value,
        a.admittime as procedure_date,
        a.hadm_id as visit_source_value,
        d.subject_id as person_source_value
       from mimiciii.procedures_icd d
       join mimiciii.admissions a on d.hadm_id = a.hadm_id;
      

      
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Procedure from procedures_icd' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Procedure from procedures_icd' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Procedure from cptevents' , 'START' );

      -- from cptevents
      INSERT INTO etl.stage_procedure
      (
        procedure_code_source_type,
        procedure_source_value,
        procedure_source_type_value,
        procedure_date,
        visit_source_value,
        person_source_value
      )
      select
        'CPT4' as procedure_code_source_type,
        d.cpt_cd as procedure_source_value,
        case 
        	when coalesce(d.ticket_id_seq, 16) > 15
        		then 38000265
        	else d.ticket_id_seq + 38000248 
        end as procedure_source_type_value,
        d.chartdate as procedure_date,
        d.hadm_id as visit_source_value,
        d.subject_id as person_source_value
       from mimiciii.cptevents d;
      

      
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Procedure from cptevents' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Procedure from cptevents' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Rx' , 'START' );
      

      INSERT INTO etl.stage_rx
      (
        drug_source_type,
        drug_source_value,
        drug_start_date,
        drug_end_date,
        stop_reason,
        refills,
        quantity,
        days_supply,
        dose_unit_source_value,
        effective_drug_dose,
        route_source_value,
        visit_source_value,
        person_source_value
      )
      select 
        'NDC' as drug_source_type,
        d.ndc as drug_source_value,
        d.startdate as drug_start_date,
        d.enddate as drug_end_date,
        null as stop_reason,
        null as refills,
        case when etl.isnumeric(d.form_val_disp)
          then cast( d.form_val_disp as numeric )
         else
          null
        end  as quantity,
        null asdays_supply,
        d.dose_unit_rx as dose_unit_source_value,
        null aseffective_drug_dose,
        d.route as route_source_value,
        d.hadm_id as visit_source_value,
        d.subject_id as person_source_value
       from 
          mimiciii.prescriptions d;
 
      
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into Rx' , rowcnt );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Rx' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'Lab' , 'START' );   

/*
      INSERT INTO etl.stage_lab
      (
        measurement_source_type,
        measurement_source_value,
        measurement_date,
        operator_source_value,
        unit_source_value,
        value_source_value,
        value_as_number,
        value_as_string,
        range_low,
        range_high,
        visit_source_value,
        person_source_value
      )
      select
        'LOINC' as measurement_source_type,
        substr(coalesce( li.loinc_code, li.label ), 1, 50 ) as measurement_source_value,
        d. charttime as measurement_date,
        null as operator_source_value,
        d.valueuom as unit_source_value,
        substr(d.value, 1, 50 ) as value_source_value,
        case when etl.isnumeric(d.value)
          then cast( d.value as numeric )
         else
          null
         end
         as value_as_number,
        null as value_as_string,
        null as range_low,
        null as range_high,
        d.hadm_id as visit_source_value,
        d.subject_id as person_source_value
       from 
        mimiciii.labevents d
        left join mimiciii.d_labitems li on d.itemid = li.itemid
      ;

      
      get diagnostics rowcnt = ROW_COUNT;
      perform etl.logm('MIMIC_STAGE_INSERT', 'insert into lab' , rowcnt );
*/
      perform etl.logm('MIMIC_STAGE_INSERT', 'Lab' , 'END' );

      perform etl.logm('MIMIC_STAGE_INSERT', 'mimic_stage_insert ' , 'END' );


end;
$body$
 VOLATILE
 COST 100;

COMMIT;
