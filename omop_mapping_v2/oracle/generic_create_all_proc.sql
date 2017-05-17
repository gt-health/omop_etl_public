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
CREATE OR REPLACE PROCEDURE omop.generic_create_procedures( v_loadid int )

AS

  rowcnt integer;

begin

-- procedures

-- create sequence omop.procedure_occurrence_id_seq;

etl.logm('generic_create_procedures', 'create procedure', 'START' );

INSERT INTO omop.procedure_occurrence_temp
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
  x_srcfile
)
select
  omop.procedure_occurrence_id_seq.nextval as procedure_occurrence_id,
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
  x_srcfile
from
(
    select
    	p.person_id as person_id,
    	coalesce( tar.concept_id, src.concept_id, 0 )  as procedure_concept_id,
    	coalesce( s.procedure_date, v.visit_start_date )  as procedure_date,
    	s.procedure_source_type_value as procedure_type_concept_id,
		  coalesce(mod.concept_id,0) as modifier_concept_id,
      s.quantity  as quantity,
    	v.provider_id as provider_id,
    	v.visit_occurrence_id as visit_occurrence_id,
    	s.procedure_source_value as procedure_source_value,
    	coalesce( src.concept_id, 0 ) as procedure_source_concept_id,
      s.code_modifier as qualifier_source_value
      , s.id as x_srcid
  		, s.load_id as x_srcloadid
      , 'STAGE_PROCEDURE' as x_srcfile
    from etl.stage_procedure_temp s
    join omop.person p on p.person_source_value = s.person_source_value
    left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
    left join omop.concept src on s.procedure_source_value = replace(src.concept_code, '.', '' )
        and src.domain_id like '%Proc%'
        and coalesce(s.procedure_code_source_type, src.vocabulary_id ) = src.vocabulary_id
        and src.invalid_reason is null
        and src.vocabulary_id in ( 'CPT4', 'HCPCS', 'ICD9Proc', 'ICD10PCS')
    left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
        and cr.relationship_id = 'Maps to'
        and cr.invalid_reason is null
    left join omop.concept tar on cr.concept_id_2 = tar.concept_id
        and tar.standard_concept = 'S'
        and tar.invalid_reason is null
    left join omop.concept mod on s.code_modifier = mod.concept_code
        and mod.concept_class_id like '%Modifier%'
        and mod.vocabulary_id = src.vocabulary_id
    left join omop.provider pr on s.provider_source_value = pr.provider_source_value
    where s.procedure_date is not null
and s.load_id = coalesce(v_loadid, s.load_id )

) a
;

  rowcnt := SQL%ROWCOUNT;
  etl.logm('generic_create_procedures', 'insert into procedure', rowcnt );
  etl.logm('generic_create_procedures', 'create procedure', 'FINISH' );


  etl.logm('generic_create_procedures', 'generic_create_proc_observations from procedures' , 'START' );

INSERT INTO omop.observation_temp
(
             observation_id
            , person_id
            , observation_concept_id
            , observation_date
            , observation_time
            , observation_type_concept_id
            , qualifier_concept_id
            , value_as_concept_id
            , visit_occurrence_id
            , observation_source_value
            , observation_source_concept_id
            , provider_id
            , qualifier_source_value
            , x_srcid
            , x_srcloadid
            , x_srcfile
)
        select
            omop.observation_id_seq.nextval as observation_id
            , person_id
            , observation_concept_id
            , observation_date
            , observation_time
            , observation_type_concept_id
            , qualifier_concept_id
            , value_as_concept_id
            , visit_occurrence_id
            , observation_source_value
            , observation_source_concept_id
            , provider_id
            , qualifier_source_value
            , x_srcid
            , x_srcloadid
            , x_srcfile
        from
        (
        select
            p.person_id as person_id
            , coalesce(tar.concept_id, 0 )  as observation_concept_id
            , coalesce(s.procedure_date, v.visit_start_date) as observation_date
            , extract( hour from s.procedure_date ) || ':' ||
                extract( minute from s.procedure_date ) || ':' ||
                extract( second from s.procedure_date ),
                'HH24:MI:SS' as observation_time
            , 38000280 as observation_type_concept_id  -- 'Observation recorded from EHR'  -- TODO: may need to be changed
            , coalesce(mod.concept_id,0) as qualifier_concept_id
            , val.concept_id as value_as_concept_id
            , v.visit_occurrence_id as visit_occurrence_id
            , s.procedure_source_value as observation_source_value
            , coalesce(src.concept_id, 0) as observation_source_concept_id
            , pr.provider_id
            , s.code_modifier as qualifier_source_value
            , s.id as x_srcid
            , s.load_id as x_srcloadid
            , 'STAGE_PROCEDURE' as x_srcfile
        from etl.stage_procedure_temp s
        join omop.person p on p.person_source_value = s.person_source_value
        left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
        join omop.concept src on s.procedure_source_value = replace(src.concept_code, '.', '' )
			                  and src.domain_id like '%Obs%'
                        and coalesce(s.procedure_code_source_type, src.vocabulary_id ) = src.vocabulary_id
			                  and src.invalid_reason is null
			                  and src.vocabulary_id in ( 'CPT4', 'HCPCS', 'ICD9Proc', 'ICD10PCS')
        left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
                  			and cr.relationship_id = 'Maps to'
                  			and cr.invalid_reason is null
        left join omop.concept tar on cr.concept_id_2 = tar.concept_id
                  			and tar.standard_concept = 'S'
                  			and tar.invalid_reason is null
        left join omop.concept_relationship crv on src.concept_id = crv.concept_id_1
                  			and crv.relationship_id = 'Maps to value'
                  			and crv.invalid_reason is null
        left join omop.concept val on crv.concept_id_2 = val.concept_id
                  			and val.standard_concept = 'S'
                  			and val.invalid_reason is null
        left join omop.concept mod on s.code_modifier = mod.concept_code
                        and mod.concept_class_id like '%Modifier%'
                        and mod.vocabulary_id = src.vocabulary_id
        left join omop.provider pr on s.provider_source_value = pr.provider_source_value
        where s.procedure_date is not null
and s.load_id = coalesce(v_loadid, s.load_id )

     ) a  ;

        rowcnt := SQL%ROWCOUNT;

        etl.logm('generic_create_procedures', 'insert into observation' , rowcnt );

        etl.logm('generic_create_procedures', 'generic_create_proc_observations from procedures' , 'FINISH' );




        etl.logm('generic_create_procedures', 'generic_create_proc_measurements from procedure' , 'START' );

        insert into omop.measurement_temp
        (
            measurement_id,
            person_id,
            measurement_concept_id,
            measurement_date,
            measurement_type_concept_id,
            value_as_concept_id,
            visit_occurrence_id,
            measurement_source_value,
            measurement_source_concept_id,
            provider_id,
            x_srcid,
            x_srcloadid,
            x_srcfile
        )
        select
            omop.measurement_id_seq.nextval as measurement_id
            , person_id
            , measurement_concept_id
            , measurement_date
            , measurement_type_concept_id
            , value_as_concept_id
            , visit_occurrence_id
            , measurement_source_value
            , measurement_source_concept_id
            , provider_id
            , x_srcid
            , x_srcloadid
            , x_srcfile
        from
        (
            select distinct
                p.person_id as person_id
                , coalesce(tar.concept_id, 0 )  as measurement_concept_id
                , coalesce( s.procedure_date, v.visit_start_date ) as measurement_date
                , 44818701 as measurement_type_concept_id  -- 'From physical examination' -- TODO: may need to be changed
                , val.concept_id as value_as_concept_id
                , v.visit_occurrence_id as visit_occurrence_id
                , s.procedure_source_value as measurement_source_value
                , coalesce(tar.concept_id, 0) as measurement_source_concept_id
                , pr.provider_id
                , s.id as x_srcid
                , s.load_id as x_srcloadid
                , 'STAGE_PROCEDURE' as x_srcfile
            from etl.stage_procedure_temp s
            join omop.person p on p.person_source_value = s.person_source_value
            left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
            join omop.concept src on s.procedure_source_value = replace(src.concept_code, '.', '' )
                            and src.domain_id like '%Meas%'
                            and coalesce(s.procedure_code_source_type, src.vocabulary_id ) = src.vocabulary_id
                            and src.invalid_reason is null
                            and src.vocabulary_id in ( 'CPT4', 'HCPCS', 'ICD9Proc', 'ICD10PCS')
            left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
                      			and cr.relationship_id = 'Maps to'
                      			and cr.invalid_reason is null
            left join omop.concept tar on cr.concept_id_2 = tar.concept_id
                      			and tar.standard_concept = 'S'
                      			and tar.invalid_reason is null
            left join omop.concept_relationship crv on src.concept_id = crv.concept_id_1
                      			and crv.relationship_id = 'Maps to value'
                      			and crv.invalid_reason is null
            left join omop.concept val on crv.concept_id_2 = val.concept_id
                      			and val.standard_concept = 'S'
                      			and val.invalid_reason is null
            left join omop.provider pr on s.provider_source_value = pr.provider_source_value
             where s.procedure_date is not null
and s.load_id = coalesce(v_loadid, s.load_id )

        ) a
        ;

        rowcnt := SQL%ROWCOUNT;

        etl.logm('generic_create_procedures', 'insert into measurements' , rowcnt );

        etl.logm('generic_create_procedures', 'generic_create_proc_measurements from procedure' , 'FINISH' );

      etl.logm('generic_create_procedures', 'generic_create_proc_drug_exposure from procedure' , 'START' );

      INSERT INTO omop.drug_exposure_temp
      (
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        drug_type_concept_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        drug_source_value,
        drug_source_concept_id,
        x_srcid,
        x_srcloadid,
        x_srcfile
      )
      select
        omop.drug_exposure_id_seq.nextval as 	drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        drug_type_concept_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        drug_source_value,
        drug_source_concept_id,
        x_srcid,
        x_srcloadid,
        x_srcfile
      from
      (
          select
          	p.person_id as person_id,
          	coalesce(tar.concept_id, 0 ) as drug_concept_id,
          	coalesce( s.procedure_date, v.visit_start_date )   as drug_exposure_start_date,
          	38000179 as drug_type_concept_id,   -- physician administered as procedure code
          	s.quantity as quantity,
          	coalesce( pr.provider_id, 0 )  as provider_id,
          	v.visit_occurrence_id as visit_occurrence_id,
          	s.procedure_source_value as drug_source_value,
          	coalesce( src.concept_id, 0) as drug_source_concept_id,
            s.id as x_srcid,
            s.load_id as x_srcloadid,
            'STAGE_PROCEDURE' as x_srcfile
          from etl.stage_procedure_temp s
          join omop.person p on p.person_source_value = s.person_source_value
          left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
           join omop.concept src on s.procedure_source_value = replace(src.concept_code, '.', '' )
              and src.domain_id like '%Drug%'
              and coalesce(s.procedure_code_source_type, src.vocabulary_id ) = src.vocabulary_id
              and src.invalid_reason is null
              and src.vocabulary_id in ( 'CPT4', 'HCPCS', 'ICD9Proc', 'ICD10PCS')
          left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
              and cr.relationship_id = 'Maps to'
              and cr.invalid_reason is null
          left join omop.concept tar on cr.concept_id_2 = tar.concept_id
              and tar.standard_concept = 'S'
              and tar.invalid_reason is null
          left join omop.provider pr on s.provider_source_value = pr.provider_source_value
--          where s.procedure_date is not null
where s.load_id = coalesce(v_loadid, s.load_id )

      ) a
      ;

        rowcnt := SQL%ROWCOUNT;

        etl.logm('generic_create_procedures', 'insert into drug_exposure' , rowcnt );

        etl.logm('generic_create_procedures', 'generic_create_proc_drug_exposure from procedure' , 'FINISH' );


      etl.logm('generic_create_procedures', 'generic_create_proc_device from procedure' , 'START' );

      INSERT INTO omop.device_exposure_temp
      (
          device_exposure_id,
          person_id,
          device_concept_id,
          device_exposure_start_date,
          device_type_concept_id,
          quantity,
          provider_id,
          visit_occurrence_id,
          device_source_value,
          device_source_concept_id,
          x_srcid,
          x_srcloadid,
          x_srcfile
      )
      select
          omop.device_exposure_id_seq.nextval as device_exposure_id,
          person_id,
          device_concept_id,
          device_exposure_start_date,
          device_type_concept_id,
          quantity,
          provider_id,
          visit_occurrence_id,
          device_source_value,
          device_source_concept_id,
          x_srcid,
          x_srcloadid,
          x_srcfile
      from
      (
          select
          	p.person_id as person_id,
          	coalesce(tar.concept_id, 0 ) as device_concept_id,
          	coalesce( s.procedure_date, v.visit_start_date )   as device_exposure_start_date,
          	44818705 as device_type_concept_id,   -- inferred from procedure code
          	s.quantity as quantity,
          	coalesce( pr.provider_id, 0 )  as provider_id,
          	v.visit_occurrence_id as visit_occurrence_id,
          	s.procedure_source_value as device_source_value,
          	coalesce( src.concept_id, 0) as device_source_concept_id,
             s.id as x_srcid,
             s.load_id as x_srcloadid,
            'STAGE_PROCEDURE' as x_srcfile
          from etl.stage_procedure_temp s
          join omop.person p on p.person_source_value = s.person_source_value
          left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
           join omop.concept src on s.procedure_source_value = replace(src.concept_code, '.', '' )
              and src.domain_id like '%Dev%'
              and coalesce(s.procedure_code_source_type, src.vocabulary_id ) = src.vocabulary_id
              and src.invalid_reason is null
              and src.vocabulary_id in ( 'CPT4', 'HCPCS', 'ICD9Proc', 'ICD10PCS')
          left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
              and cr.relationship_id = 'Maps to'
              and cr.invalid_reason is null
          left join omop.concept tar on cr.concept_id_2 = tar.concept_id
              and tar.standard_concept = 'S'
              and tar.invalid_reason is null
          left join omop.provider pr on s.provider_source_value = pr.provider_source_value
  --        where s.procedure_date is not null
where s.load_id = coalesce(v_loadid, s.load_id )

      ) a
      ;

        rowcnt := SQL%ROWCOUNT;

        etl.logm('generic_create_procedures', 'insert into device_exposure' , rowcnt );

        etl.logm('generic_create_procedures', 'generic_create_proc_device_exposure from procedure' , 'FINISH' );


      etl.logm('generic_create_procedures', 'create procedure', 'STOP' );



    end;


