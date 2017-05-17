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
CREATE OR REPLACE function omop.generic_create_measurements(p_loadid int)
returns void
language plpgsql
AS 
$$

declare
  rowcnt integer;
  v_loadid  integer;

begin

    v_loadid := p_loadid; 
    perform etl.logm('generic_create_measurements', 'create measurements loadid: '||v_loadid, 'START' );
    
    INSERT INTO omop.measurement_temp
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
        x_srcfile
     )
     select
        nextval('omop.measurement_id_seq') as measurement_id,
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
        x_srcfile
      from
     (
        select
           p.person_id as person_id,
           coalesce( tar.concept_id, 0 ) as measurement_concept_id,
           s.measurement_date as measurement_date,
            trim( to_char(extract( hour from s.measurement_date ), '09') ) || ':' ||
              trim( to_char( extract( minute from s.measurement_date ), '09') ) || ':' ||
              trim( to_char(extract( second from s.measurement_date ), '09') )
           as measurement_time,
           cast( coalesce( s.measurement_source_type_value ,'44818702') as int)  as measurement_type_concept_id,  -- default "from lab result"
           coalesce( oper.concept_id, 0 ) as operator_concept_id,
           cast(s.value_as_number as numeric ) as value_as_number,
           coalesce( val.concept_id, 0 ) as value_as_concept_id,
           coalesce(tarunit.concept_id, 0) as  unit_concept_id,
           s.range_low as range_low,
           s.range_high as range_high,
           pr.provider_id as provider_id,
           v.visit_occurrence_id as visit_occurrence_id,
           s.measurement_source_value as measurement_source_value,
           coalesce( src.concept_id, 0 ) as measurement_source_concept_id,
           s.unit_source_value as unit_source_value,
           s.value_source_value as value_source_value
           , s.id as x_srcid
           , s.load_id as x_srcloadid
           , 'STAGE_LAB' as x_srcfile
        from etl.stage_lab_temp s
        join omop.person p on p.person_source_value = s.person_source_value
        left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
        left join omop.concept src on s.measurement_source_value = src.concept_code   -- loinc has dashes
            and src.domain_id like '%Meas%'
            and coalesce(s.measurement_source_type, src.vocabulary_id ) = src.vocabulary_id
            and src.invalid_reason is null
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
            and val.domain_id = 'Meas Value'
            and val.invalid_reason is null
        left join omop.concept oper on s.operator_source_value = oper.concept_code
            and oper.domain_id = 'Meas Value Operator'
            and oper.standard_concept = 'S'
            and oper.invalid_reason is null
        left join omop.concept srcunit on s.unit_source_value = srcunit.concept_code
            and srcunit.domain_id = 'Unit'
            and srcunit.invalid_reason is null
        left join omop.concept_relationship crunit on srcunit.concept_id = crunit.concept_id_1
            and crunit.relationship_id = 'Maps to'
            and crunit.invalid_reason is null
        left join omop.concept tarunit on crunit.concept_id_2 = tarunit.concept_id
            and tarunit.standard_concept = 'S'
            and tarunit.invalid_reason is null
        left join omop.provider pr on s.provider_source_value = pr.provider_source_value
        where s.load_id = v_loadid
    
      ) a
    ;
    
    
    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('generic_create_measurements', 'insert into measurements', rowcnt );
      
    insert into omop.observation_temp
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
      unit_concept_id,
      provider_id,
      visit_occurrence_id,
      observation_source_value,
      observation_source_concept_id,
      unit_source_value,
      qualifier_source_value,
      x_srcid,
      x_srcloadid,
      x_srcfile
    )     
    select 
      nextval('omop.observation_id_seq') as observation_id,
      person_id,
      observation_concept_id,
      observation_date,
      observation_time,
      observation_type_concept_id,
      value_as_number,
      value_as_string,
      value_as_concept_id,
      unit_concept_id,
      provider_id,
      visit_occurrence_id,
      observation_source_value,
      observation_source_concept_id,
      unit_source_value,
      qualifier_source_value,
      x_srcid,
      x_srcloadid,
      x_srcfile
      x_updatedate
    from
    (
      select 
          p.person_id as person_id,
          coalesce( tar.concept_id, 0 ) as observation_concept_id,
          s.measurement_date as observation_date,
          trim( to_char(extract( hour from s.measurement_date ), '09') ) || ':' ||
            trim( to_char( extract( minute from s.measurement_date ), '09') ) || ':' ||
            trim( to_char(extract( second from s.measurement_date ), '09') )
          as observation_time,
          coalesce( cast(s.measurement_source_type_value as int), 38000280 )  as observation_type_concept_id,
          cast( s.value_as_number as numeric ) as value_as_number,
          s.value_as_string as value_as_string,
          coalesce(val.concept_id,0) as value_as_concept_id,
          coalesce(tarunit.concept_id ,0) as unit_concept_id,
          pr.provider_id as provider_id,
          v.visit_occurrence_id as visit_occurrence_id,
          s.measurement_source_value as observation_source_value,
          coalesce(src.concept_id, 0) as observation_source_concept_id,
          s.unit_source_value as unit_source_value,
          null as qualifier_source_value,
          s.id as x_srcid,
          s.load_id as x_srcloadid,
          'STAGE_LAB' as x_srcfile
      from etl.stage_lab_temp s
      join omop.person p on p.person_source_value = s.person_source_value
      left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
      join omop.concept src on s.measurement_source_value = src.concept_code   -- loinc has dashes  -- measurement is the master tables, so only insert mapped concepts
          and src.domain_id like '%Obs%'
          and coalesce(s.measurement_source_type, src.vocabulary_id ) = src.vocabulary_id
          and src.invalid_reason is null
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
          and val.domain_id = 'Meas Value'
          and val.invalid_reason is null
      left join omop.concept srcunit on s.unit_source_value = srcunit.concept_code
          and srcunit.domain_id = 'Unit'
          and srcunit.invalid_reason is null
      left join omop.concept_relationship crunit on srcunit.concept_id = crunit.concept_id_1
          and crunit.relationship_id = 'Maps to'
          and crunit.invalid_reason is null
      left join omop.concept tarunit on crunit.concept_id_2 = tarunit.concept_id
          and tarunit.standard_concept = 'S'
          and tarunit.invalid_reason is null
      left join omop.provider pr on s.provider_source_value = pr.provider_source_value
      where s.load_id = v_loadid
      ) a
    ;
      
    get diagnostics rowcnt = ROW_COUNT;
    perform etl.logm('generic_create_measurements', 'insert into observations', rowcnt );

    perform etl.logm('generic_create_measurements', 'create measurements', 'FINISH' );


end;
$$
