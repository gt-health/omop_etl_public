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


drop table if exists omop.concept_condition;
create table omop.concept_condition as
select * from
(
  select
    src.concept_code as raw_concept_code,
    replace(src.concept_code, '.', '' ) as clean_concept_code,
    src.vocabulary_id as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
  from omop.concept src
  left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
      and cr.relationship_id = 'Maps to'
  left join omop.concept tar on cr.concept_id_2 = tar.concept_id
      and tar.standard_concept = 'S'
  where src.domain_id like '%Cond%'
)
a 
where rn =1
 ;

create index idx_concept_condition_raw on omop.concept_condition( raw_concept_code, src_vocabulary_id );
create index idx_concept_condition on omop.concept_condition( clean_concept_code, src_vocabulary_id );



drop table if exists omop.concept_measurement;
create table omop.concept_measurement as
select * from 
(
  select
    src.concept_code as raw_concept_code,
    replace(src.concept_code, '.', '' ) as clean_concept_code,
    src.vocabulary_id as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    val.concept_id as val_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason, val.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last , val.concept_id nulls last
                      )  as rn
  from omop.concept src
  left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
          and cr.relationship_id = 'Maps to'
  left join omop.concept tar on cr.concept_id_2 = tar.concept_id
          and tar.standard_concept = 'S'
  left join omop.concept_relationship crv on src.concept_id = crv.concept_id_1
          and crv.relationship_id = 'Maps to value'
  left join omop.concept val on crv.concept_id_2 = val.concept_id
          and val.standard_concept = 'S'
          and val.domain_id = 'Meas Value'
  where src.domain_id like '%Meas%'
) a
where rn=1
;

create index idx_concept_meas_raw on omop.concept_measurement( raw_concept_code, src_vocabulary_id );
create index idx_concept_meas on omop.concept_measurement( clean_concept_code, src_vocabulary_id );


drop table if exists omop.concept_observation;
create table omop.concept_observation as
select * from
(
  select
    src.concept_code as raw_concept_code,
    replace(src.concept_code, '.', '' ) as clean_concept_code,
    src.vocabulary_id as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    val.concept_id as val_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason, val.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last , val.concept_id nulls last
                      )  as rn
  from omop.concept src
  left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
              and cr.relationship_id = 'Maps to'
  left join omop.concept tar on cr.concept_id_2 = tar.concept_id
              and tar.standard_concept = 'S'
  left join omop.concept_relationship crv on src.concept_id = crv.concept_id_1
              and crv.relationship_id = 'Maps to value'
  left join omop.concept val on crv.concept_id_2 = val.concept_id
              and val.standard_concept = 'S'
  where src.domain_id like '%Obs%'
) a
where rn = 1
;

create index idx_concept_obs_raw on omop.concept_observation( raw_concept_code, src_vocabulary_id );
create index idx_concept_obs on omop.concept_observation( clean_concept_code, src_vocabulary_id );


drop table if exists omop.concept_procedure;
create table omop.concept_procedure as
select * from 
(
  select
    src.concept_code as raw_concept_code,
    replace(src.concept_code, '.', '' ) as clean_concept_code,
    src.vocabulary_id as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
    from omop.concept src
    left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
        and cr.relationship_id = 'Maps to'
    left join omop.concept tar on cr.concept_id_2 = tar.concept_id
        and tar.standard_concept = 'S'
    where src.domain_id like '%Proc%'
) a
where rn=1
;


create index idx_concept_proc_raw on omop.concept_procedure( raw_concept_code, src_vocabulary_id );
create index idx_concept_proc on omop.concept_procedure( clean_concept_code, src_vocabulary_id );


drop table if exists omop.concept_drug;
create table omop.concept_drug as
select * from 
(
  select 
    src.concept_code as raw_concept_code,
    replace(src.concept_code, '.', '' ) as clean_concept_code,
    src.vocabulary_id as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
  from omop.concept src
  left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
      and cr.relationship_id = 'Maps to'
  left join omop.concept tar on cr.concept_id_2 = tar.concept_id
      and tar.standard_concept = 'S'
  where src.domain_id like '%Drug%'
) a
where rn = 1
;

create index idx_concept_drug_raw on omop.concept_drug( raw_concept_code, src_vocabulary_id );
create index idx_concept_drug on omop.concept_drug( clean_concept_code, src_vocabulary_id );

drop table if exists omop.concept_device;
create table omop.concept_device as
select * from 
(
  select
    src.concept_code as raw_concept_code,
    replace(src.concept_code, '.', '' ) as clean_concept_code,
    src.vocabulary_id as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
  from omop.concept src
  left join omop.concept_relationship cr on src.concept_id = cr.concept_id_1
  and cr.relationship_id = 'Maps to'
  left join omop.concept tar on cr.concept_id_2 = tar.concept_id
  and tar.standard_concept = 'S'
  where src.domain_id like '%Dev%'
) a
where rn=1

;
create index idx_concept_dev_raw on omop.concept_device( raw_concept_code, src_vocabulary_id );
create index idx_concept_dev_proc on omop.concept_device( clean_concept_code, src_vocabulary_id );


