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


    create or replace function omop.generic_create_visits(p_loadid int)
    returns void
    language plpgsql
 
    as
    $$

    declare
        rowcnt  integer;
        v_loadid integer;

    begin
         v_loadid := p_loadid;

         perform etl.logm('generic_create_visits', 'generic_create_visits loadid: '||v_loadid , 'START' );

            insert into omop.visit_occurrence
            (
                visit_occurrence_id,
                visit_concept_id,
                visit_source_value,
                visit_type_concept_id,
                visit_start_date,
                visit_start_time,
                visit_end_date,
                visit_end_time,
                care_site_id,
                person_id,
                provider_id,
                x_srcid,
                x_srcloadid,
                x_srcfile
            )
            select
                nextval('omop.VISIT_OCCURRENCE_ID_seq') as visit_occurrence_id
                , case vis.visit_type
                    when 'IP' then 9201
                    when 'OUT' then 9202
                    when 'ER' then 9203
                    when 'LONGTERM' then 42898160
                    else 0
                end as visit_concept_id
                , vis.visit_source_value as visit_source_value
                , case vis.visit_source_type_value
                    when 'CLAIM' then 44818517
                    when 'EHR' then 44818518
                    when 'STUDY' then 44818519
                    else 0
                   end as visit_type_concept_id
                , vis.visit_start_date as visit_start_date
                , trim( to_char( extract( hour from vis.visit_start_date ), '09') ) || ':' ||
                        trim( to_char(extract( minute from vis.visit_start_date ), '09') ) || ':' ||
                        trim( to_char(extract( second from vis.visit_start_date ), '09') )
                     as visit_start_time
                , coalesce(vis.visit_end_date, vis.visit_start_date) as visit_end_date
                ,  trim( to_char(extract( hour from vis.visit_end_date ), '09') ) || ':' ||
                       trim( to_char( extract( minute from vis.visit_end_date ), '09') ) || ':' ||
                        trim( to_char(extract( second from vis.visit_end_date ), '09') )
                as visit_end_time
                , coalesce( cs.care_site_id, pr.care_site_id ) as care_site
                , p.person_id as person_id
                , pr.provider_id as provider_id
                , vis.id as x_srcid
                , vis.load_id as x_srcloadid
                , 'STAGE_VISIT' as x_srcfile
            from etl.stage_visit vis
            join omop.person p on vis.person_source_value = p.person_source_value
            left join omop.provider pr on vis.provider_source_value = pr.provider_source_value
            left join omop.care_site cs on vis.care_site_source_value = cs.care_site_source_value
			 left join omop.visit_occurrence vo on vis.visit_source_value = vo.visit_source_value
			 where vo.visit_occurrence_id is null
       and vis.load_id = v_loadid
;

          get diagnostics rowcnt = ROW_COUNT;

          perform etl.logm('generic_create_visits', 'insert into visit_occurrence' , rowcnt );

          perform etl.logm('generic_create_visits', 'generic_create_visits' , 'FINISH' );


    end;
$$
