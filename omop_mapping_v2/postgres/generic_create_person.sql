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

-- create person records

    create or replace function omop.generic_create_person(p_loadid int)
    returns void
    language plpgsql
    as
    $$

    declare
        rowcnt  integer;
        v_loadid integer;

    begin

        v_loadid := p_loadid;
        perform etl.logm('generic_create_person', 'generic_create_person loadid:'||v_loadid , 'START' );


        INSERT INTO omop.person
        (
            person_id,
            gender_concept_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            time_of_birth,
            race_concept_id,
            ethnicity_concept_id,
            location_id,
            provider_id,
            care_site_id,
            person_source_value,
            gender_source_value,
            gender_source_concept_id,
            race_source_value,
            race_source_concept_id,
            ethnicity_source_value,
            ethnicity_source_concept_id,
            x_srcid,
            x_srcloadid,
            x_srcfile
        )
        select
            nextval('omop.person_id_seq') as person_id,
            case pat.gender
                when 'M' then 8507
                when 'F' then 8532
                when 'A' then 8570
                when 'U' then 8551
                when 'O' then 8521
                when null then null
                else 8551
            end as gender_concept_id,
            coalesce(pat.year_of_birth,1885) as year_of_birth,
            pat.month_of_birth as month_of_birth,
            pat.day_of_birth as day_of_birth,
            pat.time_of_birth as time_of_birth,
            case pat.race
                when 'White' then 8527
                when 'Black' then 8516
                when 'Asian' then 8515
                when 'American Indian or Alaska Native' then 8657
                when 'Native Hawaiian or Other Pacific Islander' then 8557
                when 'Other' then 8522
                else 8552
            end as race_concept_id,
            case 
                when pat.ethnicity like '%Hispanic%' then 38003563
                else 38003564
            end as ethnicity_concept_id,
            loc.location_id as location_id,
            prov.provider_id as provider_id,
            coalesce( cs.care_site_id, prov.care_site_id ) as care_site_id,
            pat.person_source_value as person_source_value,
            pat.gender_source_value  as gender_source_value,
            0 as gender_source_concept_id,
            pat.race_source_value as race_source_value,
            0 as race_source_concept_id,
            pat.ethnicity_source_value as ethnicity_source_value,
            0 as ethnicity_source_concept_id
            , pat.id as x_srcid
            , pat.load_id as x_srcloadid
            , 'STAGE_PERSON' as x_srcfile
        from etl.stage_person pat
        left join omop.location loc on pat.location_source_value = loc.location_source_value
        left join omop.provider prov on pat.provider_source_value  = prov.provider_source_value
        left join omop.care_site cs on pat.care_site_source_value = cs.care_site_source_value
		left join omop.person pers on pat.person_source_value = pers.person_source_value
		where pers.person_id is null
		and pat.load_id = v_loadid
        ;


        get diagnostics rowcnt = ROW_COUNT;
          perform etl.logm('generic_create_person', 'insert into person' , rowcnt );

          perform etl.logm('generic_create_person', 'generic_create_person' , 'FINISH' );


    end;
 $$ 
