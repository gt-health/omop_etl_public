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

create or replace function omop.generic_create_providers( p_loadid int)
returns void
language plpgsql
as
$$

declare
	rowcnt  integer;
    v_loadid  integer;

begin

    v_loadid := p_loadid;
    perform etl.logm('generic_create_providers', 'generic_create_providers loadid: '||v_loadid , 'START' );

    INSERT INTO omop.provider
    (
        provider_id,
        provider_name,
        NPI,
        DEA,
        specialty_concept_id,
        care_site_id,
        year_of_birth,
        gender_concept_id,
        provider_source_value,
        specialty_source_value,
        specialty_source_concept_id,
        gender_source_value,
        gender_source_concept_id,
        x_srcid,
        x_srcloadid,
        x_srcfile
    )
    select
        nextval('omop.provider_id_seq') as provider_id,
        prov.provider_name as provider_name,
        prov.npi as NPI,
        prov.dea as DEA,
        spcon.concept_id as specialty_concept_id,
        cs.care_site_id as care_site_id,
        prov.year_of_birth as year_of_birth,
        case prov.gender
            when 'M' then 8507
            when 'F' then 8532
            when 'A' then 8570
            when 'U' then 8551
            when 'O' then 8521
            else 8551
        end as gender_concept_id,
        prov.provider_source_value as provider_source_value,  -- may need to manipulate this to get almost unique value ||'_'||npi for example
        prov.specialty_source_value as specialty_source_value,
        44819097 as specialty_source_concept_id,  -- SNOMED concept_id, for example
        prov.gender_source_value as gender_source_value,
        null as gender_source_concept_id
        , prov.id as x_srcid
        , prov.load_id as x_srcloadid
        , 'STAGE_PROVIDER' as x_srcfile
    from etl.stage_provider prov
    left join omop.care_site cs on prov.care_site_source_value = cs.care_site_source_value  -- up to individual implementation
    left join omop.concept spcon on prov.specialty_source_value = spcon.concept_code
                                                        and spcon.domain_id = 'Provider Specialty'
	left join omop.provider provide on prov.provider_source_value = provide.provider_source_value
	where provide.provider_id is null
and prov.load_id = v_loadid

    ;



      get diagnostics rowcnt = ROW_COUNT;

      perform etl.logm('generic_create_providers', 'insert into provider' , rowcnt );

      perform etl.logm('generic_create_providers', 'generic_create_providers' , 'FINISH' );


end;
$$
