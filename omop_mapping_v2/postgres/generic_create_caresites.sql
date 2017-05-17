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

-- create care sites
-- this is very dependent on the source data setup
-- this could be in the provider record
-- or in a separate hospital location table
-- there are ways of handling hierarchies using the place of service concepts
-- but that would be implementation specific
-- if coming from different tables could prepend 'PROV'|| or 'HOSP'|| to care_site_source value to help provider joins later

-- the generic function will get the care+site from the provider table
-- performance tip.  create/update the index on the location table prior to this to speed up the location_source_value join

create or replace function omop.generic_create_caresites(p_loadid int )
returns void
language plpgsql

as
$$

declare

	rowcnt  integer;
	v_loadid integer;
	
begin
    
	v_loadid := p_loadid;
    perform etl.logm('generic_create_caresites', 'generic_create_caresites loadid:'||v_loadid , 'START' );

    insert into omop.CARE_SITE
    (
        care_site_id
        , care_site_name
		, place_of_service_concept_id
        , location_id
        , care_site_source_value
		, place_of_service_source_value
		, x_srcloadid
    )
    select 
        nextval('omop.care_site_id_seq') as care_site_id,
        hosp.care_site_name as care_site_name,
		ps.concept_id as place_of_service_concept_id,
        lo.location_id  as location_id,
        hosp.care_site_source_value,
		hosp.place_of_service_source_value,
		hosp.load_id as x_srcloadid
    from 
    (
        select distinct 
			care_site_name as care_site_name,
            care_site_source_value as care_site_source_value,
            location_source_value,
			place_of_service_source_value,
			cs.load_id
        from etl.stage_care_site cs
		where cs.load_id = v_loadid
    ) hosp
    left join omop.location lo on hosp.location_source_value = lo.location_source_value
	left join omop.care_site cs on hosp.care_site_source_value = cs.care_site_source_value
	left join omop.concept ps on hosp.place_of_service_source_value = ps.concept_code 
		and ps.domain_id = 'Place of Service' 
		and ps.standard_concept = 'S' 
		and ps.invalid_reason is null
	where cs.care_site_id is null
    ;
 
  
    get diagnostics rowcnt = ROW_COUNT;
  
      perform etl.logm('generic_create_caresites', 'insert into care site' , rowcnt );
      
      perform etl.logm('generic_create_caresites', 'generic_create_caresites' , 'FINISH' );
      
end;

$$
