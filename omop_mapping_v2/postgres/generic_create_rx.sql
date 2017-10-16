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
CREATE OR REPLACE function omop.generic_create_drug_exposures( p_loadid int)
returns void
language plpgsql
AS 
$$

declare
  rowcnt integer;
  v_loadid integer;

begin

-- drug exposure from pharmacy
 v_loadid := p_loadid;
 perform etl.logm('generic_create_drug_exposures', 'create drug_exposures loadid: '||v_loadid, 'START' );

INSERT INTO omop.drug_exposure_temp
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
--  sig,
  route_concept_id,
  effective_drug_dose,
  dose_unit_concept_id,
--  lot_number,
  provider_id,
  visit_occurrence_id,
  drug_source_value,
  drug_source_concept_id,
  route_source_value,
  dose_unit_source_value,
  x_srcid,
  x_srcloadid,
  x_srcfile
)
select
  nextval('omop.drug_exposure_id_seq') as 	drug_exposure_id,
  person_id,
  drug_concept_id,
  drug_exposure_start_date,
  drug_exposure_end_date,
  drug_type_concept_id,
  stop_reason,
  refills,
  quantity,
  days_supply,
--  sig,
  route_concept_id,
  effective_drug_dose,
  dose_unit_concept_id,
--  lot_number,
  provider_id,
  visit_occurrence_id,
  drug_source_value,
  drug_source_concept_id,
  route_source_value,
  dose_unit_source_value,
  x_srcid,
  x_srcloadid,
  x_srcfile
from
(
    select
    	p.person_id as person_id,
    	coalesce(src.tar_concept_id, 0 ) as drug_concept_id,
    	s.drug_start_date  as drug_exposure_start_date,
    	s.drug_end_date as  drug_exposure_end_date,
    	coalesce(cast(s.drug_source_type_value as int),0) as drug_type_concept_id,
    	s.stop_reason as stop_reason,
    	s.refills as refills,
    	s.quantity as quantity,
    	s.days_supply as days_supply,
    	coalesce( rte.concept_id, 0 ) as route_concept_id,
    	s.effective_drug_dose as effective_drug_dose,
    	coalesce( dose.concept_id, 0 ) as dose_unit_concept_id,
    	coalesce( pr.provider_id, 0 )  as provider_id,
    	v.visit_occurrence_id as visit_occurrence_id,
    	s.drug_source_value as drug_source_value,
    	coalesce( src.src_concept_id, 0) as drug_source_concept_id,
    	s.route_source_value as route_source_value,
      s.dose_unit_source_value as dose_unit_source_value
      , s.id as x_srcid
      , s.load_id as x_srcloadid
      , 'STAGE_RX' as x_srcfile
    from etl.stage_rx_temp s
    join omop.person p on p.person_source_value = s.person_source_value
    left join omop.visit_occurrence v on s.visit_source_value = v.visit_source_value
    left join omop.concept_drug src on s.drug_source_value = src.clean_concept_code
        and coalesce(s.drug_source_type, src.src_vocabulary_id ) = src.src_vocabulary_id
    left join omop.concept dose on s.dose_unit_source_value = dose.concept_code    ---should be for effective_drug_dose
        and dose.domain_id = 'Unit'
        and dose.invalid_reason is null
        and dose.standard_concept = 'S'
    left join omop.concept rte on s.route_source_value = rte.concept_code   -- SNOMED Number
        and rte.domain_id = 'Route'
        and rte.invalid_reason is null
        and rte.standard_concept = 'S'
    left join omop.provider pr on s.provider_source_value = pr.provider_source_value
    where s.drug_start_date is not null
    and s.load_id = v_loadid
) a
;

  get diagnostics rowcnt = ROW_COUNT;
  perform etl.logm('generic_create_drug_exposures', 'insert into drug_exposure', rowcnt );
  perform etl.logm('generic_create_drug_exposures', 'create drug_exposure', 'FINISH' );

end;
$$
