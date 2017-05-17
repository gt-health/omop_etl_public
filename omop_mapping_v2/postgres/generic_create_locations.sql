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
-- create locations
-- this creates location for patient, providers and caresites at the same time to avoid duplicates.
-- if it is broken apart for performance reasons, deduplication will need to be considered.
-- later matches to get location id will use the concatenated location_source_value to match locations
-- obviously, the raw column and table names will need to be modified and columns may need to be removed.
CREATE OR REPLACE
function omop.generic_create_locations (  p_loadid INT )
returns void
language plpgsql

AS
$$

declare
  rowcnt INTEGER;
  v_loadid integer;

BEGIN

  v_loadid := p_loadid;
  perform etl.logm('generic_create_locations', 'generic_create_locations loadid: '||v_loadid , 'START' );
  
  INSERT
  INTO omop.location
    (
      location_id ,
      address_1 ,
      address_2 ,
      city ,
      state ,
      zip ,
      county ,
      location_source_value
    )
  SELECT 
    nextval('omop.location_id_seq') AS location_id ,
    address_1                            AS address_1 ,
    address_2                            AS address_2 ,
    city                                 AS city ,
    state                                AS state ,
    zip                                  AS zip ,
    county                               AS county ,
    location_source_value                AS location_source_value
  FROM
    (SELECT MIN(a.address_1) AS address_1 ,
      MIN(a.address_2)       AS address_2 ,
      MIN(a.city)            AS city ,
      MIN(a.state)           AS state ,
      MIN(a.zip)             AS zip, -- substr( zip, 1, 3 ) to get zip_3 for deid purposes, if needed
      MIN(a.county) AS county ,
      a.location_source_value
    FROM
      ( SELECT DISTINCT address_1 AS address_1 ,
        address_2                 AS address_2 ,
        city                      AS city ,
        state                     AS state ,
        zip                       AS zip, -- substr( zip, 1, 3 ) to get zip_3 for deid purposes, if needed
        county AS county ,
        location_source_value
      FROM etl.stage_person p
      WHERE p.load_id = v_loadid
      
      UNION
      
      SELECT DISTINCT address_1 AS address_1 ,
        address_2               AS address_2 ,
        city                    AS city ,
        state                   AS state ,
        zip                     AS zip, -- substr( zip, 1, 3 ) to get zip_3 for deid purposes, if needed
        county AS county ,
        location_source_value
      FROM etl.stage_provider pr
      WHERE pr.load_id = v_loadid
      
      UNION
      
      SELECT DISTINCT address_1 AS address_1 ,
        address_2               AS address_2 ,
        city                    AS city ,
        state                   AS state ,
        zip                     AS zip, -- substr( zip, 1, 3 ) to get zip_3 for deid purposes, if needed
        county AS county ,
        location_source_value
      FROM etl.stage_care_site cs
      WHERE cs.load_id = v_loadid
      ) a
    LEFT JOIN omop.location loc
    ON a.location_source_value   = loc.location_source_value
    WHERE loc.location_id       IS NULL
    AND a.location_source_value IS NOT NULL
    GROUP BY a.location_source_value
    ) b ;
  
  get diagnostics rowcnt = ROW_COUNT;
  
  perform etl.logm('generic_create_locations', 'insert into location' , rowcnt );
  perform etl.logm('generic_create_locations', 'generic_create_locations' , 'FINISH' );
  
END;
$$
