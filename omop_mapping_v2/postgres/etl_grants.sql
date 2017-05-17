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
grant select, delete, insert, update  on etl.stage_condition to omop  ;
grant select, delete, insert, update on etl.stage_condition_error to omop   ;
grant select, delete, insert, update on etl.stage_condition_temp  to omop  ;

grant select, delete, insert, update  on etl.stage_procedure  to omop  ;
grant select, delete, insert, update  on etl.stage_procedure_error  to omop  ;
grant select, delete, insert, update  on etl.stage_procedure_temp  to omop  ;

grant select, delete, insert, update  on etl.stage_lab to omop   ;
grant select, delete, insert, update on etl.stage_lab_error to omop   ;
grant select, delete, insert, update on etl.stage_lab_temp to omop   ;

grant select, delete, insert, update on etl.stage_rx to omop   ;
grant select, delete, insert, update  on etl.stage_rx_error to omop   ;
grant select, delete, insert, update  on etl.stage_rx_temp to omop   ;

grant select, delete, insert, update  on etl.stage_person to omop   ;
grant select, delete, insert, update  on etl.stage_care_site to omop   ;
grant select, delete, insert, update on etl.stage_death to omop   ;

grant select, delete, insert, update  on etl.stage_provider to omop   ;
grant select, delete, insert, update  on etl.stage_visit  to omop  ;
