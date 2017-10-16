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

create table if not exists etl.load_info
(
    load_id serial,
    load_name       varchar(100),
    load_description    varchar(1000),
    status          int
);

create or replace function etl.start_etl_load( p_load_name varchar(100), p_load_description varchar(1000))
  returns integer
  language plpgsql
as
$body$
begin
  insert into etl.load_info( load_id, load_name, load_description, status )
    values( default, p_load_name, p_load_description, 0)
  returning load_id;

end;
$body$
;


CREATE OR REPLACE FUNCTION etl.startfile(filename character varying, load_id integer)
  RETURNS void
  LANGUAGE plpgsql
AS
$body$
BEGIN

   INSERT INTO etl.file_load (filename, load_id )
        VALUES (filename, load_id);
END;
$body$
;

create table if not exists etl.logmessage
(
  msg_id serial,
  logtime timestamp default clock_timestamp(),
  process varchar(50),
  step  varchar(200),
  details varchar(200)
);

CREATE OR REPLACE FUNCTION etl.logm(process character varying, step character varying, details integer)
  RETURNS void
  LANGUAGE plpgsql
AS
$body$
BEGIN

   INSERT INTO etl.logmessage (process, step, details)
        VALUES (process, step, details);
END;
$body$
;


CREATE OR REPLACE FUNCTION etl.logm(process character varying, step character varying, details character varying)
  RETURNS void
  LANGUAGE plpgsql
AS
$body$
BEGIN

   INSERT INTO etl.logmessage (process, step, details)
        VALUES (process, step, details);
END;
$body$
 ;

