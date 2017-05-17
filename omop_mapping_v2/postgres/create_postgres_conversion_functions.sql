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
create or replace function etl.convert_to_integer(v_input text)
returns integer
language plpgsql
as
$$
declare v_int_value integer default null;

begin
	begin
		v_int_value := cast(v_input as int );
	exception when others then
	raise notice 'Invalid int value: "%". Returning NULL.', v_input;
	return null;
	end;
return v_int_value;
end;
$$	
;

create or replace function etl.convert_to_numeric(v_input text)
returns numeric
language plpgsql
as
$$
declare v_int_value numeric default null;

begin
	begin
		v_int_value := cast(v_input as numeric );
	exception when others then
	raise notice 'Invalid numeric value: "%". Returning NULL.', v_input;
	return null;
	end;
return v_int_value;
end;
$$
;
