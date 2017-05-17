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
    create table etl.logmessage
    (
      msg_id int,
     logtime date default sysdate,
     process varchar2(50),
     step  varchar2(200),
     details varchar2(200)
    );
    
    
   CREATE SEQUENCE  etl.SEQ_LOG_MSG_NUM;


create or replace procedure                 etl.logm( v_process varchar2, v_step varchar2, v_details varchar2 ) as
pragma autonomous_transaction;
begin
  insert into etl.logmessage
  ( msg_id, process, step, details )
  values
  ( etl.seq_log_msg_num.nextval, v_process, v_step, v_details );
  
  commit;
end;
/
grant execute on etl.logm to omop;
