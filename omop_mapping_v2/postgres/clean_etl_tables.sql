-- clean etl tables

-- you may want to keep the logmessage history, if not, uncomment below

-- alter sequence etl.logmessage_msg_id restart;
-- truncate table etl.logmessage ;

alter sequence etl.stage_death_id_seq restart;
alter sequence etl.stage_lab_id_seq restart;
alter sequence etl.stage_person_id_seq restart;
alter sequence etl.stage_procedure_id_seq restart;
alter sequence etl.stage_provider_id_seq restart;
alter sequence etl.stage_rx_id_seq restart;
alter sequence etl.stage_visit_id_seq restart;
alter sequence etl.stage_condition_id_seq restart;


truncate table etl.stage_condition ;
truncate table etl.stage_death ;
truncate table etl.stage_lab ;
truncate table etl.stage_person ;
truncate table etl.stage_procedure ;
truncate table etl.stage_provider ;
truncate table etl.stage_rx ;
truncate table etl.stage_visit ;