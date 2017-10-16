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
create or replace function omop.qa_condition_load(p_loadid int)
  RETURNS void
  LANGUAGE plpgsql
AS
$$

  declare 
    rowcnt integer;
    v_loadid  integer;
    logmsg varchar(200); 

  begin

-- -- 0 = not loaded, 1 = loaded, but not matched concept, 2 = loaded matched source concept, 3 = loaded matched target concept

        v_loadid := p_loadid;

        perform etl.logm('qa_condition_load', 'process loadid '||v_loadid , 'START' ); 
        
        drop index if exists etl.etl_stage_cond_id; 
        create index etl_stage_cond_id on etl.stage_condition_temp( id, load_id );

        perform etl.logm('qa_condition_load', 'update from conditions' , 'START' ); 


-- create new table for qa

/*
create table etl.qa_stage_condition
(
  id bigint,
  load_id int,
  loaded int
);
*/



-- insert records into qa table.  
-- join for inserts table max(loaded) group by id, load_id
-- need to left join to pick up '0' records


        -- check condition_occurrence

insert into etl.qa_stage_condition 
( id, load_id, loaded )
select 
  cot.x_srcid as id,
  cot.x_srcloadid as load_id, 
  case
    when nullif(cot.condition_concept_id, 0 ) is not null
      then 3
    when nullif(cot.condition_source_concept_id, 0 ) is not null
      then 2
    else 1
  end as loaded
from omop.condition_occurrence_temp cot
join etl.stage_condition_temp sct on cot.x_srcid = sct.id and sct.load_id = cot.x_srcloadid
where cot.x_srcfile = 'STAGE_CONDITION'
and cot.x_srcloadid = v_loadid
;



/*
        update etl.stage_condition_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(condition_concept_id, 0 ) is not null
                  then 3
                when nullif(condition_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid	
            from omop.condition_occurrence_temp
            where x_srcfile = 'STAGE_CONDITION'
			and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
		    and load_id = b.loadid
         ;
*/


        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'update from conditions: ',rowcnt ); 
        perform etl.logm('qa_condition_load', 'update from conditions', 'FINISH' ); 
        
        perform etl.logm('qa_condition_load', 'update from measurements' , 'START' );  
        
        -- check measurement

insert into etl.qa_stage_condition 
( id, load_id, loaded )
select 
  cot.x_srcid as id,
  cot.x_srcloadid as load_id, 
  case
    when nullif(cot.measurement_concept_id, 0 ) is not null
      then 3
    when nullif(cot.measurement_source_concept_id, 0 ) is not null
      then 2
    else 1
  end as loaded
from omop.measurement_temp cot
join etl.stage_condition_temp sct on cot.x_srcid = sct.id and sct.load_id = cot.x_srcloadid
where cot.x_srcfile = 'STAGE_CONDITION'
and cot.x_srcloadid = v_loadid
;

/*
        update etl.stage_condition_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(measurement_concept_id, 0 ) is not null
                  then 3
                when nullif(measurement_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.measurement_temp
            where x_srcfile = 'STAGE_CONDITION'
		    and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
		    and load_id = b.loadid
         ;
*/ 
        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'update from measurements: ',rowcnt ); 
        perform etl.logm('qa_condition_load', 'update from measurements', 'FINISH' ); 
        
        perform etl.logm('qa_condition_load', 'update from observations' , 'START' ); 


        -- check observation

insert into etl.qa_stage_condition 
( id, load_id, loaded )
select 
  cot.x_srcid as id,
  cot.x_srcloadid as load_id, 
  case
    when nullif(cot.observation_concept_id, 0 ) is not null
      then 3
    when nullif(cot.observation_source_concept_id, 0 ) is not null
      then 2
    else 1
  end as loaded
from omop.observation_temp cot
join etl.stage_condition_temp sct on cot.x_srcid = sct.id and sct.load_id = cot.x_srcloadid
where cot.x_srcfile = 'STAGE_CONDITION'
and cot.x_srcloadid = v_loadid
;
/*
        update etl.stage_condition_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(observation_concept_id, 0 ) is not null
                  then 3
                when nullif(observation_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.observation_temp
            where x_srcfile = 'STAGE_CONDITION'
		    and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
		    and load_id = b.loadid
         ;
*/

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'update from observations: ',rowcnt );  
        perform etl.logm('qa_condition_load', 'update from observations', 'FINISH' );  
        
        perform etl.logm('qa_condition_load', 'update from procedures' , 'START' );  
        

        -- check procedure

insert into etl.qa_stage_condition 
( id, load_id, loaded )
select 
  cot.x_srcid as id,
  cot.x_srcloadid as load_id, 
  case
    when nullif(cot.procedure_concept_id, 0 ) is not null
      then 3
    when nullif(cot.procedure_source_concept_id, 0 ) is not null
      then 2
    else 1
  end as loaded
from omop.procedure_occurrence_temp cot
join etl.stage_condition_temp sct on cot.x_srcid = sct.id and sct.load_id = cot.x_srcloadid
where cot.x_srcfile = 'STAGE_CONDITION'
and cot.x_srcloadid = v_loadid
;
/*
        update etl.stage_condition_temp sc
        set loaded = b.loaded
        from
        (
           select 
              case
                when nullif(procedure_concept_id, 0 ) is not null
                  then 3
                when nullif(procedure_source_concept_id, 0 ) is not null
                  then 2
                else 1
              end as loaded,
              x_srcid as id,
			  x_srcloadid as loadid
            from omop.procedure_occurrence_temp
            where x_srcfile = 'STAGE_CONDITION'
		    and x_srcloadid = v_loadid
         ) b
         where sc.id = b.id
         and sc.loaded < b.loaded
		    and load_id = b.loadid
         ;
*/

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'update from procedures: ',rowcnt );  
        perform etl.logm('qa_condition_load', 'update from procedures', 'FINISH' );  


        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg
        from 
        (
          select max(loaded) as loaded, id, load_id 
          from etl.qa_stage_condition 
          where load_id = v_loadid
          group by id, load_id
        ) a
        where loaded = 3
        ;
        perform etl.logm('qa_condition_load', 'QA check count: ' , logmsg );

        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg
        from 
        (
          select max(loaded) as loaded, id, load_id 
          from etl.qa_stage_condition 
          where load_id = v_loadid
          group by id, load_id
        ) a
        where loaded = 2
        ;
        perform etl.logm('qa_condition_load', 'QA check count: ' , logmsg );

        select 'Loadid: '||v_loadid||', loaded = 3:' || count(*) into logmsg
        from 
        (
          select max(loaded) as loaded, id, load_id 
          from etl.qa_stage_condition 
          where load_id = v_loadid
          group by id, load_id
        ) a
        where loaded = 1
        ;
        perform etl.logm('qa_condition_load', 'QA check count: ' , logmsg );



        -- copy records that did not make it over to error
        insert into etl.stage_condition_error
        (
          id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        )
        select sct.id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          sct.load_id,
          sct.loaded
        from etl.stage_condition_temp sct
        left join etl.qa_stage_condition qsc on sct.id = qsc.id and sct.load_id = qsc.load_id
        where qsc.id is null  -- there is no record in the etl.qa_stage file, so nothing matched to load it
        and load_id = v_loadid;

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'Error records moved to etl.stage_condition_error: ' , rowcnt );

        -- remove unmapped records that were mapped in other tables
        -- postgres performance improvement
/*
        delete from omop.condition_occurrence_temp co
        using etl.stage_condition_temp sl
        where co.condition_source_concept_id = 0
        and co.x_srcfile = 'STAGE_CONDITION'
        and co.x_srcid = sl.id
        and sl.loaded <> 1
        ;
*/

        --  generic subselect method for other DBs.
        delete from omop.condition_occurrence_temp co
        where co.x_srcid in
        (
            select distinct id
            from etl.qa_stage_condition c
            join omop.condition_occurrence_temp co2 on ( c.id = co2.x_srcid and c.load_id = co2.x_srcloadid )
            where c.loaded >1
            and co2.condition_source_concept_id = 0
            and co2.x_srcfile = 'STAGE_CONDITION'
            and c.load_id = v_loadid
        )
        and co.x_srcfile = 'STAGE_CONDITION'
        and co.x_srcloadid = v_loadid
        ;
        

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'unmapped records removed that were mapped in other tables: ' , rowcnt );


        -- move to etl stage permanent home
        insert into etl.stage_condition
        (
          id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          load_id,
          loaded
        )
        select 
          sct.id,
          condition_code_source_type,
          condition_source_value,
          condition_source_type_value,
          start_date,
          end_date,
          stop_reason,
          visit_source_value,
          person_source_value,
          provider_source_value,
          sct.load_id,
          qsc.loaded
        from etl.stage_condition_temp sct
        join -- only add records that are in etl.qa_stage
        ( select max(loaded) as loaded, id, load_id 
            from etl.qa_stage_condition 
            where load_id = v_loadid
            group by id, load_id
        ) qsc on sct.id = qsc.id and sct.load_id = qsc.load_id
        where load_id = v_loadid;

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'moved from etl.stage_condition_temp to permanent: ' , rowcnt );

        delete from etl.stage_condition_temp
        where load_id = v_loadid;

        get diagnostics rowcnt = ROW_COUNT;
        perform etl.logm('qa_condition_load', 'deleted from etl.stage_condition_temp: ' , rowcnt );

        perform etl.logm('qa_condition_load', 'process' , 'FINISH' ); 

    end;
$$
 