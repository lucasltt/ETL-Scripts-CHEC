create or replace package ETL_CARGA_CHEC is

  -- Author  : LTTURCHE
  -- Created : 21/05/2018 13:30:48
  -- Purpose : 

  function GenerarInsertQuery(pTabla varchar2) return varchar2;
  function GenerarInsertQueryConLTT(pTabla varchar2) return varchar2;

  procedure CargarCircuitoJob(pCircuito varchar2);

  procedure CargarCircuito(pCircuito varchar2);

  procedure BorrarCircuito(pCircuito varchar2);

  procedure CrearReport(pCircuito     in varchar2,
                        pJob          in varchar2,
                        pRegistrosOk  in number,
                        pRegistrosErr in number,
                        pTiempo       in timestamp);

  procedure ControlTriggers(pDisable in number);

end ETL_CARGA_CHEC;
/
create or replace package body ETL_CARGA_CHEC is

  function GenerarInsertQuery(pTabla varchar2) return varchar2 is
    vSQLSelect varchar2(2000);
    vSQLInsert varchar2(2000);
    vCampos    varchar2(2000);
  begin
  
    select listagg(column_name, ', ') within group(order by table_name desc)
      into vCampos
      from all_tab_columns
     where table_name = pTabla
       and column_name not like 'LTT_%';
  
    vSQLSelect := 'SELECT ' || vCampos || ' from T$' || pTabla;
    vSQLInsert := 'INSERT INTO ' || pTabla || '(' || vCampos || ')';
    return vSQLInsert || ' ' || vSQLSelect;
  
  end;

  function GenerarInsertQueryConLTT(pTabla varchar2) return varchar2 is
    vSQLSelect varchar2(2000);
    vSQLInsert varchar2(2000);
    vCampos    varchar2(2000);
    vCamposLTT varchar2(100) := ', LTT_ID, LTT_DATE';
    vValorLTT  varchar2(100) := ', 0, SYSDATE';
  begin
  
    select listagg(column_name, ', ') within group(order by table_name desc)
      into vCampos
      from all_tab_columns
     where table_name = pTabla
       and column_name not like 'LTT_%';
  
    vSQLSelect := 'SELECT ' || vCampos || vValorLTT || ' from T$' || pTabla;
    vSQLInsert := 'INSERT INTO B$' || pTabla || '(' || vCampos ||
                  vCamposLTT || ')';
    return vSQLInsert || ' ' || vSQLSelect;
  
  end;

  procedure CargarCircuito(pCircuito varchar2) is
    vCount  number(5);
    vErr    varchar2(200);
    vRegErr number(6) := 0;
    vRegOk  number(6) := 0;
  
    vTime timestamp := CURRENT_TIMESTAMP;
  
  begin
  
    ControlTriggers(1);
  
    for ele in (select * from etl_code2fid where circuito = pCircuito) loop
    
      for comp in (select c.g3e_table
                     from g3e_component c
                    inner join g3e_featurecomponent fc
                       on fc.g3e_cno = c.g3e_cno
                    where fc.g3e_fno = ele.g3e_fno
                      and c.g3e_detail = 0
                    order by fc.g3e_ordinal) loop
        begin
        
          execute immediate 'select count(1) from B$' || comp.g3e_table ||
                            ' where g3e_fid = ' || ele.g3e_fid ||
                            ' and g3e_fno = ' || ele.g3e_fno
            into vCount;
        
          if vCount > 0 then
            continue;
          end if;
        
          execute immediate GenerarInsertQueryConLTT(comp.g3e_table) ||
                            ' where g3e_fid = ' || ele.g3e_fid;
        
          insert into etl_carga_log
          values
            ('SIN JOB',
             pCircuito,
             ele.g3e_fid,
             ele.g3e_fno,
             'OK',
             comp.g3e_table,
             sysdate,
             null);
          vRegOk := vRegOk + 1;
        exception
          when others then
            vErr := SUBSTR(SQLERRM, 1, 200);
            insert into etl_carga_log
            values
              ('SIN JOB',
               pCircuito,
               ele.g3e_fid,
               ele.g3e_fno,
               'ERRO',
               comp.g3e_table,
               sysdate,
               vErr);
            vRegErr := vRegErr + 1;
        end;
      end loop;
      commit;
    
    end loop;
  
    ControlTriggers(0);
    CrearReport(pCircuito, 'SIN JOB', vRegOk, vRegErr, vTime);
  end;

  procedure BorrarCircuito(pCircuito varchar2) is
    vErr    varchar2(200);
    vRegErr number(6) := 0;
    vRegOk  number(6) := 0;
  
    vTime timestamp := CURRENT_TIMESTAMP;
  
  begin
  
    ControlTriggers(1);
  
    for ele in (select * from etl_code2fid where circuito = pCircuito) loop
    
      for comp in (select c.g3e_table
                     from g3e_component c
                    inner join g3e_featurecomponent fc
                       on fc.g3e_cno = c.g3e_cno
                    where fc.g3e_fno = ele.g3e_fno
                      and c.g3e_detail = 0
                    order by fc.g3e_ordinal) loop
        begin
        
          execute immediate 'delete from b$' || comp.g3e_table ||
                            ' where g3e_fid = ' || ele.g3e_fid;
        
          insert into etl_carga_log
          values
            ('BORRAR',
             pCircuito,
             ele.g3e_fid,
             ele.g3e_fno,
             'OK',
             comp.g3e_table,
             sysdate,
             null);
          vRegOk := vRegOk + 1;
        exception
          when others then
            vErr := SUBSTR(SQLERRM, 1, 200);
            insert into etl_carga_log
            values
              ('BORRAR',
               pCircuito,
               ele.g3e_fid,
               ele.g3e_fno,
               'ERRO',
               comp.g3e_table,
               sysdate,
               vErr);
            vRegErr := vRegErr + 1;
        end;
      end loop;
      commit;
    
    end loop;
  
    ControlTriggers(0);
    CrearReport(pCircuito, 'BORRAR', vRegOk, vRegErr, vTime);
  end;

  procedure CargarCircuitoJob(pCircuito varchar2) is
    vCount  number(5);
    vJob    varchar2(10);
    vErr    varchar2(200);
    vRegErr number(6) := 0;
    vRegOk  number(6) := 0;
  
    vTime timestamp := CURRENT_TIMESTAMP;
  
  begin
  
    begin
      select g3e_identifier
        into vJob
        from g3e_job
       where g3e_description = pCircuito
         and rownum = 1
       order by g3e_creation desc;
      ltt_user.editjob(vJob);
      ltt_user.discardjob();
      ltt_user.findpendingedits();
    exception
      when no_data_found then
        select etl_carga_seq.nextval into vCount from dual;
        vJob := 'ETL_' || to_char(vCount);
      
        ltt_admin.CreateJob(vJob);
        insert into g3e_job
          (g3e_identifier,
           g3e_description,
           g3e_owner,
           g3e_status,
           g3e_creation,
           cd_seq,
           ts_extwr,
           g3e_processingstatus,
           g3e_jobclass,
           work_request,
           version,
           gis_job_status,
           region)
        values
          (vJob,
           pCircuito,
           'GENERGIA',
           'Open',
           sysdate,
           1,
           sysdate,
           1,
           0,
           1,
           1,
           'NEW',
           'HSV');
        commit;
        ltt_user.EditJob(vJob);
    end;
  
    for ele in (select * from etl_code2fid where circuito = pCircuito) loop
    
      for comp in (select c.g3e_table
                     from g3e_component c
                    inner join g3e_featurecomponent fc
                       on fc.g3e_cno = c.g3e_cno
                    where fc.g3e_fno = ele.g3e_fno
                      and c.g3e_detail = 0
                    order by fc.g3e_ordinal) loop
        begin
          execute immediate GenerarInsertQuery(comp.g3e_table) ||
                            ' where g3e_fid = ' || ele.g3e_fid;
        
          insert into etl_carga_log
          values
            (vJob,
             pCircuito,
             ele.g3e_fid,
             ele.g3e_fno,
             'OK',
             comp.g3e_table,
             sysdate,
             null);
          vRegOk := vRegOk + 1;
        exception
          when others then
            vErr := SUBSTR(SQLERRM, 1, 200);
            insert into etl_carga_log
            values
              (vJob,
               pCircuito,
               ele.g3e_fid,
               ele.g3e_fno,
               'ERRO',
               comp.g3e_table,
               sysdate,
               vErr);
            vRegErr := vRegErr + 1;
        end;
      end loop;
      commit;
    
    end loop;
    begin
      G3E_MANAGEMODLOG.GTUpdateDeltasAll(1);
      commit;
    exception
      when others then
        null;
    end;
  
    begin
      LTT_USER.findpendingedits();
      commit;
    exception
      when others then
        null;
    end;
    CrearReport(pCircuito, vJob, vRegOk, vRegErr, vTime);
  end;

  procedure CrearReport(pCircuito     in varchar2,
                        pJob          in varchar2,
                        pRegistrosOk  in number,
                        pRegistrosErr in number,
                        pTiempo       in timestamp) is
  
    vTiempo number(8);
    vDelta  interval day(1) to second;
  begin
    vDelta := current_timestamp - pTiempo;
  
    delete from etl_carga_report
     where circuito = pCircuito
       and job = pJob;
  
    select extract(second from vDelta) + extract(minute from vDelta) * 60
      into vTiempo
      from dual;
  
    insert into etl_carga_report
    values
      (pCircuito, pJob, vTiempo, pRegistrosOk, pRegistrosErr, sysdate);
    commit;
  
  end;

  procedure ControlTriggers(pDisable in number) is
  
  begin
  
    for tabla in (select distinct table_name
                    from all_all_tables
                   where owner = 'GENERGIA'
                     and table_name like 'T$%') loop
    
      if pDisable = 1 then
        execute immediate 'alter table ' ||
                          replace(tabla.table_name, 'T$', 'B$') ||
                          ' disable all triggers';
      else
        execute immediate 'alter table ' ||
                          replace(tabla.table_name, 'T$', 'B$') ||
                          ' enable all triggers';
      end if;
    
    end loop;
  
  end;

end ETL_CARGA_CHEC;
/
