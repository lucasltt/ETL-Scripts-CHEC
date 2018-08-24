create or replace package ETL_EXTRACCION_CHEC is
  procedure PE_AISLADERO(cargarTodo number default 0,
                         pCircuito  varchar2 default 'TODOS');
  procedure PE_ARRENDAMIENTO(cargarTodo number default 0,
                             pCircuito  varchar2 default 'TODOS');
  procedure PE_BARRAJE(cargarTodo number default 0,
                       pCircuito  varchar2 default 'TODOS');
  procedure PE_CAMARA(cargarTodo number default 0,
                      pCircuito  varchar2 default 'TODOS');
  procedure PE_CONDUCTOR_PRIMARIO(cargarTodo number default 0,
                                  pCircuito  varchar2 default 'TODOS');
  procedure PE_CONDUCTOR_TRANSMISION(cargarTodo number default 0,
                                     pCircuito  varchar2 default 'TODOS');
  procedure PE_CONECTIVIDAD(cargarTodo number default 0,
                            pCircuito  varchar2 default 'TODOS');
  procedure PE_CUCHILLA(cargarTodo number default 0,
                        pCircuito  varchar2 default 'TODOS');
  procedure PE_INDICADOR_FALLA(cargarTodo number default 0,
                               pCircuito  varchar2 default 'TODOS');
  procedure PE_INTERRUPTOR(cargarTodo number default 0,
                           pCircuito  varchar2 default 'TODOS');
  procedure PE_NODO_CONDUCTOR(cargarTodo number default 0,
                              pCircuito  varchar2 default 'TODOS');
  procedure PE_POSTE(cargarTodo number default 0,
                     pCircuito  varchar2 default 'TODOS');
  procedure PE_RECONECTADOR(cargarTodo number default 0,
                            pCircuito  varchar2 default 'TODOS');
  procedure PE_REFERENCIA(cargarTodo number default 0,
                          pCircuito  varchar2 default 'TODOS');
  procedure PE_SECCIONALIZADOR(cargarTodo number default 0,
                               pCircuito  varchar2 default 'TODOS');
  procedure PE_SUBESTACION(cargarTodo number default 0,
                           pCircuito  varchar2 default 'TODOS');
  procedure PE_SUICHE(cargarTodo number default 0,
                      pCircuito  varchar2 default 'TODOS');
  procedure PE_TORRE_TRANSMISION(cargarTodo number default 0,
                                 pCircuito  varchar2 default 'TODOS');

  procedure PE_TRANSFORMADOR(cargarTodo number default 0,
                             pCircuito  varchar2 default 'TODOS');
  procedure PE_TRANSF_POT(cargarTodo number default 0,
                          pCircuito  varchar2 default 'TODOS');
  procedure PE_FEEDERS(cargarTodo number default 0,
                       pCircuito  varchar2 default 'TODOS');
  procedure PE_CONDUCTOR_SECUNDARIO(cargarTodo number default 0,
                                    pCircuito  varchar2 default 'TODOS');
  procedure PE_PARARRAYO(cargarTodo number default 0,
                         pCircuito  varchar2 default 'TODOS');
  procedure PE_CAJA_DISTRIBUICION(cargarTodo number default 0,
                                  pCircuito  varchar2 default 'TODOS');
  procedure PE_LUMINARIA(cargarTodo number default 0,
                         pCircuito  varchar2 default 'TODOS');

  procedure PE_FLUJO(cargarTodo number default 0,
                     pCircuito  varchar2 default 'TODOS');

  procedure LimpiarLog(pTablaOrigem  varchar2,
                       pTablaDestino varchar2,
                       pCircuito     in varchar2);

  procedure CrearReport(pCircuito  in varchar2,
                        pTabla     in varchar2,
                        pRegistros in number,
                        pTiempo    in timestamp);

end ETL_EXTRACCION_CHEC;
/
create or replace package body ETL_EXTRACCION_CHEC is

  procedure PE_AISLADERO(cargarTodo number default 0,
                         pCircuito  varchar2 default 'TODOS') is
  
    vSWITCHES     SWITCHES%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$AISLADERO';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where ASSEMBLY IN
                     ('1CC', '2CC', '3CC', 'CR', 'ER', 'EL', 'CFR')
                 and code not in (select code from x$aisladero)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := null;
      vSWITCHES := c;
      vCircuito := vSWITCHES.FPARENT;
    
      --Hace verificacion estrucutural de los datos
      if vSWITCHES.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.STATE not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.STATE = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$aisladero',
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$aisladero
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$aisladero',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
  
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_ARRENDAMIENTO(cargarTodo number default 0,
                             pCircuito  varchar2 default 'TODOS') is
  
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'NETOWNERS';
    vTablaDestino varchar2(30) := 'X$ARRENDAMIENTO';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select code, phnode, height, company, type, cuadrilla
                from NETOWNERS
               where nivel = 1
                 and code not in (select code from X$ARRENDAMIENTO)) loop
    
      begin
        select code
          into vCircuito
          from mvphnode
         where code = c.phnode
           and rownum = 1;
      exception
        when no_data_found then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'PHNODE no encontrado en la tabla MVPHNODE',
             vCircuito,
             sysdate);
          continue;
      end;
    
      begin
        select fparent
          into vCircuito
          from mvelnode
         where phnode = c.phnode
           and rownum = 1;
      exception
        when no_data_found then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'PHNODE no encontrado en la tabla MVELNODE',
             vCircuito,
             sysdate);
          continue;
      end;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into x$ARRENDAMIENTO
          (code, phnode, height, company, type, cuadrilla)
        VALUES
          (c.code, c.phnode, c.height, c.company, c.type, c.cuadrilla);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'Erro al insertar el la tabla X$ARRENDAMIENTO',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_BARRAJE(cargarTodo number default 0,
                       pCircuito  varchar2 default 'TODOS') is
    vSRCBUSES     SRCBUSES%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SRCBUSES';
    vTablaDestino varchar2(30) := 'X$BARRAJE';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SRCBUSES
               where code not in (select code from X$BARRAJE)
                 and substation =
                     DECODE(pCircuito,
                            'TODOS',
                            substation,
                            substr(pCircuito, 1, 3))) loop
    
      vSRCBUSES := NULL;
      vSRCBUSES := c;
      vCircuito := pCircuito;
    
      --Hace verificaci?n estrucutural de los datos
      if vSRCBUSES.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSRCBUSES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSRCBUSES.SUBSTATION is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSRCBUSES.CODE,
           'Elemento sin SUBSTATION',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSRCBUSES.XPOS < 1081440 or vSRCBUSES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSRCBUSES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSRCBUSES.YPOS < 984125 or vSRCBUSES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSRCBUSES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into x$BARRAJE
          (ADDRESS,
           AMP,
           CODE,
           DESCRIPTIO,
           KV,
           LATITUD,
           LONGITUD,
           MVA1PH_SCC,
           MVA3PH_SCC,
           PICTURE,
           PROJECT,
           USER_,
           XPOS,
           YPOS,
           SUBSTATION)
        VALUES
          (vSRCBUSES.ADDRESS,
           vSRCBUSES.AMP,
           vSRCBUSES.CODE,
           vSRCBUSES.DESCRIPTIO,
           vSRCBUSES.KV,
           vSRCBUSES.LATITUD,
           vSRCBUSES.LONGITUD,
           vSRCBUSES.MVA1PH_SCC,
           vSRCBUSES.MVA3PH_SCC,
           vSRCBUSES.PICTURE,
           vSRCBUSES.PROJECT,
           vSRCBUSES.USER_,
           vSRCBUSES.XPOS,
           vSRCBUSES.YPOS,
           vSRCBUSES.SUBSTATION);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSRCBUSES.CODE,
             'Erro al insertar el la tabla x$BARRAJE',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_CAMARA(cargarTodo number default 0,
                      pCircuito  varchar2 default 'TODOS') is
    vMVPHNODE MVPHNODE%ROWTYPE;
  
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'MVPHNODE';
    vTablaDestino varchar2(30) := 'X$CAMARA';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from MVPHNODE
               where ASSEMBLY LIKE 'AC%'
                 and code not in (select code from x$CAMARA)) loop
    
      vMVPHNODE := NULL;
      vMVPHNODE := c;
    
      if pCircuito != 'TODOS' then
        begin
          select fparent
            into vCircuito
            from mvelnode
           where phnode = vMVPHNODE.CODE
             and rownum = 1;
        exception
          when no_data_found then
            insert into etl_extraccion_log
            values
              (vTablaOrigem,
               vTablaDestino,
               vMVPHNODE.CODE,
               'CODE no encontrado en la tabla MVELNODE.PHNODE',
               vCircuito,
               sysdate);
            continue;
        end;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vMVPHNODE.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'POBLACION no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.XPOS < 1081440 or vMVPHNODE.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.YPOS < 984125 or vMVPHNODE.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$CAMARA
          (ADDRESS,
           ASSEMBLY,
           CODE,
           DEP_ID,
           DESCRIPTIO,
           LATITUD,
           LONGITUD,
           MUN_ID,
           OWNER,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (vMVPHNODE.ADDRESS,
           vMVPHNODE.ASSEMBLY,
           vMVPHNODE.CODE,
           vMVPHNODE.DEP_ID,
           vMVPHNODE.DESCRIPTIO,
           vMVPHNODE.LATITUD,
           vMVPHNODE.LONGITUD,
           vMVPHNODE.MUN_ID,
           vMVPHNODE.OWNER,
           vMVPHNODE.PICTURE,
           vMVPHNODE.POBLACION,
           vMVPHNODE.PROJECT,
           vMVPHNODE.TOWNER,
           vMVPHNODE.USER_,
           vMVPHNODE.XPOS,
           vMVPHNODE.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVPHNODE.CODE,
             'Erro al insertar el la tabla X$CAMARA',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_CONDUCTOR_PRIMARIO(cargarTodo number default 0,
                                  pCircuito  varchar2 default 'TODOS') is
  
    vMVLINSEC     MVLINSEC%ROWTYPE;
    vCircuito     varchar2(50);
    vCount        number(2);
    vTablaOrigem  varchar2(30) := 'MVLINSEC';
    vTablaDestino varchar2(30) := 'X$CONDUCTOR_PRIMARIO';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from MVLINSEC
               where kvnom < 47.5
                 and code not in (select code from X$CONDUCTOR_PRIMARIO)
                 and fparent =
                     DECODE(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vMVLINSEC := NULL;
      vMVLINSEC := c;
      vCircuito := c.fparent;
    
      --Hace verificaci?n estrucutural de los datos
      if vMVLINSEC.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.CLASS not in (1, 0) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin CLASS',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin POBLACION',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.PHASES < 1 and vMVLINSEC.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.XPOS1 < 1081440 or vMVLINSEC.XPOS1 > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'XPOS1 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.YPOS1 < 984125 or vMVLINSEC.YPOS1 > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'YPOS1 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.XPOS2 < 1081440 or vMVLINSEC.XPOS2 > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'XPOS2 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.YPOS2 < 984125 or vMVLINSEC.YPOS2 > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'YPOS2 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.FPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      select count(1)
        into vCount
        from CONDUCTO
       where code = vMVLINSEC.CONDUCTOR;
    
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'CONDUCTOR no encontrado en la tabla CONDUCTO',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.GUARDA is not null then
        select count(1)
          into vCount
          from CONDUCTO
         where code = vMVLINSEC.GUARDA;
      
        if vCount = 0 then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVLINSEC.CODE,
             'GUARDA no encontrado en la tabla CONDUCTO',
             vCircuito,
             sysdate);
        
        end if;
      end if;
    
      if vMVLINSEC.NEUTRAL is not null then
        select count(1)
          into vCount
          from CONDUCTO
         where code = vMVLINSEC.NEUTRAL;
      
        if vCount = 0 then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVLINSEC.CODE,
             'NEUTRAL no encontrado en la tabla CONDUCTO',
             vCircuito,
             sysdate);
        
        end if;
      end if;
    
      begin
        --Inserta en la tabla
        insert into x$CONDUCTOR_PRIMARIO
          (ADDRESS,
           CLASS,
           CODE,
           CONDUCTOR,
           DATE_,
           DATE_REM,
           DESCRIPTIO,
           ELNODE1,
           ELNODE2,
           FPARENT,
           GUARDA,
           KVNOM,
           LAT1,
           LAT2,
           LENGTH,
           LON1,
           LON2,
           NEUTRAL,
           OWNER,
           PHASES,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS1,
           XPOS2,
           YPOS1,
           YPOS2,
           ZPOS1,
           ZPOS2)
        VALUES
          (vMVLINSEC.ADDRESS,
           vMVLINSEC.CLASS,
           vMVLINSEC.CODE,
           vMVLINSEC.CONDUCTOR,
           vMVLINSEC.DATE_,
           vMVLINSEC.DATE_REM,
           vMVLINSEC.DESCRIPTIO,
           vMVLINSEC.ELNODE1,
           vMVLINSEC.ELNODE2,
           vMVLINSEC.FPARENT,
           vMVLINSEC.GUARDA,
           vMVLINSEC.KVNOM,
           vMVLINSEC.LAT1,
           vMVLINSEC.LAT2,
           vMVLINSEC.LENGTH,
           vMVLINSEC.LON1,
           vMVLINSEC.LON2,
           vMVLINSEC.NEUTRAL,
           vMVLINSEC.OWNER,
           vMVLINSEC.PHASES,
           vMVLINSEC.PICTURE,
           vMVLINSEC.POBLACION,
           vMVLINSEC.PROJECT,
           vMVLINSEC.TOWNER,
           vMVLINSEC.USER_,
           vMVLINSEC.XPOS1,
           vMVLINSEC.XPOS2,
           vMVLINSEC.YPOS1,
           vMVLINSEC.YPOS2,
           vMVLINSEC.ZPOS1,
           vMVLINSEC.ZPOS2);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVLINSEC.CODE,
             'Erro al insertar el la tabla X$CONDUCTOR_PRIMARIO',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_CONDUCTOR_TRANSMISION(cargarTodo number default 0,
                                     pCircuito  varchar2 default 'TODOS') is
    vMVLINSEC MVLINSEC%ROWTYPE;
  
    vCircuito     varchar2(50);
    vCount        number(2);
    vTablaOrigem  varchar2(30) := 'MVLINSEC';
    vTablaDestino varchar2(30) := 'X$CONDUCTOR_TRANSMISION';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from MVLINSEC
               where kvnom >= 47.5
                 and code not in (select code from X$CONDUCTOR_TRANSMISION)
                 and fparent =
                     DECODE(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vMVLINSEC := NULL;
      vMVLINSEC := c;
      vCircuito := c.fparent;
    
      --Hace verificaci?n estrucutural de los datos
      if vMVLINSEC.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.CLASS not in (1, 0) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin CLASS',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin POBLACION',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.PHASES < 1 and vMVLINSEC.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.XPOS1 < 1081440 or vMVLINSEC.XPOS1 > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'XPOS1 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.YPOS1 < 984125 or vMVLINSEC.YPOS1 > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'YPOS1 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.XPOS2 < 1081440 or vMVLINSEC.XPOS2 > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'XPOS2 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.YPOS2 < 984125 or vMVLINSEC.YPOS2 > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'YPOS2 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.FPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'Elemento sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      select count(1)
        into vCount
        from CONDUCTO
       where code = vMVLINSEC.CONDUCTOR;
    
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVLINSEC.CODE,
           'CONDUCTOR no encontrado en la tabla CONDUCTO',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVLINSEC.GUARDA is not null then
        select count(1)
          into vCount
          from CONDUCTO
         where code = vMVLINSEC.GUARDA;
      
        if vCount = 0 then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVLINSEC.CODE,
             'GUARDA no encontrado en la tabla CONDUCTO',
             vCircuito,
             sysdate);
        
        end if;
      end if;
    
      if vMVLINSEC.NEUTRAL is not null then
        select count(1)
          into vCount
          from CONDUCTO
         where code = vMVLINSEC.NEUTRAL;
      
        if vCount = 0 then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVLINSEC.CODE,
             'NEUTRAL no encontrado en la tabla CONDUCTO',
             vCircuito,
             sysdate);
        
        end if;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$CONDUCTOR_TRANSMISION
          (ADDRESS,
           CLASS,
           CODE,
           CONDUCTOR,
           DATE_,
           DATE_REM,
           DESCRIPTIO,
           ELNODE1,
           ELNODE2,
           FPARENT,
           GUARDA,
           KVNOM,
           LAT1,
           LAT2,
           LENGTH,
           LON1,
           LON2,
           NEUTRAL,
           OWNER,
           PHASES,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS1,
           XPOS2,
           YPOS1,
           YPOS2,
           ZPOS1,
           ZPOS2)
        VALUES
          (vMVLINSEC.ADDRESS,
           vMVLINSEC.CLASS,
           vMVLINSEC.CODE,
           vMVLINSEC.CONDUCTOR,
           vMVLINSEC.DATE_,
           vMVLINSEC.DATE_REM,
           vMVLINSEC.DESCRIPTIO,
           vMVLINSEC.ELNODE1,
           vMVLINSEC.ELNODE2,
           vMVLINSEC.FPARENT,
           vMVLINSEC.GUARDA,
           vMVLINSEC.KVNOM,
           vMVLINSEC.LAT1,
           vMVLINSEC.LAT2,
           vMVLINSEC.LENGTH,
           vMVLINSEC.LON1,
           vMVLINSEC.LON2,
           vMVLINSEC.NEUTRAL,
           vMVLINSEC.OWNER,
           vMVLINSEC.PHASES,
           vMVLINSEC.PICTURE,
           vMVLINSEC.POBLACION,
           vMVLINSEC.PROJECT,
           vMVLINSEC.TOWNER,
           vMVLINSEC.USER_,
           vMVLINSEC.XPOS1,
           vMVLINSEC.XPOS2,
           vMVLINSEC.YPOS1,
           vMVLINSEC.YPOS2,
           vMVLINSEC.ZPOS1,
           vMVLINSEC.ZPOS2);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVLINSEC.CODE,
             'Erro al insertar el la tabla X$CONDUCTOR_TRANSMISION',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_CONECTIVIDAD(cargarTodo number default 0,
                            pCircuito  varchar2 default 'TODOS') is
  
    vCount        number(2);
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'MVELNODE';
    vTablaDestino varchar2(30) := 'X$CONECTIVIDAD';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select kvnom,
                     ifalla,
                     emedida,
                     pt,
                     prayos,
                     owner,
                     towner,
                     feeder,
                     phnode,
                     isconnecte,
                     spacing,
                     height,
                     xpos,
                     ypos,
                     fparent,
                     null as tparent,
                     phases,
                     assembly,
                     code
                from mvelnode
               where code not in (select code from X$CONECTIVIDAD)
                 and fparent =
                     DECODE(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vCircuito := c.fparent;
    
      if c.XPOS < 1081440 or c.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.YPOS < 984125 or c.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      select count(1) into vCount from mvphnode where code = c.phnode;
    
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'CODE no encontrado en la tabla MVPHNODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$CONECTIVIDAD
          (kvnom,
           ifalla,
           emedida,
           pt,
           prayos,
           owner,
           towner,
           feeder,
           phnode,
           isconnecte,
           spacing,
           height,
           xpos,
           ypos,
           fparent,
           tparent,
           phases,
           assembly,
           code)
        VALUES
          (c.kvnom,
           c.ifalla,
           c.emedida,
           c.pt,
           c.prayos,
           c.owner,
           c.towner,
           c.feeder,
           c.phnode,
           c.isconnecte,
           c.spacing,
           c.height,
           c.xpos,
           c.ypos,
           c.fparent,
           c.tparent,
           c.phases,
           c.assembly,
           c.code);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'Erro al insertar el la tabla X$CONECTIVIDAD',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
  
    vTablaOrigem := 'LVELNODE';
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select kvnom,
                     ifalla,
                     emedida,
                     pt,
                     prayos,
                     owner,
                     towner,
                     null as feeder,
                     phnode,
                     isconnecte,
                     spacing,
                     height,
                     xpos,
                     ypos,
                     fparent,
                     tparent,
                     phases,
                     assembly,
                     code
                from lvelnode
               where code not in (select code from X$CONECTIVIDAD)
                 and fparent =
                     DECODE(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vCircuito := c.fparent;
    
      if c.XPOS < 1081440 or c.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.YPOS < 984125 or c.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      select count(1) into vCount from lvphnode where code = c.phnode;
    
      if vCount = 0 then
        select count(1) into vCount from custmetr where code = c.phnode;
        if vCount = 0 then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'CODE no encontrado en la tabla LVPHNODE ni en la  CUSTMETR',
             vCircuito,
             sysdate);
          continue;
        end if;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$CONECTIVIDAD
          (kvnom,
           ifalla,
           emedida,
           pt,
           prayos,
           owner,
           towner,
           feeder,
           phnode,
           isconnecte,
           spacing,
           height,
           xpos,
           ypos,
           fparent,
           phases,
           assembly,
           code)
        VALUES
          (c.kvnom,
           c.ifalla,
           c.emedida,
           c.pt,
           c.prayos,
           c.owner,
           c.towner,
           c.feeder,
           c.phnode,
           c.isconnecte,
           c.spacing,
           c.height,
           c.xpos,
           c.ypos,
           c.fparent,
           c.phases,
           c.assembly,
           c.code);
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'Erro al insertar el la tabla X$CONECTIVIDAD',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
  
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_CUCHILLA(cargarTodo number default 0,
                        pCircuito  varchar2 default 'TODOS') is
    vSWITCHES SWITCHES%ROWTYPE;
  
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$CUCHILLA';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where assembly in ('RAM', '3OS')
                 and code not in (select code from x$cuchilla)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := NULL;
      vSWITCHES := c;
    
      vCircuito := c.Fparent;
    
      --Hace verificaci?n estrucutural de los datos
      if vSWITCHES.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.State not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.State = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$cuchilla',
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$cuchilla
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$cuchilla',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_INDICADOR_FALLA(cargarTodo number default 0,
                               pCircuito  varchar2 default 'TODOS') is
  
    rIFALLA       IFALLA%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'IFALLA';
    vTablaDestino varchar2(30) := 'X$INDICADOR_FALLA';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from IFALLA
               where code not in (select code from X$INDICADOR_FALLA)) loop
    
      rIFALLA := null;
      rIFALLA := c;
    
      begin
        select fparent
          into vCircuito
          from mvelnode
         where ifalla = c.code
           and rownum = 1;
      exception
        when no_data_found then
          begin
            select fparent
              into vCircuito
              from lvelnode
             where ifalla = c.code
               and rownum = 1;
          exception
            when no_data_found then
              insert into etl_extraccion_log
              values
                (vTablaOrigem,
                 vTablaDestino,
                 rIFALLA.CODE,
                 'Elemento sin FPARENT',
                 'SIN CIRCUITO',
                 sysdate);
              commit;
              continue;
          end;
      end;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if rIFALLA.Code is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           rIFALLA.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$INDICADOR_FALLA
          (CODE, IFALLA, MAKER, TIPO, INOM, ELNODE)
        VALUES
          (rIFALLA.CODE,
           rIFALLA.IFALLA,
           rIFALLA.MAKER,
           rIFALLA.TIPO,
           rIFALLA.INOM,
           rIFALLA.ELNODE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             rIFALLA.CODE,
             'Erro al insertar el la tabla X$INDICADOR_FALLA',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_INTERRUPTOR(cargarTodo number default 0,
                           pCircuito  varchar2 default 'TODOS') is
    vSWITCHES     SWITCHES%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$INTERRUPTOR';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where assembly = '3IG'
                 and code not in (select code from x$interruptor)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := NULL;
      vSWITCHES := c;
      vCircuito := c.fparent;
    
      --Hace verificacion estrucutural de los datos
      if vSWITCHES.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.State not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.State = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$interruptor',
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$interruptor
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$interruptor',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_NODO_CONDUCTOR(cargarTodo number default 0,
                              pCircuito  varchar2 default 'TODOS') is
  
    vMVPHNODE     MVPHNODE%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'MVPHNODE';
    vTablaDestino varchar2(30) := 'X$NODO_CONDUCTOR';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from MVPHNODE
               where ASSEMBLY = 'GOTERA'
                 and code not in (select code from X$NODO_CONDUCTOR)) loop
    
      vMVPHNODE := NULL;
      vMVPHNODE := c;
    
      if pCircuito != 'TODOS' then
        begin
          select fparent
            into vCircuito
            from mvelnode
           where code = vMVPHNODE.CODE
             and rownum = 1;
        exception
          when no_data_found then
            insert into etl_extraccion_log
            values
              (vTablaOrigem,
               vTablaDestino,
               vMVPHNODE.CODE,
               'CODE no encontrado en la tabla MVELNODE',
               'SIN CIRCUITO',
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vMVPHNODE.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'POBLACION no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.XPOS < 1081440 or vMVPHNODE.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.YPOS < 984125 or vMVPHNODE.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$NODO_CONDUCTOR
          (ADDRESS,
           ASSEMBLY,
           CODE,
           DEP_ID,
           DESCRIPTIO,
           LATITUD,
           LONGITUD,
           MUN_ID,
           OWNER,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (vMVPHNODE.ADDRESS,
           vMVPHNODE.ASSEMBLY,
           vMVPHNODE.CODE,
           vMVPHNODE.DEP_ID,
           vMVPHNODE.DESCRIPTIO,
           vMVPHNODE.LATITUD,
           vMVPHNODE.LONGITUD,
           vMVPHNODE.MUN_ID,
           vMVPHNODE.OWNER,
           vMVPHNODE.PICTURE,
           vMVPHNODE.POBLACION,
           vMVPHNODE.PROJECT,
           vMVPHNODE.TOWNER,
           vMVPHNODE.USER_,
           vMVPHNODE.XPOS,
           vMVPHNODE.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            ('MVPHNODE',
             'X$NODO_CONDUCTOR',
             vMVPHNODE.CODE,
             'Erro al insertar el la tabla X$NODO_CONDUCTOR',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_POSTE(cargarTodo number default 0,
                     pCircuito  varchar2 default 'TODOS') is
  
    vMVPHNODE     MVPHNODE%ROWTYPE;
    vLVPHNODE     LVPHNODE%ROWTYPE;
    vCount        number(2);
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'MVPHNODE';
    vTablaDestino varchar2(30) := 'X$POSTE';
    vTime         timestamp;
    vRegistros    number(8) := 0;
    vKV           number(9, 2);
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select ph.*, el.fparent, el.kvnom
                from MVPHNODE ph
               inner join MVELNODE el
                  on el.phnode = ph.code
               where (ph.ASSEMBLY LIKE 'A%' or ph.ASSEMBLY = 'PORTICO')
                 and ph.assembly not like 'AC%'
                 and ph.code not in (select code from X$POSTE)
                 and el.fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vCircuito := c.Fparent;
    
      /*
      vMVPHNODE := NULL;
      vMVPHNODE := c;
      
      if pCircuito != 'TODOS' then
        begin
          select fparent
            into vCircuito
            from mvelnode
           where code = vMVPHNODE.CODE
             and rownum = 1;
        exception
          when no_data_found then
            insert into etl_extraccion_log
            values
              (vTablaOrigem,
               vTablaDestino,
               vMVPHNODE.CODE,
               'CODE no encontrado en la tabla MVELNODE',
               'SIN CIRCUITO',
               sysdate);
            commit;
            continue;
        end;
      end if;
      
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
      
      
      --Verifica si es poste o torre de acuerdo con el nivel de tension
      select kvnom
        into vKV
        from mvelnode
       where phnode = vMVPHNODE.CODE
         and rownum = 1;
         */
    
      if c.KVNOM != 33 and C.KVNOM != 13.2 then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if c.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'POBLACION no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.XPOS < 1081440 or c.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.YPOS < 984125 or c.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvelnode
       where code = vMVPHNODE.CODE;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('MVPHNODE',
           'X$POSTE',
           vMVPHNODE.CODE,
           'CODE no encontrado en la tabla MVELNODE',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      begin
        --Inserta en la tabla
        insert into X$POSTE
          (ADDRESS,
           ASSEMBLY,
           CODE,
           DEP_ID,
           DESCRIPTIO,
           LATITUD,
           LONGITUD,
           MUN_ID,
           OWNER,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (c.ADDRESS,
           c.ASSEMBLY,
           c.CODE,
           c.DEP_ID,
           c.DESCRIPTIO,
           c.LATITUD,
           c.LONGITUD,
           c.MUN_ID,
           c.OWNER,
           c.PICTURE,
           c.POBLACION,
           c.PROJECT,
           c.TOWNER,
           c.USER_,
           c.XPOS,
           c.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVPHNODE.CODE,
             'Erro al insertar el la tabla X$POSTE',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
  
    vTablaOrigem := 'LVPHNODE';
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select ph.*, el.fparent, el.kvnom
                from LVPHNODE ph
               inner join MVELNODE el
                  on el.phnode = ph.code
               where (ph.ASSEMBLY LIKE 'AK%' or ph.ASSEMBLY LIKE 'AV%' or
                     ph.ASSEMBLY LIKE 'AR%' or ph.ASSEMBLY LIKE 'AM%' or
                     ph.ASSEMBLY LIKE 'AT%' or ph.ASSEMBLY LIKE 'AP%' or
                     ph.ASSEMBLY LIKE 'AB%' or ph.ASSEMBLY LIKE 'AZ%' or
                     ph.ASSEMBLY LIKE 'AI%' or ph.ASSEMBLY = 'GOTERA')
                 and ph.code not in (select code from X$POSTE)
                 and el.fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
      vCircuito := c.fparent;
    
      /*
      if pCircuito != 'TODOS' then
        begin
          select fparent
            into vCircuito
            from lvelnode
           where code = vLVPHNODE.CODE
             and rownum = 1;
        exception
          when no_data_found then
            insert into etl_extraccion_log
            values
              (vTablaOrigem,
               vTablaDestino,
               c.CODE,
               'CODE no encontrado en la tabla LVELNODE',
               'SIN CIRCUITO',
               sysdate);
            commit;
            continue;
        end;
      end if;
      
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
      */
    
      --Hace verificaci?n estrucutural de los datos
      if c.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'POBLACION no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.XPOS < 1081440 or c.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if c.YPOS < 984125 or c.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           c.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('LVPHNODE',
           'X$POSTE_B',
           vLVPHNODE.CODE,
           'CODE no encontrado en la tabla MVELNODE',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      begin
        --Inserta en la tabla
        insert into X$POSTE
          (ADDRESS,
           ASSEMBLY,
           CODE,
           DEP_ID,
           DESCRIPTIO,
           LATITUD,
           LONGITUD,
           MUN_ID,
           OWNER,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (c.ADDRESS,
           c.ASSEMBLY,
           c.CODE,
           c.DEP_ID,
           c.DESCRIPTIO,
           c.LATITUD,
           c.LONGITUD,
           c.MUN_ID,
           c.OWNER,
           c.PICTURE,
           c.POBLACION,
           c.PROJECT,
           c.TOWNER,
           c.USER_,
           c.XPOS,
           c.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             c.CODE,
             'Erro al insertar el la tabla X$POSTE',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
  
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_RECONECTADOR(cargarTodo number default 0,
                            pCircuito  varchar2 default 'TODOS') is
    vSWITCHES     SWITCHES%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$RECONECTADOR';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where ASSEMBLY IN ('3RL', '3RG')
                 and code not in (select code from x$reconectador)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := NULL;
      vSWITCHES := c;
    
      vCircuito := c.Fparent;
    
      --Hace verificaci?n estrucutural de los datos
      if vSWITCHES.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.State not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.State = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$reconectador',
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$reconectador
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$reconectador',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_REFERENCIA(cargarTodo number default 0,
                          pCircuito  varchar2 default 'TODOS') is
  
    vSWITCHES     SWITCHES%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$REFERENCIA';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where ASSEMBLY = 'ARCOS'
                 and code not in (select code from x$referencia)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := NULL;
      vSWITCHES := c;
    
      vCircuito := c.Fparent;
    
      --Hace verificaci?n estrucutural de los datos
      if vSWITCHES.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.State not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.State = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$referencia',
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$referencia
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$referencia',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_SECCIONALIZADOR(cargarTodo number default 0,
                               pCircuito  varchar2 default 'TODOS') is
    vSWITCHES SWITCHES%ROWTYPE;
  
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$SECCIONALIZADOR';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where ASSEMBLY IN ('3OC', '3SC')
                 and code not in (select code from x$seccionalizador)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := NULL;
      vSWITCHES := c;
    
      vCircuito := c.Fparent;
    
      --Hace verificaci?n estrucutural de los datos
      if vSWITCHES.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.State not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.STATE = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$seccionalizador',
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$seccionalizador
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$seccionalizador',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_SUBESTACION(cargarTodo number default 0,
                           pCircuito  varchar2 default 'TODOS') is
    vSUBSTATI     SUBSTATI%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SUBSTATI';
    vTablaDestino varchar2(30) := 'X$SUBESTACION';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SUBSTATI
               where code not in (select code from x$subestacion)
                 and code = DECODE(pCircuito,
                                   'TODOS',
                                   code,
                                   substr(pCircuito, 1, 3))) loop
    
      vSUBSTATI := NULL;
      vSUBSTATI := c;
      vCircuito := pCircuito;
      --Hace verificaci?n estrucutural de los datos
      if vSUBSTATI.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSUBSTATI.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSUBSTATI.DESCRIPTIO is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSUBSTATI.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSUBSTATI.XPOS < 1081440 or vSUBSTATI.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSUBSTATI.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSUBSTATI.YPOS < 984125 or vSUBSTATI.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSUBSTATI.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into x$subestacion
          (ADDRESS,
           ADMIN,
           AREA,
           CODE,
           COST,
           DESCRIPTIO,
           DPTO,
           LATITUD,
           LONGITUD,
           MAIN_KV,
           MUNICIPIO,
           MVAR_PEAK,
           MVA_INSTAL,
           MW_PEAK,
           OWNER,
           POBLACION,
           PROJECT,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (vSUBSTATI.ADDRESS,
           vSUBSTATI.ADMIN,
           vSUBSTATI.AREA,
           vSUBSTATI.CODE,
           vSUBSTATI.COST,
           vSUBSTATI.DESCRIPTIO,
           vSUBSTATI.DPTO,
           vSUBSTATI.LATITUD,
           vSUBSTATI.LONGITUD,
           vSUBSTATI.MAIN_KV,
           vSUBSTATI.MUNICIPIO,
           vSUBSTATI.MVAR_PEAK,
           vSUBSTATI.MVA_INSTAL,
           vSUBSTATI.MW_PEAK,
           vSUBSTATI.OWNER,
           vSUBSTATI.POBLACION,
           vSUBSTATI.PROJECT,
           vSUBSTATI.USER_,
           vSUBSTATI.XPOS,
           vSUBSTATI.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSUBSTATI.CODE,
             'Erro al insertar el la tabla x$subestacion',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_SUICHE(cargarTodo number default 0,
                      pCircuito  varchar2 default 'TODOS') is
    vSWITCHES     SWITCHES%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'SWITCHES';
    vTablaDestino varchar2(30) := 'X$SUICHE';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from SWITCHES
               where ASSEMBLY IN ('31SA', '31S6')
                 and code not in (select code from x$suiche)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vSWITCHES := NULL;
      vSWITCHES := c;
      vCircuito := c.FPARENT;
    
      --Hace verificaci?n estrucutural de los datos
      if vSWITCHES.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.State not in (0, 7) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'STATE no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.PHASES < 1 and vSWITCHES.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.XPOS < 1081440 or vSWITCHES.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.YPOS < 984125 or vSWITCHES.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSWITCHES.FPARENT is null and vSWITCHES.STATE = 7 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'Elemento cerrado sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvlinsec
       where code = vSWITCHES.LINESECTIO;
      if vCount = 0 then
        select count(1)
          into vCount
          from lvlinsec
         where code = vSWITCHES.LINESECTIO;
      end if;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSWITCHES.CODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into x$suiche
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           DATE_INST,
           DESCRIPTIO,
           FPARENT,
           KV,
           LATITUD,
           LINESECTIO,
           LONGITUD,
           MAKER,
           MUNICIPIO,
           OWNER,
           PHASES,
           PICTURE,
           PROJECT,
           STATE,
           TOWNER,
           USER_,
           XPOS,
           YPOS,
           ZONE)
        VALUES
          (vSWITCHES.ADDRESS,
           vSWITCHES.AMP,
           vSWITCHES.ASSEMBLY,
           vSWITCHES.CODE,
           vSWITCHES.DATE_INST,
           vSWITCHES.DESCRIPTIO,
           vSWITCHES.FPARENT,
           vSWITCHES.KV,
           vSWITCHES.LATITUD,
           vSWITCHES.LINESECTIO,
           vSWITCHES.LONGITUD,
           vSWITCHES.MAKER,
           vSWITCHES.MUNICIPIO,
           vSWITCHES.OWNER,
           vSWITCHES.PHASES,
           vSWITCHES.PICTURE,
           vSWITCHES.PROJECT,
           vSWITCHES.STATE,
           vSWITCHES.TOWNER,
           vSWITCHES.USER_,
           vSWITCHES.XPOS,
           vSWITCHES.YPOS,
           vSWITCHES.ZONE);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSWITCHES.CODE,
             'Erro al insertar el la tabla x$suiche',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_TORRE_TRANSMISION(cargarTodo number default 0,
                                 pCircuito  varchar2 default 'TODOS') is
    vMVPHNODE     MVPHNODE%ROWTYPE;
    vCircuito     varchar2(50);
    vCount        number(2);
    vTablaOrigem  varchar2(30) := 'MVPHNODE';
    vTablaDestino varchar2(30) := 'X$TORRE_TRANSMISION';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from MVPHNODE
               where (ASSEMBLY LIKE 'AH%' or ASSEMBLY LIKE 'AS%')
                 and code not in (select code from x$torre_transmision)) loop
    
      vMVPHNODE := NULL;
      vMVPHNODE := c;
    
      --Verifica si es poste o torre de acuerdo con el nivel de tension
      select count(1)
        into vCount
        from mvelnode
       where phnode = vMVPHNODE.CODE
         and kvnom = 115;
    
      if vCount = 0 then
        continue;
      end if;
    
      if pCircuito != 'TODOS' then
        begin
          select fparent
            into vCircuito
            from mvelnode
           where code = vMVPHNODE.CODE
             and rownum = 1;
        exception
          when no_data_found then
            insert into etl_extraccion_log
            values
              (vTablaOrigem,
               vTablaDestino,
               vMVPHNODE.CODE,
               'CODE no encontrado en la tabla MVELNODE',
               'SIN CIRCUITO',
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vMVPHNODE.CODE is null then
      
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'POBLACION no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.XPOS < 1081440 or vMVPHNODE.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vMVPHNODE.YPOS < 984125 or vMVPHNODE.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vMVPHNODE.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      /*
      select count(1)
        into vCount
        from mvelnode
       where code = vMVPHNODE.CODE;
      
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('MVPHNODE',
           'x$torre_transmision',
           vMVPHNODE.CODE,
           'CODE no encontrado en la tabla MVELNODE',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      begin
        --Inserta en la tabla
        insert into x$torre_transmision
          (ADDRESS,
           ASSEMBLY,
           CODE,
           DEP_ID,
           DESCRIPTIO,
           LATITUD,
           LONGITUD,
           MUN_ID,
           OWNER,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (vMVPHNODE.ADDRESS,
           vMVPHNODE.ASSEMBLY,
           vMVPHNODE.CODE,
           vMVPHNODE.DEP_ID,
           vMVPHNODE.DESCRIPTIO,
           vMVPHNODE.LATITUD,
           vMVPHNODE.LONGITUD,
           vMVPHNODE.MUN_ID,
           vMVPHNODE.OWNER,
           vMVPHNODE.PICTURE,
           vMVPHNODE.POBLACION,
           vMVPHNODE.PROJECT,
           vMVPHNODE.TOWNER,
           vMVPHNODE.USER_,
           vMVPHNODE.XPOS,
           vMVPHNODE.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vMVPHNODE.CODE,
             'Erro al insertar el la tabla x$torre_transmision',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_TRANSFORMADOR(cargarTodo number default 0,
                             pCircuito  varchar2 default 'TODOS') is
    vTRANSFOR     TRANSFOR%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'TRANSFOR';
    vTablaDestino varchar2(30) := 'X$TRANSFORMADOR';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from TRANSFOR
               where code not in (select code from x$transformador)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vTRANSFOR := null;
      vTRANSFOR := c;
      vCircuito := c.FPARENT;
    
      --Hace verificaci?n estrucutural de los datos
      if vTRANSFOR.Code is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSFOR.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vTRANSFOR.PHASES < 1 and vTRANSFOR.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSFOR.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vTRANSFOR.XPOS < 1081440 or vTRANSFOR.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSFOR.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vTRANSFOR.YPOS < 984125 or vTRANSFOR.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSFOR.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vTRANSFOR.FPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSFOR.CODE,
           'Elemento sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vTRANSFOR.AUTOPROT != 'S' and vTRANSFOR.AUTOPROT != 'N' then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSFOR.CODE,
           'Elemento con AUTOPROT inválido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      --select count(1) into vCount from mvlinsec where code = vLINESECTIO;
      --if vCount = 0 then
      -- select count(1) into vCount from lvlinsec where code = vLINESECTIO;
      -- end if;
    
      /* -- Es Reservado
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          ('SWITCHES',
           'x$cuchilla',
           vCODE,
           'LINESECTIO no encontrado en las tablas MVLINSEC/LVLINSEC',
           vCircuito, sysdate);
        continue;
      end if;
      */
    
      --Inserta en la tabla
      begin
        insert into X$TRANSFORMADOR
          (ADDRESS,
           AUTOPROT,
           CASO,
           CODE,
           CULOSSES,
           DATE_FAB,
           DATE_INST,
           DATE_REM,
           DEP_ID,
           DESCRIPTIO,
           ELNODE,
           FELOSSES,
           FPARENT,
           GROUP_,
           IMPEDANCE,
           INVNUMBER,
           LATITUD,
           LONGITUD,
           LVELNODE,
           MARCA,
           MUNICIPIO,
           NCALIDAD,
           OWNER,
           OWNER1,
           OWNER2,
           PCV,
           PHASES,
           PHNODE,
           PICTURE,
           POBLACION,
           SERIAL,
           TIPOFUSES,
           TIPOSUB,
           TIPO_RED,
           TIPO_SUB,
           TRFTYPE,
           USER_,
           VALVULA,
           XPOS,
           YPOS,
           ZONE,
           PROJECT,
           NUM_TRFS)
        VALUES
          (vTRANSFOR.ADDRESS,
           vTRANSFOR.AUTOPROT,
           vTRANSFOR.CASO,
           vTRANSFOR.CODE,
           vTRANSFOR.CULOSSES,
           vTRANSFOR.DATE_FAB,
           vTRANSFOR.DATE_INST,
           vTRANSFOR.DATE_REM,
           vTRANSFOR.DEP_ID,
           vTRANSFOR.DESCRIPTIO,
           vTRANSFOR.ELNODE,
           vTRANSFOR.FELOSSES,
           vTRANSFOR.FPARENT,
           vTRANSFOR.GROUP_,
           vTRANSFOR.IMPEDANCE,
           vTRANSFOR.INVNUMBER,
           vTRANSFOR.LATITUD,
           vTRANSFOR.LONGITUD,
           vTRANSFOR.LVELNODE,
           vTRANSFOR.MARCA,
           vTRANSFOR.MUNICIPIO,
           vTRANSFOR.NCALIDAD,
           vTRANSFOR.OWNER,
           vTRANSFOR.OWNER1,
           vTRANSFOR.OWNER2,
           vTRANSFOR.PCV,
           vTRANSFOR.PHASES,
           vTRANSFOR.PHNODE,
           vTRANSFOR.PICTURE,
           vTRANSFOR.POBLACION,
           vTRANSFOR.SERIAL,
           vTRANSFOR.TIPOFUSES,
           vTRANSFOR.TIPOSUB,
           vTRANSFOR.TIPO_RED,
           vTRANSFOR.TIPO_SUB,
           vTRANSFOR.TRFTYPE,
           vTRANSFOR.USER_,
           vTRANSFOR.VALVULA,
           vTRANSFOR.XPOS,
           vTRANSFOR.YPOS,
           vTRANSFOR.ZONE,
           vTRANSFOR.PROJECT,
           vTRANSFOR.NUM_TRFS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vTRANSFOR.CODE,
             'Erro al insertar en la tabla X$TRANSFORMADOR',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_TRANSF_POT(cargarTodo number default 0,
                          pCircuito  varchar2 default 'TODOS') is
    vTRANSF_POT   PT%ROWTYPE;
    vCircuito     varchar2(50);
    vXPOS         NUMBER;
    vYPOS         NUMBER;
    vTablaOrigem  varchar2(30) := 'PT';
    vTablaDestino varchar2(30) := 'X$TRANSF_POT';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from PT
               where code not in (select code from X$TRANSF_POT)) loop
    
      vTRANSF_POT := null;
      vTRANSF_POT := c;
    
      begin
        select fparent
          into vCircuito
          from mvelnode
         where pt = c.code
           and rownum = 1;
      exception
        when no_data_found then
          begin
            select fparent
              into vCircuito
              from lvelnode
             where pt = c.code
               and rownum = 1;
          exception
            when no_data_found then
              insert into etl_extraccion_log
              values
                (vTablaOrigem,
                 vTablaDestino,
                 vTRANSF_POT.CODE,
                 'Elemento sin FPARENT',
                 'SIN CIRCUITO',
                 sysdate);
              commit;
              continue;
          end;
      end;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vTRANSF_POT.Code is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSF_POT.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      --Busca la posicion en la tabla de nodos eletricos
      begin
        select xpos, ypos
          into vXPOS, vYPOS
          from mvelnode
         where code = c.elnode;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vTRANSF_POT.CODE,
             'Erro al buscar las coordinadas',
             vCircuito,
             sysdate);
      end;
    
      if vXPOS < 1081440 or vXPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSF_POT.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vYPOS < 984125 or vYPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vTRANSF_POT.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      --Inserta en la tabla
      begin
        insert into X$TRANSF_POT
          (BURDEN,
           CAPACIDAD,
           CLASE,
           CODE,
           EFASE,
           ELNODE,
           EMAG,
           MAKER,
           PRESS,
           RACTUAL,
           RELAS,
           TIPO,
           XPOS,
           YPOS)
        VALUES
          (vTRANSF_POT.BURDEN,
           vTRANSF_POT.CAPACIDAD,
           vTRANSF_POT.CLASE,
           vTRANSF_POT.CODE,
           vTRANSF_POT.EFASE,
           vTRANSF_POT.ELNODE,
           vTRANSF_POT.EMAG,
           vTRANSF_POT.MAKER,
           vTRANSF_POT.PRESS,
           vTRANSF_POT.RACTUAL,
           vTRANSF_POT.RELAS,
           vTRANSF_POT.TIPO,
           vXPOS,
           vYPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vTRANSF_POT.CODE,
             'Erro al insertar en la tabla X$TRANSF_POT',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_FEEDERS(cargarTodo number default 0,
                       pCircuito  varchar2 default 'TODOS') is
    vFEEDER       FEEDERS%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'FEEDERS';
    vTablaDestino varchar2(30) := 'X$FEEDERS';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from FEEDERS
               where code not in (select code from X$FEEDERS)
                 and fparent =
                     decode(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vFEEDER := null;
      vFEEDER := c;
    
      --Hace verificaci?n estrucutural de los datos
      if vFEEDER.Code is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vFEEDER.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vFEEDER.PHASES < 1 and vFEEDER.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vFEEDER.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vFEEDER.XPOS < 1081440 or vFEEDER.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vFEEDER.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vFEEDER.YPOS < 984125 or vFEEDER.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vFEEDER.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vFEEDER.FPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vFEEDER.CODE,
           'Elemento sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      --Inserta en la tabla
      begin
        insert into X$FEEDERS
          (ADDRESS,
           AMP,
           ASSEMBLY,
           CODE,
           CUSTOMERS,
           DESCRIPTIO,
           FPARENT,
           GROUP_,
           ISON,
           ISOPEN,
           LATITUD,
           LINK,
           LONGITUD,
           METERCODE,
           OWNER,
           PHASES,
           PICTURE,
           PRSTATE,
           RADIAL,
           RECLOSIN,
           SEI,
           SOURCEBUS,
           TYPE,
           USER_,
           USER_HIST,
           XPOS,
           YPOS,
           Z)
        VALUES
          (vFEEDER.ADDRESS,
           vFEEDER.AMP,
           vFEEDER.ASSEMBLY,
           vFEEDER.CODE,
           vFEEDER.CUSTOMERS,
           vFEEDER.DESCRIPTIO,
           vFEEDER.FPARENT,
           vFEEDER.GROUP_,
           vFEEDER.ISON,
           vFEEDER.ISOPEN,
           vFEEDER.LATITUD,
           vFEEDER.LINK,
           vFEEDER.LONGITUD,
           vFEEDER.METERCODE,
           vFEEDER.OWNER,
           vFEEDER.PHASES,
           vFEEDER.PICTURE,
           vFEEDER.PRSTATE,
           vFEEDER.RADIAL,
           vFEEDER.RECLOSIN,
           vFEEDER.SEI,
           vFEEDER.SOURCEBUS,
           vFEEDER.TYPE,
           vFEEDER.USER_,
           vFEEDER.USER_HIST,
           vFEEDER.XPOS,
           vFEEDER.YPOS,
           vFEEDER.Z);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vFEEDER.CODE,
             'Erro al insertar en la tabla X$FEEDERS',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_FLUJO(cargarTodo number default 0,
                     pCircuito  varchar2 default 'TODOS') is
  
  begin
    pe_interruptor(cargarTodo, pCircuito);
    pe_cuchilla(cargarTodo, pCircuito);
    pe_suiche(cargarTodo, pCircuito);
    pe_reconectador(cargarTodo, pCircuito);
    pe_referencia(cargarTodo, pCircuito);
    pe_seccionalizador(cargarTodo, pCircuito);
    pe_aisladero(cargarTodo, pCircuito);
  
    /*
      delete from etl_extraccion_log
       where tabla = 'SWITCHES'
         and tabla_e is null;
      commit;
    
      insert into etl_extraccion_log
        select 'SWITCHES',
               null,
               code,
               'Elemento con ASSEMBLY no Identificado',
               null,
               sysdate
          from (select code
                  from switches
                minus (select code
                        from x$interruptor
                      union all
                      select code
                        from x$cuchilla
                      union all
                      select code
                        from x$suiche
                      union all
                      select code
                        from x$reconectador
                      union all
                      select code
                        from x$referencia
                      union all
                      select code
                        from x$seccionalizador
                      union all
                      select code
                        from x$aisladero
                      union all
                      select codigo
                        from etl_extraccion_log
                       where tabla = 'SWITCHES'
                         and tabla_e is not null));
      commit;
    */
    pe_camara(cargarTodo, pCircuito);
    pe_torre_transmision(cargarTodo, pCircuito);
    pe_nodo_conductor(cargarTodo, pCircuito);
    pe_poste(cargarTodo, pCircuito);
  
    /*
    insert into etl_extraccion_log
      select 'MVPHNODE',
             null,
             code,
             'Elemento con ASSEMBLY no Identificado',
             null,
             sysdate
        from (select code
                from mvphnode
              minus (select code
                      from x$camara
                    union all
                    select code
                      from x$poste
                    union all
                    select code
                      from x$torre_transmision
                    union all
                    select code
                      from x$nodo_conductor
                    union all
                    select codigo
                      from etl_extraccion_log
                     where tabla = 'MVPHONDE'
                       and tabla_e is not null));
    commit;
    */
  
    pe_conductor_primario(cargarTodo, pCircuito);
    pe_conductor_transmision(cargarTodo, pCircuito);
    pe_conductor_secundario(cargarTodo, pCircuito);
  
    /*
    insert into etl_extraccion_log
      select 'MVLINSEC',
             null,
             code,
             'Elemento con ASSEMBLY no Identificado',
             null,
             sysdate
        from (select code
                from MVLINSEC
              minus (select code
                      from x$conductor_primario
                    union all
                    select code
                      from x$conductor_transmision
                    union all
                    select codigo
                      from etl_extraccion_log
                     where tabla = 'MVLINSEC'
                       and tabla_e is not null));
    commit;
    */
  
    pe_subestacion(cargarTodo, pCircuito);
    pe_barraje(cargarTodo, pCircuito);
    pe_conectividad(cargarTodo, pCircuito);
    pe_arrendamiento(cargarTodo, pCircuito);
    pe_indicador_falla(cargarTodo, pCircuito);
    pe_transformador(cargarTodo, pCircuito);
    pe_transf_pot(cargarTodo, pCircuito);
    pe_feeders(cargarTodo, pCircuito);
    pe_luminaria(cargarTodo, pCircuito);
    pe_pararrayo(cargarTodo, pCircuito);
  
  end;

  procedure PE_CONDUCTOR_SECUNDARIO(cargarTodo number default 0,
                                    pCircuito  varchar2 default 'TODOS') is
    vLVLINSEC     LVLINSEC%ROWTYPE;
    vCount        number(2);
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'LVLINSEC';
    vTablaDestino varchar2(30) := 'X$CONDUCTOR_SECUNDARIO';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
    for c in (select *
                from LVLINSEC
               WHERE code not in (select code from X$CONDUCTOR_SECUNDARIO)
                 and uso != 'AC'
                 and fparent =
                     DECODE(pCircuito, 'TODOS', fparent, pCircuito)) loop
    
      vLVLINSEC := null;
      vLVLINSEC := c;
      vCircuito := c.FPARENT;
    
      --Hace verificaci?n estrucutural de los datos
      if vLVLINSEC.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.CLASS not in (1, 0) then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'Elemento sin CLASS',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'Elemento sin POBLACION',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.PHASES < 1 and vLVLINSEC.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.XPOS1 < 1081440 or vLVLINSEC.XPOS1 > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'XPOS1 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.YPOS1 < 984125 or vLVLINSEC.YPOS1 > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'YPOS1 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.XPOS2 < 1081440 or vLVLINSEC.XPOS2 > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'XPOS2 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.YPOS2 < 984125 or vLVLINSEC.YPOS2 > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'YPOS2 fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.FPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'Elemento sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.TPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'Elemento sin TPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.USO is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'Elemento sin USO',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      select count(1)
        into vCount
        from CONDUCTO
       where code = vLVLINSEC.CONDUCTOR;
    
      if vCount = 0 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVLINSEC.CODE,
           'CONDUCTOR no encontrado en la tabla CONDUCTO',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVLINSEC.NEUTRAL is not null then
        select count(1)
          into vCount
          from CONDUCTO
         where code = vLVLINSEC.NEUTRAL;
      
        if vCount = 0 then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vLVLINSEC.CODE,
             'NEUTRAL no encontrado en la tabla CONDUCTO',
             vCircuito,
             sysdate);
        
        end if;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$CONDUCTOR_SECUNDARIO
          (ADDRESS,
           CLASS,
           CODE,
           CONDUCTOR,
           DATE_,
           DATE_REM,
           DESCRIPTIO,
           ELNODE1,
           ELNODE2,
           FPARENT,
           KVNOM,
           LENGTH,
           NEUTRAL,
           OWNER,
           PHASES,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           TPARENT,
           USER_,
           USO,
           XPOS1,
           XPOS2,
           YPOS1,
           YPOS2,
           ZPOS1,
           ZPOS2,
           LAT1,
           LAT2,
           LON1,
           LON2)
        VALUES
          (vLVLINSEC.ADDRESS,
           vLVLINSEC.CLASS,
           vLVLINSEC.CODE,
           vLVLINSEC.CONDUCTOR,
           vLVLINSEC.DATE_,
           vLVLINSEC.DATE_REM,
           vLVLINSEC.DESCRIPTIO,
           vLVLINSEC.ELNODE1,
           vLVLINSEC.ELNODE2,
           vLVLINSEC.FPARENT,
           vLVLINSEC.KVNOM,
           vLVLINSEC.LENGTH,
           vLVLINSEC.NEUTRAL,
           vLVLINSEC.OWNER,
           vLVLINSEC.PHASES,
           vLVLINSEC.PICTURE,
           vLVLINSEC.POBLACION,
           vLVLINSEC.PROJECT,
           vLVLINSEC.TOWNER,
           vLVLINSEC.TPARENT,
           vLVLINSEC.USER_,
           vLVLINSEC.USO,
           vLVLINSEC.XPOS1,
           vLVLINSEC.XPOS2,
           vLVLINSEC.YPOS1,
           vLVLINSEC.YPOS2,
           vLVLINSEC.ZPOS1,
           vLVLINSEC.ZPOS2,
           vLVLINSEC.LAT1,
           vLVLINSEC.LAT2,
           vLVLINSEC.LON1,
           vLVLINSEC.LON2);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vLVLINSEC.CODE,
             'Erro al insertar el la tabla X$CONDUCTOR_SECUNDARIO',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_PARARRAYO(cargarTodo number default 0,
                         pCircuito  varchar2 default 'TODOS') is
    vPRAYO        PRAYOS%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'PRAYOS';
    vTablaDestino varchar2(30) := 'X$PARARRAYOS';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from PRAYOS
               where code not in (select code from X$PARARRAYOS)) loop
    
      vPRAYO := null;
      vPRAYO := c;
    
      begin
        select fparent
          into vCircuito
          from mvelnode
         where prayos = c.code
           and rownum = 1;
      exception
        when no_data_found then
          begin
            select fparent
              into vCircuito
              from lvelnode
             where prayos = c.code
               and rownum = 1;
          exception
            when no_data_found then
              insert into etl_extraccion_log
              values
                (vTablaOrigem,
                 vTablaDestino,
                 vPRAYO.CODE,
                 'Elemento sin FPARENT',
                 'SIN CIRCUITO',
                 sysdate);
              commit;
              continue;
          end;
      end;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vPRAYO.Code is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vPRAYO.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      --Inserta en la tabla
      begin
        insert into X$PARARRAYOS
          (BIL, CODE, CLASE, INOM, VCEBADO, VMAX, VNOM)
        VALUES
          (vPRAYO.BIL,
           vPRAYO.CODE,
           vPRAYO.CLASE,
           vPRAYO.INOM,
           vPRAYO.VCEBADO,
           vPRAYO.VMAX,
           vPRAYO.VNOM);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vPRAYO.CODE,
             'Erro al insertar en la tabla X$PARARRAYOS',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_CAJA_DISTRIBUICION(cargarTodo number default 0,
                                  pCircuito  varchar2 default 'TODOS') is
    vLVPHNODE     LVPHNODE%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'LVPHNODE';
    vTablaDestino varchar2(30) := 'X$CAJA_DISTRIBUICION';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from LVPHNODE
               WHERE assembly LIKE 'AC%'
                 and code not in (select code from X$CAJA_DISTRIBUICION)) loop
    
      vLVPHNODE := null;
      vLVPHNODE := c;
    
      if pCircuito != 'TODOS' then
        begin
          select fparent
            into vCircuito
            from lvelnode
           where code = vLVPHNODE.CODE
             and rownum = 1;
        exception
          when no_data_found then
            insert into etl_extraccion_log
            values
              (vTablaOrigem,
               vTablaDestino,
               vLVPHNODE.CODE,
               'CODE no encontrado en la tabla LVELNODE',
               'SIN CIRCUITO',
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vLVPHNODE.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVPHNODE.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVPHNODE.POBLACION not in ('R', 'U') then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVPHNODE.CODE,
           'Elemento sin POBLACION',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVPHNODE.XPOS < 1081440 or vLVPHNODE.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVPHNODE.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vLVPHNODE.YPOS < 984125 or vLVPHNODE.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vLVPHNODE.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$CAJA_DISTRIBUICION
          (ADDRESS,
           ASSEMBLY,
           CODE,
           DEP_ID,
           DESCRIPTIO,
           LATITUD,
           LONGITUD,
           MUN_ID,
           OWNER,
           PICTURE,
           POBLACION,
           PROJECT,
           TOWNER,
           USER_,
           XPOS,
           YPOS)
        VALUES
          (vLVPHNODE.ADDRESS,
           vLVPHNODE.ASSEMBLY,
           vLVPHNODE.CODE,
           vLVPHNODE.DEP_ID,
           vLVPHNODE.DESCRIPTIO,
           vLVPHNODE.LATITUD,
           vLVPHNODE.LONGITUD,
           vLVPHNODE.MUN_ID,
           vLVPHNODE.OWNER,
           vLVPHNODE.PICTURE,
           vLVPHNODE.POBLACION,
           vLVPHNODE.PROJECT,
           vLVPHNODE.TOWNER,
           vLVPHNODE.USER_,
           vLVPHNODE.XPOS,
           vLVPHNODE.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vLVPHNODE.CODE,
             'Erro al insertar el la tabla X$CAJA_DISTRIBUICION',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure PE_LUMINARIA(cargarTodo number default 0,
                         pCircuito  varchar2 default 'TODOS') is
    vSTREETLG     STREETLG%ROWTYPE;
    vCircuito     varchar2(50);
    vTablaOrigem  varchar2(30) := 'STREETLG';
    vTablaDestino varchar2(30) := 'X$LUMINARIA';
    vTime         timestamp;
    vRegistros    number(8) := 0;
  
  begin
  
    vTime := CURRENT_TIMESTAMP;
  
    if cargarTodo = 1 then
      execute immediate 'truncate table ' || vTablaDestino;
    end if;
  
    LimpiarLog(vTablaOrigem, vTablaDestino, pCircuito);
  
    for c in (select *
                from STREETLG
               WHERE code not in (select code from X$LUMINARIA)) loop
    
      vSTREETLG := null;
      vSTREETLG := c;
    
      begin
        select fparent
          into vCircuito
          from mvelnode
         where code = c.elnode
           and rownum = 1;
      exception
        when no_data_found then
          begin
            select fparent
              into vCircuito
              from lvelnode
             where code = c.elnode
               and rownum = 1;
          exception
            when no_data_found then
              insert into etl_extraccion_log
              values
                (vTablaOrigem,
                 vTablaDestino,
                 vSTREETLG.CODE,
                 'STREETLG sin FPARENT',
                 'SIN CIRCUITO',
                 sysdate);
              commit;
              continue;
          end;
      end;
    
      if pCircuito != 'TODOS' and vCircuito != pCircuito then
        continue;
      end if;
    
      --Hace verificaci?n estrucutural de los datos
      if vSTREETLG.CODE is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSTREETLG.CODE,
           'Elemento sin CODE',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSTREETLG.PHASES < 1 and vSTREETLG.PHASES > 15 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSTREETLG.CODE,
           'PHASES no reconocido',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSTREETLG.FPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSTREETLG.CODE,
           'Elemento sin FPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSTREETLG.TPARENT is null then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSTREETLG.CODE,
           'Elemento sin TPARENT',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSTREETLG.XPOS < 1081440 or vSTREETLG.XPOS > 1280608 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSTREETLG.CODE,
           'XPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      if vSTREETLG.YPOS < 984125 or vSTREETLG.YPOS > 1145785 then
        insert into etl_extraccion_log
        values
          (vTablaOrigem,
           vTablaDestino,
           vSTREETLG.CODE,
           'YPOS fuera del MBR',
           vCircuito,
           sysdate);
        continue;
      end if;
    
      begin
        --Inserta en la tabla
        insert into X$LUMINARIA
          (ADDRESS,
           CODE,
           ELNODE,
           ETIQUETA,
           F_UTILIZACION,
           KV,
           KW,
           LATITUD,
           LONGITUD,
           MEDIDA,
           MUN_ID,
           OBSERVAC,
           OWNER,
           PERDIDAS,
           PHASES,
           PROJECT,
           TPARENT,
           TYPE,
           USER_,
           USO,
           XPOS,
           YPOS)
        VALUES
          (vSTREETLG.ADDRESS,
           vSTREETLG.CODE,
           vSTREETLG.ELNODE,
           vSTREETLG.ETIQUETA,
           vSTREETLG.F_UTILIZACION,
           vSTREETLG.KV,
           vSTREETLG.KW,
           vSTREETLG.LATITUD,
           vSTREETLG.LONGITUD,
           vSTREETLG.MEDIDA,
           vSTREETLG.MUN_ID,
           vSTREETLG.OBSERVAC,
           vSTREETLG.OWNER,
           vSTREETLG.PERDIDAS,
           vSTREETLG.PHASES,
           vSTREETLG.PROJECT,
           vSTREETLG.TPARENT,
           vSTREETLG.TYPE,
           vSTREETLG.USER_,
           vSTREETLG.USO,
           vSTREETLG.XPOS,
           vSTREETLG.YPOS);
      
        vRegistros := vRegistros + 1;
      exception
        when others then
          insert into etl_extraccion_log
          values
            (vTablaOrigem,
             vTablaDestino,
             vSTREETLG.CODE,
             'Erro al insertar el la tabla X$LUMINARIA',
             vCircuito,
             sysdate);
      end;
      commit;
    end loop;
    commit;
    CrearReport(pCircuito, vTablaDestino, vRegistros, vTime);
  end;

  procedure LimpiarLog(pTablaOrigem  varchar2,
                       pTablaDestino varchar2,
                       pCircuito     in varchar2) is
  
  begin
  
    if pCircuito = 'TODOS' then
      delete from etl_extraccion_log
       where tabla = pTablaOrigem
         and tabla_e = pTablaDestino;
      commit;
    else
      delete from etl_extraccion_log
       where tabla = pTablaOrigem
         and tabla_e = pTablaDestino
         and circuito = pCircuito;
      commit;
    end if;
  
  end;

  procedure CrearReport(pCircuito  in varchar2,
                        pTabla     in varchar2,
                        pRegistros in number,
                        pTiempo    in timestamp) is
  
    vTiempo number(8);
    vDelta  interval day(1) to second;
    vReg    number(5);
  begin
    vDelta := current_timestamp - pTiempo;
  
    delete from ETL_EXTRACCION_REPORT
     where circuito = pCircuito
       and tabla = pTabla;
  
    select extract(second from vDelta) + extract(minute from vDelta) * 60
      into vTiempo
      from dual;
  
    select count(1)
      into vReg
      from etl_extraccion_log
     where circuito = pCircuito
       and tabla_e = pTabla;
  
    insert into ETL_EXTRACCION_REPORT
    values
      (pCircuito, pTabla, vTiempo, pRegistros, vReg, sysdate);
    commit;
  
  end;

end ETL_EXTRACCION_CHEC;
/
