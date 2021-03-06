create or replace procedure ETL_CrearTablasIntermedias is

begin
  for c in (select c.g3e_table
              from g3e_component c
             inner join g3e_featurecomponent fc
                on fc.g3e_cno = c.g3e_cno
             where fc.g3e_fno in (29000,
                                  20500,
                                  19000,
                                  18100,
                                  18900,
                                  17400,
                                  19100,
                                  17100,
                                  17000,
                                  17700,
                                  19300,
                                  19400,
                                  18800,
                                  18700,
                                  19800,
                                  19200,
                                  19600,
                                  19700,
                                  20400,
                                  21200,
                                  20100,
                                  21400)
               and c.g3e_detail = 0
             group by c.g3e_table) loop

    begin
      execute immediate 'drop table T$' || c.g3e_table;
    exception
      when others then
        dbms_output.put_line('Tabla T$' || c.g3e_table || ' no existe');
    end;

    begin
      execute immediate 'create table T$' || c.g3e_table ||
                        ' as select * from B$' || c.g3e_table ||
                        ' where 1=2';

      execute immediate 'alter table T$' || c.g3e_table ||
                        ' modify ltt_id null';

    exception
      when others then
        dbms_output.put_line('Error al crear la tabla T$' || c.g3e_table);
    end;

  end loop;
end;
/
