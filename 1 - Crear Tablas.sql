
--***************************************************************************************
--*   Version       : 1.0
--*   Req.          : 
--*   OC.           : 
--*   Analista      : Luis Alexander
--*   Desarrollador : Lucas Turchet
--*   Fecha         : 01 Marzo de 2017
--*   Descripcion   : Crear proceso ETL CHEC
--***************************************************************************************


--***************************************************************************************
--BORRAR TABLAS VIEJAS
--***************************************************************************************
declare
  type tableArrays is table of varchar2(60);
  tables tableArrays := tableArrays('X$AISLADERO',
                                    'X$ARRENDAMIENTO',
                                    'X$BARRAJE',
                                    'X$CAJA_DISTRIBUICION',
                                    'X$CAMARA',
                                    'X$CONDUCTOR_PRIMARIO',
                                    'X$CONDUCTOR_SECUNDARIO',
                                    'X$CONDUCTOR_TRANSMISION',
                                    'X$CONECTIVIDAD',
                                    'X$CUCHILLA',
                                    'X$FEEDERS',
                                    'X$INDICADOR_FALLA',
                                    'X$INTERRUPTOR',
                                    'X$LUMINARIA',
                                    'X$NODO_CONDUCTOR',
                                    'X$PARARRAYOS',
                                    'X$POSTE',
                                    'X$RECONECTADOR',
                                    'X$REFERENCIA',
                                    'X$SECCIONALIZADOR',
                                    'X$SUBESTACION',
                                    'X$SUICHE',
                                    'X$TORRE_TRANSMISION',
                                    'X$TRANSFORMADOR',
                                    'X$TRANSF_POT',
                                    'ETL_CARGA_LOG',
                                    'ETL_CARGA_REPORT',
                                    'ETL_CODE2FID',
                                    'ETL_EXTRACCION_LOG',
                                    'ETL_EXTRACCION_REPORT',
                                    'ETL_INDEXES',
                                    'ETL_NORMAS',
                                    'ETL_PICKLISTS',
                                    'ETL_TRANSFORMACION_LOG',
                                    'ETL_TRANSFORMACION_REPORT',
                                    'ETL_VALIDATION_LOG');
begin
  for i in 1 .. tables.count loop
    begin
      execute immediate 'drop table ' || tables(i);
    exception
      when others then
        null;
    end;
  
  end loop;
end;
/

--***************************************************************************************
--CREAR TABLAS NUEVAS
--***************************************************************************************

create table etl_indexes 
(
index_name varchar2(100),
table_name varchar2(100),
index_ddl varchar2(3000)
);

create table etl_normas
(
g3e_fno number(5),
tipo varchar(50),
valor varchar2(15),
norma varchar2(50),
cantidad number(5),
grupo varchar(20)
);

create table ETL_EXTRACCION_LOG
(
tabla varchar2(80),
tabla_e varchar2(80),
codigo varchar2(20),
msg varchar2(300),
circuito varchar2(50),
fecha date
);

create table ETL_EXTRACCION_REPORT
(
circuito varchar2(50),
tabla varchar2(50),
tiempo_ejecucion number(8),
registros_ext number(8),
registros_err number(8),
fecha date
);

create table ETL_TRANSFORMACION_LOG
(
tabla varchar2(80),
tabla_e varchar2(80),
codigo varchar2(20),
preoredad number(2),
msg varchar2(300),
circuito varchar2(50),
fecha date
);


create table ETL_TRANSFORMACION_REPORT
(
circuito varchar2(50),
elemento varchar2(50),
tiempo_ejecucion number(8),
registros_ext number(8),
registros_err number(8),
fecha date
);


create table ETL_CARGA_LOG
(
jobname      varchar2(50),
circuito varchar2(50),
g3e_fid varchar2(80),
g3e_fno varchar2(80),
status varchar2(50),
componente varchar2(50),
fecha date,
erro varchar2(200)
);

create table etl_validation_log
(
circuito varchar2(20),
G3E_FID NUMBER(8),
G3E_FNO NUMBER(5),
G3E_CNO NUMBER(5),
ErrorPriority VARCHAR2(5),
ErrorDescription VARCHAR2(500),
ErrorLocation VARCHAR2(200)
);


create table ETL_CARGA_REPORT
(
circuito varchar2(50),
job varchar2(50),
tiempo_ejecucion number(8),
registros_ext number(8),
registros_err number(8),
fecha date
);

create table ETL_PICKLISTS
(
categoria varchar2(50),
valor varchar2(80),
descrip varchar2(80),
valor_carga varchar2(100)
);

create table ETL_CODE2FID
(
tabla varchar2(80),
code varchar2(80),
g3e_fno number(5),
g3e_fid number(10),
circuito varchar2(50)
);

create table X$INDICADOR_FALLA
(
  code   VARCHAR2(16),
  maker  VARCHAR2(16),
  tipo   VARCHAR2(16),
  inom   NUMBER(10,2),
  ifalla NUMBER(10,2),
  elnode VARCHAR2(16)
);

create table X$LUMINARIA
(
  address       VARCHAR2(50),
  code          VARCHAR2(16),
  elnode        VARCHAR2(16),
  etiqueta      CHAR(15),
  f_utilizacion NUMBER(5,2),
  kv            NUMBER(6,3),
  kw            NUMBER(6,3),
  latitud       NUMBER(15,8),
  longitud      NUMBER(15,8),
  medida        VARCHAR2(2),
  mun_id        NUMBER(10),
  observac      VARCHAR2(60),
  owner         VARCHAR2(30),
  perdidas      NUMBER(11,3) not null,
  phases        NUMBER(2),
  project       VARCHAR2(16),
  tparent       VARCHAR2(16),
  type          VARCHAR2(1),
  user_         VARCHAR2(30),
  uso           CHAR(15),
  xpos          NUMBER(9,1),
  ypos          NUMBER(9,1)
);

create table X$CONDUCTOR_TRANSMISION
(
  address    VARCHAR2(50),
  code       VARCHAR2(16),
  conductor  VARCHAR2(16),
  descriptio VARCHAR2(50),
  elnode1    VARCHAR2(16),
  elnode2    VARCHAR2(16),
  fparent    VARCHAR2(16),
  guarda     VARCHAR2(16),
  neutral    VARCHAR2(16),
  owner      VARCHAR2(30),
  picture    VARCHAR2(80),
  poblacion  VARCHAR2(1),
  project    VARCHAR2(16),
  towner     VARCHAR2(2),
  user_      VARCHAR2(50),
  class      NUMBER(1),
  kvnom      NUMBER(7,3),
  lat1       NUMBER(20,10),
  lat2       NUMBER(20,10),
  length     NUMBER(8,1),
  lon1       NUMBER(20,10),
  lon2       NUMBER(20,10),
  phases     NUMBER(2),
  xpos1      NUMBER(9,1),
  xpos2      NUMBER(9,1),
  ypos1      NUMBER(9,1),
  ypos2      NUMBER(9,1),
  zpos1      NUMBER(10,2),
  zpos2      NUMBER(10,2),
  date_rem   DATE,
  date_      DATE
);


create table X$INTERRUPTOR
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  date_inst  DATE
);




create table X$CUCHILLA
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  date_inst  DATE
);



create table X$SUICHE
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  date_inst  DATE
);

create table X$RECONECTADOR
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  date_inst  DATE
);


create table X$REFERENCIA
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  date_inst  DATE
);

create table X$SECCIONALIZADOR
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  date_inst  DATE
);




create table X$AISLADERO
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  project    VARCHAR2(16),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  kv         NUMBER(7,3),
  amp        NUMBER(6,1),
  state      NUMBER(1),
  linesectio VARCHAR2(16),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  maker      VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(1),
  municipio  NUMBER(10),
  zone       NUMBER(10),
  tipofuses  VARCHAR2(10),
  date_inst  DATE
);

create table X$CAMARA
(
ADDRESS   VARCHAR2(50),
ASSEMBLY   VARCHAR2(16),
CODE   VARCHAR2(16),
DESCRIPTIO   VARCHAR2(50),
OWNER   VARCHAR2(30),
PICTURE   VARCHAR2(80),
POBLACION   VARCHAR2(1),
PROJECT   VARCHAR2(16),
TOWNER   VARCHAR2(2),
USER_   VARCHAR2(50),
DEP_ID   NUMBER(2),
LATITUD   NUMBER(15,8),
LONGITUD   NUMBER(15,8),
MUN_ID   NUMBER(10),
XPOS   NUMBER(9,1),
YPOS   NUMBER(9,1)
);


create table X$TORRE_TRANSMISION
(
ADDRESS   VARCHAR2(50),
ASSEMBLY   VARCHAR2(16),
CODE   VARCHAR2(16),
DESCRIPTIO   VARCHAR2(50),
OWNER   VARCHAR2(30),
PICTURE   VARCHAR2(80),
POBLACION   VARCHAR2(1),
PROJECT   VARCHAR2(16),
TOWNER   VARCHAR2(2),
USER_   VARCHAR2(50),
DEP_ID   NUMBER(2),
LATITUD   NUMBER(15,8),
LONGITUD   NUMBER(15,8),
MUN_ID   NUMBER(10),
XPOS   NUMBER(9,1),
YPOS   NUMBER(9,1)
);


create table X$NODO_CONDUCTOR
(
ADDRESS   VARCHAR2(50),
ASSEMBLY   VARCHAR2(16),
CODE   VARCHAR2(16),
DESCRIPTIO   VARCHAR2(50),
OWNER   VARCHAR2(30),
PICTURE   VARCHAR2(80),
POBLACION   VARCHAR2(1),
PROJECT   VARCHAR2(16),
TOWNER   VARCHAR2(2),
USER_   VARCHAR2(50),
DEP_ID   NUMBER(2),
LATITUD   NUMBER(15,8),
LONGITUD   NUMBER(15,8),
MUN_ID   NUMBER(10),
XPOS   NUMBER(9,1),
YPOS   NUMBER(9,1)
).

create table X$SUBESTACION
(
ADDRESS   VARCHAR2(50),
ADMIN   VARCHAR2(50),
CODE   VARCHAR2(16),
DESCRIPTIO   VARCHAR2(50),
OWNER   VARCHAR2(50),
POBLACION   VARCHAR2(1),
PROJECT   VARCHAR2(16),
USER_   VARCHAR2(50),
AREA   NUMBER(10,2),
COST   NUMBER(11,0),
DPTO   NUMBER(4,0),
LATITUD   NUMBER(15,8),
LONGITUD   NUMBER(15,8),
MAIN_KV   NUMBER(7,3),
MUNICIPIO   NUMBER(4,0),
MVA_INSTAL   NUMBER(7,1),
MVAR_PEAK   NUMBER(8,2),
MW_PEAK   NUMBER(8,2),
XPOS   NUMBER(9,1),
YPOS   NUMBER(9,1)
);




create table X$POSTE
(
ADDRESS   VARCHAR2(50),
ASSEMBLY   VARCHAR2(16),
CODE   VARCHAR2(16),
DESCRIPTIO   VARCHAR2(50),
OWNER   VARCHAR2(30),
PICTURE   VARCHAR2(80),
POBLACION   VARCHAR2(1),
PROJECT   VARCHAR2(16),
TOWNER   VARCHAR2(2),
USER_   VARCHAR2(50),
DEP_ID   NUMBER(2),
LATITUD   NUMBER(15,8),
LONGITUD   NUMBER(15,8),
MUN_ID   NUMBER(10),
XPOS   NUMBER(9,1),
YPOS   NUMBER(9,1)
);

create table X$CONECTIVIDAD
(
  kvnom      NUMBER(9,2),
  ifalla     VARCHAR2(16),
  emedida    VARCHAR2(16),
  pt         VARCHAR2(16),
  prayos     VARCHAR2(16),
  owner      VARCHAR2(30),
  towner     VARCHAR2(2),
  feeder     VARCHAR2(16),
  phnode     VARCHAR2(16),
  isconnecte NUMBER(1),
  spacing    VARCHAR2(16),
  height     NUMBER(6,1),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  fparent    VARCHAR2(16),
  tparent    VARCHAR2(16),
  phases     NUMBER(2),
  assembly   VARCHAR2(16),
  code       VARCHAR2(16)
);


create table X$arrendamiento
(
  code       VARCHAR2(16),
  phnode     VARCHAR2(16),
  height     NUMBER(10,2),
  company    VARCHAR2(16),
  type       VARCHAR2(16),
  cuadrilla  VARCHAR2(30)
);


create table X$TRANSFORMADOR
(
  address    VARCHAR2(50),
  autoprot   CHAR(2),
  caso       CHAR(20),
  code       VARCHAR2(16),
  date_fab   DATE,
  date_inst  DATE,
  date_rem   DATE,
  descriptio VARCHAR2(50),
  elnode     VARCHAR2(16),
  fparent    VARCHAR2(16),
  invnumber  VARCHAR2(15),
  lvelnode   VARCHAR2(16),
  marca      VARCHAR2(16),
  owner      VARCHAR2(30),
  owner1     VARCHAR2(1),
  owner2      VARCHAR2(30),
  phnode     VARCHAR2(16),
  picture    VARCHAR2(80),
  poblacion  VARCHAR2(1),
  serial     VARCHAR2(16),
  tipofuses  CHAR(5),
  tipo_red   VARCHAR2(1),
  tiposub    VARCHAR2(50),
  trftype    VARCHAR2(16),
  user_      VARCHAR2(50),
  valvula    CHAR(1),
  zone       VARCHAR2(100),
  culosses   NUMBER(5,2),
  dep_id     NUMBER(2),
  felosses   NUMBER(5,2),
  group_     NUMBER(1),
  impedance  NUMBER(5,2),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  municipio  NUMBER(3),
  ncalidad   NUMBER(1),
  pcv        NUMBER(1),
  phases     NUMBER(2),
  tipo_sub   NUMBER(2),
  project    VARCHAR2(16),
  num_trfs   NUMBER(1),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1)
);




create table X$BARRAJE
(
  address    VARCHAR2(50),
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  picture    VARCHAR2(80),
  project    VARCHAR2(16),
  user_      VARCHAR2(50),
  amp        NUMBER(7,1),
  kv         NUMBER(7,3),
  latitud    NUMBER(20,10),
  longitud   NUMBER(20,10),
  mva1ph_scc NUMBER(7,1),
  mva3ph_scc NUMBER(7,1),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  substation VARCHAR2(16),
  orientatio NUMBER(3)
);


create table X$CONDUCTOR_PRIMARIO
(
  address    VARCHAR2(50),
  code       VARCHAR2(16),
  conductor  VARCHAR2(16),
  descriptio VARCHAR2(50),
  elnode1    VARCHAR2(16),
  elnode2    VARCHAR2(16),
  fparent    VARCHAR2(16),
  guarda     VARCHAR2(16),
  neutral    VARCHAR2(16),
  owner      VARCHAR2(30),
  picture    VARCHAR2(80),
  poblacion  VARCHAR2(1),
  project    VARCHAR2(16),
  towner     VARCHAR2(2),
  user_      VARCHAR2(50),
  class      NUMBER(1),
  kvnom      NUMBER(7,3),
  lat1       NUMBER(20,10),
  lat2       NUMBER(20,10),
  length     NUMBER(8,1),
  lon1       NUMBER(20,10),
  lon2       NUMBER(20,10),
  phases     NUMBER(2),
  xpos1      NUMBER(9,1),
  xpos2      NUMBER(9,1),
  ypos1      NUMBER(9,1),
  ypos2      NUMBER(9,1),
  zpos1      NUMBER(10,2),
  zpos2      NUMBER(10,2),
  date_rem   DATE,
  date_      DATE
);

create table X$TRANSF_POT
(
  code      VARCHAR2(16),
  maker     VARCHAR2(16),
  press     NUMBER(10,2),
  relas     VARCHAR2(16),
  ractual   NUMBER(10,2),
  burden    NUMBER(10,2),
  clase     VARCHAR2(16),
  tipo      VARCHAR2(16),
  capacidad NUMBER(10,2),
  efase     NUMBER(10,2),
  emag      NUMBER(10,2),
  elnode    VARCHAR2(16),
  xpos      NUMBER(9,1),
  ypos      NUMBER(9,1)
);

create table X$FEEDERS
(
  code       VARCHAR2(16),
  descriptio VARCHAR2(50),
  address    VARCHAR2(50),
  isopen     NUMBER(1),
  assembly   VARCHAR2(16),
  user_      VARCHAR2(50),
  picture    VARCHAR2(80),
  phases     NUMBER(2),
  metercode  VARCHAR2(16),
  customers  NUMBER(5),
  fparent    VARCHAR2(16),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1),
  z          NUMBER(7,1),
  amp        NUMBER(6,1),
  ison       NUMBER(1),
  sourcebus  VARCHAR2(16),
  group_     NUMBER(1),
  owner      NUMBER(1),
  reclosin   NUMBER(1),
  type       NUMBER(1),
  radial     VARCHAR2(1),
  user_hist  VARCHAR2(150),
  prstate    NUMBER(3),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  link       NUMBER(1),
  sei        VARCHAR2(16)
);

create table X$CONDUCTOR_SECUNDARIO
(
  address    VARCHAR2(50),
  class      NUMBER(1),
  code       VARCHAR2(16),
  conductor  VARCHAR2(16),
  date_      DATE,
  date_rem   DATE,
  descriptio VARCHAR2(50),
  elnode1    VARCHAR2(16),
  elnode2    VARCHAR2(16),
  fparent    VARCHAR2(16),
  kvnom      NUMBER(6,3),
  length     NUMBER(8,1),
  neutral    VARCHAR2(16),
  owner      VARCHAR2(30),
  phases     NUMBER(2),
  picture    VARCHAR2(32),
  poblacion  VARCHAR2(1),
  project    VARCHAR2(16),
  towner     VARCHAR2(2),
  tparent    VARCHAR2(16),
  user_      VARCHAR2(50),
  uso        VARCHAR2(2),
  xpos1      NUMBER(9,1),
  xpos2      NUMBER(9,1),
  ypos1      NUMBER(9,1),
  ypos2      NUMBER(9,1),
  zpos1      NUMBER(10,2),
  zpos2      NUMBER(10,2),
  lat1       NUMBER(20,10),
  lat2       NUMBER(20,10),
  lon1       NUMBER(20,10),
  lon2       NUMBER(20,10)
);

create table X$PARARRAYOS
(
  bil     NUMBER(10,2),
  code    VARCHAR2(16),
  clase   VARCHAR2(16),
  inom    NUMBER(10,2),
  vcebado NUMBER(10,2),
  vmax    NUMBER(10,2),
  vnom    NUMBER(10,2)
);


create table X$CAJA_DISTRIBUICION
(
  address    VARCHAR2(50),
  assembly   VARCHAR2(16),
  code       VARCHAR2(16),
  dep_id     NUMBER(2),
  descriptio VARCHAR2(50),
  latitud    NUMBER(15,8),
  longitud   NUMBER(15,8),
  mun_id     NUMBER(10),
  owner      VARCHAR2(30),
  picture    VARCHAR2(50),
  poblacion  VARCHAR2(1),
  project    VARCHAR2(16),
  towner     VARCHAR2(2),
  user_      VARCHAR2(50),
  xpos       NUMBER(9,1),
  ypos       NUMBER(9,1)
);


--***************************************************************************************
--Actualizar secuencias
--***************************************************************************************
begin
  execute immediate 'drop sequence etl_carga_seq';
exception
  when others then
    null;
end;
/

create sequence etl_carga_seq start with 100 increment by 1;


--***************************************************************************************
--CREAR INDICES PARA OPTIMIZAR EL PROCESO DE TRANSFORMACIONS
--***************************************************************************************

declare
  type tableArrays is table of varchar2(60);
  tables tableArrays := tableArrays('AREDES.idx_mvpho_code',
                                    'AREDES.idx_lvpho_code',
                                    'AREDES.idx_lvel_code',
                                    'AREDES.idx_lvel_phnode',
                                    'AREDES.idx_mvel_code',
                                    'AREDES.idx_mvel_phnode',
                                    'idx_etl_c2f_a',
                                    'idx_x$conn_fp',
                                    'idx_chec_nodo_code');
begin
  for i in 1 .. tables.count loop
    begin
      execute immediate 'drop index ' || tables(i);
    exception
      when others then
        null;
    end;
  
  end loop;
end;
/

create index AREDES.idx_mvpho_code on aredes.MVPHNODE(CODE);
create index AREDES.idx_lvpho_code on aredes.LVPHNODE(CODE);
create index AREDES.idx_lvel_code on aredes.LVELNODE(CODE);
create index AREDES.idx_lvel_phnode on aredes.LVELNODE(PHNODE);
create index AREDES.idx_mvel_code on aredes.MVELNODE(CODE);
create index AREDES.idx_mvel_phnode on aredes.MVELNODE(PHNODE);
create index idx_etl_c2f_a on etl_code2fid(code, g3e_fno);
create index idx_x$conn_fp on x$conectividad (phnode,fparent);
create index idx_chec_nodo_code on CHEC_NODOS_ELEC(code);
--***************************************************************************************
--CREAR VISTA DE CONTROL
--***************************************************************************************

create or replace view etl_chec_resumen as
select c.fparent,
       decode(count(ext.circuito), 0, 'NO', 'SI') EXTRACION,
       decode(count(tra.circuito), 0, 'NO', 'SI') TRANSFORMACION,
       decode(count(car.circuito), 0, 'NO', 'SI') CARGA,
       decode(count(val.circuito), 0, 'NO', 'SI') VALIDACION
  from mvelnode c
  full join etl_extraccion_report ext
    on ext.circuito = c.fparent
  full join etl_transformacion_report tra
    on tra.circuito = c.fparent
  full join etl_carga_report car
    on car.circuito = c.fparent
  full join (select distinct circuito from etl_validation_log) val
    on val.circuito = c.fparent
 group by c.fparent
 order by c.fparent;
/

--***************************************************************************************
--CREAR SINONIMOS PUBLICOS
--***************************************************************************************

create or replace public synonym CUSTMETR for AREDES.CUSTMETR;
create or replace public synonym FEEDERS for AREDES.FEEDERS;
create or replace public synonym FUSES for AREDES.FUSES;
create or replace public synonym GENERATR for AREDES.GENERATR;
create or replace public synonym IFALLA for AREDES.IFALLA;
create or replace public synonym LVELNODE for AREDES.LVELNODE;
create or replace public synonym LVLINSEC for AREDES.LVLINSEC;
create or replace public synonym LVPHNODE for AREDES.LVPHNODE;
create or replace public synonym MVELNODE for AREDES.MVELNODE;
create or replace public synonym MVLINSEC for AREDES.MVLINSEC;
create or replace public synonym MVPHNODE for AREDES.MVPHNODE;
create or replace public synonym NETOWNERS for AREDES.NETOWNERS;
create or replace public synonym PRAYOS for AREDES.PRAYOS;
create or replace public synonym PT for AREDES.PT;
create or replace public synonym SRCBUSES for AREDES.SRCBUSES;
create or replace public synonym STREETLG for AREDES.STREETLG;
create or replace public synonym SUBSTATI for AREDES.SUBSTATI;
create or replace public synonym SWITCHES for AREDES.SWITCHES;
create or replace public synonym TRANSFOR for AREDES.TRANSFOR;
create or replace public synonym CONDUCTO for AREDES.CONDUCTO;
create or replace public synonym TRFTYPES for AREDES.TRFTYPES;


create or replace public synonym X$AISLADERO for GENERGIA.X$AISLADERO;
create or replace public synonym X$ARRENDAMIENTO for GENERGIA.X$ARRENDAMIENTO;
create or replace public synonym X$BARRAJE for GENERGIA.X$BARRAJE;
create or replace public synonym X$CAJA_DISTRIBUICION for GENERGIA.X$CAJA_DISTRIBUICION;
create or replace public synonym X$CAMARA for GENERGIA.X$CAMARA;
create or replace public synonym X$CONDUCTOR_PRIMARIO for GENERGIA.X$CONDUCTOR_PRIMARIO;
create or replace public synonym X$CONDUCTOR_SECUNDARIO for GENERGIA.X$CONDUCTOR_SECUNDARIO;
create or replace public synonym X$CONDUCTOR_TRANSMISION for GENERGIA.X$CONDUCTOR_TRANSMISION;
create or replace public synonym X$CONECTIVIDAD for GENERGIA.X$CONECTIVIDAD;
create or replace public synonym X$CUCHILLA for GENERGIA.X$CUCHILLA;
create or replace public synonym X$FEEDERS for GENERGIA.X$FEEDERS;
create or replace public synonym X$INDICADOR_FALLA for GENERGIA.X$INDICADOR_FALLA;
create or replace public synonym X$INTERRUPTOR for GENERGIA.X$INTERRUPTOR;
create or replace public synonym X$LUMINARIA for GENERGIA.X$LUMINARIA;
create or replace public synonym X$PARARRAYOS for GENERGIA.X$PARARRAYOS;
create or replace public synonym X$POSTE for GENERGIA.X$POSTE;
create or replace public synonym X$RECONECTADOR for GENERGIA.X$RECONECTADOR;
create or replace public synonym X$REFERENCIA for GENERGIA.X$REFERENCIA;
create or replace public synonym X$SECCIONALIZADOR for GENERGIA.X$SECCIONALIZADOR;
create or replace public synonym X$SUBESTACION for GENERGIA.X$SUBESTACION;
create or replace public synonym X$SUICHE for GENERGIA.X$SUICHE;
create or replace public synonym X$TORRE_TRANSMISION for GENERGIA.X$TORRE_TRANSMISION;
create or replace public synonym X$TRANSFORMADOR for GENERGIA.X$TRANSFORMADOR;
create or replace public synonym X$TRANSFORMADOR_BKP for GENERGIA.X$TRANSFORMADOR_BKP;
create or replace public synonym X$TRANSF_POT for GENERGIA.X$TRANSF_POT;
create or replace public synonym ETL_CARGA_LOG for GENERGIA.ETL_CARGA_LOG;
create or replace public synonym ETL_CARGA_REPORT for GENERGIA.ETL_CARGA_REPORT;
create or replace public synonym ETL_CODE2FID for GENERGIA.ETL_CODE2FID;
create or replace public synonym ETL_EXTRACCION_LOG for GENERGIA.ETL_EXTRACCION_LOG;
create or replace public synonym ETL_EXTRACCION_REPORT for GENERGIA.ETL_EXTRACCION_REPORT;
create or replace public synonym ETL_INDEXES for GENERGIA.ETL_INDEXES;
create or replace public synonym ETL_NORMAS for GENERGIA.ETL_NORMAS;
create or replace public synonym ETL_PICKLISTS for GENERGIA.ETL_PICKLISTS;
create or replace public synonym ETL_TRANSFORMACION_LOG for GENERGIA.ETL_TRANSFORMACION_LOG;
create or replace public synonym ETL_TRANSFORMACION_REPORT for GENERGIA.ETL_TRANSFORMACION_REPORT;
create or replace public synonym ETL_VALIDATION_LOG for GENERGIA.ETL_VALIDATION_LOG;
create or replace public synonym etl_chec_resumen for GENERGIA.etl_chec_resumen;
--***************************************************************************************
--CREAR PERMISOS
--***************************************************************************************
grant select, update, delete on ETL_CARGA_LOG to GDESIGNER;
grant select, update, delete on ETL_CARGA_REPORT to GDESIGNER;
grant select, update, delete on ETL_CODE2FID to GDESIGNER;
grant select, update, delete on ETL_EXTRACCION_LOG to GDESIGNER;
grant select, update, delete on ETL_EXTRACCION_REPORT to GDESIGNER;
grant select, update, delete on ETL_INDEXES to GDESIGNER;
grant select, update, delete on ETL_NORMAS to GDESIGNER;
grant select, update, delete on ETL_PICKLISTS to GDESIGNER;
grant select, update, delete on ETL_TRANSFORMACION_LOG to GDESIGNER;
grant select, update, delete on ETL_TRANSFORMACION_REPORT to GDESIGNER;
grant select, update, delete on ETL_VALIDATION_LOG to GDESIGNER;
grant select, update, delete on X$AISLADERO to GDESIGNER;
grant select, update, delete on X$ARRENDAMIENTO to GDESIGNER;
grant select, update, delete on X$BARRAJE to GDESIGNER;
grant select, update, delete on X$CAJA_DISTRIBUICION to GDESIGNER;
grant select, update, delete on X$CAMARA to GDESIGNER;
grant select, update, delete on X$CONDUCTOR_PRIMARIO to GDESIGNER;
grant select, update, delete on X$CONDUCTOR_SECUNDARIO to GDESIGNER;
grant select, update, delete on X$CONDUCTOR_TRANSMISION to GDESIGNER;
grant select, update, delete on X$CONECTIVIDAD to GDESIGNER;
grant select, update, delete on X$CUCHILLA to GDESIGNER;
grant select, update, delete on X$FEEDERS to GDESIGNER;
grant select, update, delete on X$INDICADOR_FALLA to GDESIGNER;
grant select, update, delete on X$INTERRUPTOR to GDESIGNER;
grant select, update, delete on X$LUMINARIA to GDESIGNER;
grant select, update, delete on X$PARARRAYOS to GDESIGNER;
grant select, update, delete on X$POSTE to GDESIGNER;
grant select, update, delete on X$RECONECTADOR to GDESIGNER;
grant select, update, delete on X$REFERENCIA to GDESIGNER;
grant select, update, delete on X$SECCIONALIZADOR to GDESIGNER;
grant select, update, delete on X$SUBESTACION to GDESIGNER;
grant select, update, delete on X$SUICHE to GDESIGNER;
grant select, update, delete on X$TORRE_TRANSMISION to GDESIGNER;
grant select, update, delete on X$TRANSFORMADOR to GDESIGNER;
grant select, update, delete on X$TRANSFORMADOR_BKP to GDESIGNER;
grant select, update, delete on X$TRANSF_POT to GDESIGNER;
