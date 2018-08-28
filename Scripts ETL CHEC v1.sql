
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


--***************************************************************************************
--ETL_CREARTABLASINTERMEDIAS
--***************************************************************************************

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
    
      execute immediate 'create or replace public synonym T$' ||
                        c.g3e_table || ' for GENERGIA.T$' || c.g3e_table;
    
      execute immediate 'grant select, update, delete on T$' || c.g3e_table ||
                        ' to GDESIGNER';
    
    exception
      when others then
        dbms_output.put_line('Error al crear la tabla T$' || c.g3e_table);
    end;
  
  end loop;
end;
/


execute ETL_CrearTablasIntermedias;


--***************************************************************************************
--ETL_EXTRACION
--***************************************************************************************

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


--***************************************************************************************
--ETL_TRANFORMACION
--***************************************************************************************

create or replace package ETL_TRANSFORMACION_CHEC is

  -- Author  : LTTURCHE
  -- Created : 09/04/2018 16:59:57
  -- Purpose :

  assembly_fisico_invalido   exception;
  assembly_eletrico_invalido exception;

  -- Public constant declarations
  cErrorPropietario      constant varchar2(80) := 'TOWNER no corresponde al OWNER';
  cErrorSinPropietario   constant varchar2(80) := 'Un OWNER es requerido para este elemento';
  cErrorAssemblyFisico   constant varchar2(80) := 'Assembly Fisico invalido - ';
  cErrorAssemblyEletrico constant varchar2(80) := 'Assembly Eletrico invalido - ';
  cErrorMaterial         constant varchar2(80) := 'Material es requerido para este elemento';
  cErrorDepMun           constant varchar2(80) := 'Departamento/Municipio es requerido';
  cErrorAssemblyNotFound constant varchar2(80) := 'Assembly Eletrico no encontrado';
  cErrorVientoPrimario   constant varchar2(80) := 'Norma de Viento Primario no encontrada - ';
  cErrorVientoSecundario constant varchar2(80) := 'Norma de Viento Secundario no encontrada - ';
  cErrorApoyos           constant varchar2(80) := 'Norma de Apoyo no encontrada - ';
  cErrorTierra           constant varchar2(80) := 'Norma de Tierra no encontrada - ';
  cErrorUser             constant varchar2(80) := 'No es posible hacer el desglose del campo USER';
  cErrorGeneral          constant varchar2(80) := 'Error General';

  function BuscarZetaNodoEletrico(pCodigo varchar) return number;

  function ConvertPoint2Sigma(longitud number,
                              latitud  number,
                              zeta     number default 0) return sdo_geometry;

  function ConvertPoint2SigmaTexto(longitud number,
                                   latitud  number,
                                   zeta     number default 0,
                                   offset   number default 1E-5)
    return sdo_geometry;

  function ConvertLine2Sigma(longitud1 number,
                             latitud1  number,
                             zeta1     number,
                             longitud2 number,
                             latitud2  number,
                             zeta2     number) return sdo_geometry;

  function ConvertPoint2Chec(longitud number, latitud number)
    return sdo_geometry;

  procedure ConvertLine2Chec(geometry sdo_geometry,
                             X1       in out number,
                             Y1       in out number,
                             X2       in out number,
                             Y2       in out number);

  function ConvertPoint2Barraje(longitud    number,
                                latitud     number,
                                orientacion number) return sdo_geometry;

  function ConvertPoint2Subestacion(longitud number,
                                    latitud  number,
                                    distance number default 10)
    return sdo_geometry;

  function SpatialDepartamento(geom     sdo_geometry,
                               code     varchar2,
                               tabla    varchar2,
                               circuito varchar2) return varchar2;
  function SpatialMunicipio(geom     sdo_geometry,
                            code     varchar2,
                            tabla    varchar2,
                            circuito varchar2) return varchar2;
  function SpatialSubregion(geom     sdo_geometry,
                            code     varchar2,
                            tabla    varchar2,
                            circuito varchar2) return varchar2;

  function SpatialClaMercado(geom     sdo_geometry,
                             code     varchar2,
                             tabla    varchar2,
                             circuito varchar2) return varchar2;

  function SpatialVereda(geom     sdo_geometry,
                         code     varchar2,
                         tabla    varchar2,
                         circuito varchar2) return varchar2;

  function DecodeFase(fase in NUMBER) return varchar2;
  function DecodeTipoConductorSec(pUso in VARCHAR2) return varchar2;

  procedure DesgloseAerMVPHNODE(assembly            in VARCHAR2,
                                material            in out VARCHAR2,
                                apoyos              in out NUMBER,
                                longitud            in out NUMBER,
                                vientos_primarios   in out VARCHAR2,
                                vientos_secundarios in out NUMBER,
                                tierra              in out VARCHAR2);

  procedure DesgloseSubMVPHNODE(assembly         in VARCHAR2,
                                dimension        in out VARCHAR2,
                                tipo             in out VARCHAR2,
                                ductos           in out NUMBER,
                                dimension_ductos in out VARCHAR2);

  procedure DesglosePriMVELNODE(assembly         in VARCHAR2,
                                tipo_red         in out NUMBER,
                                material         in out VARCHAR2,
                                posicion         in out VARCHAR2,
                                neutro           in out VARCHAR2,
                                fases            in out NUMBER,
                                tipo_estructura  in out VARCHAR2,
                                longitud_cruceta in out VARCHAR2);

  procedure DesgloseSecMVELNODE(assembly         in VARCHAR2,
                                material         in out VARCHAR2,
                                disposicion      in out VARCHAR2,
                                neutro           in out VARCHAR2,
                                fases            in out NUMBER,
                                perchas          in out VARCHAR2,
                                cantidad_perchas in out NUMBER);

  --GENERICOS
  --procedure INS_COMMUN(pCommu IN T$CCOMUN%ROWTYPE);

  --MVPHNODE
  procedure PT_CAMARA(pCircuito in VARCHAR2);
  procedure PT_TORRE_TRANSMISION(pCircuito in VARCHAR2);
  procedure PT_POSTE(pCircuito in VARCHAR2);
  procedure PT_NODO_CONDUCTOR(pCircuito in VARCHAR2);

  --SWICHES
  procedure PT_AISLADERO(pCircuito in VARCHAR2);
  procedure PT_CUCHILLA(pCircuito in VARCHAR2);
  procedure PT_RECONECTADOR(pCircuito in VARCHAR2);
  procedure PT_SECCIONALIZADOR(pCircuito in VARCHAR2);
  procedure PT_REFERENCIA(pCircuito in VARCHAR2);
  procedure PT_SUICHE(pCircuito in VARCHAR2);
  procedure PT_INTERRUPTOR(pCircuito in VARCHAR2);

  --MVLINSEC
  procedure PT_CONDUCTOR_PRIMARIO(pCircuito in VARCHAR2);
  procedure PT_CONDUCTOR_TRANSMISION(pCircuito in VARCHAR2);

  --LVLINSEC
  procedure PT_CONDUCTOR_SECUNDARIO(pCircuito in VARCHAR2);

  procedure PT_BARRAJE(pCircuito in VARCHAR2);
  procedure PT_SUBESTACION(pCircuito in VARCHAR2);

  procedure PT_TRANSF_POT(pCircuito in varchar2);
  procedure PT_TRANSFORMADOR(pCircuito in VARCHAR2);

  procedure PT_PARARRAYO(pCircuito in VARCHAR2);
  procedure PT_INDICADOR(pCircuito in VARCHAR2);
  procedure PT_FEEDER(pCircuito in VARCHAR2);
  procedure PT_LUMINARIA(pCircuito in VARCHAR2);
  procedure PT_CAJA_DIS(pCircuito in VARCHAR2);

  procedure PT_FLUJO_CIR(pCircuito in VARCHAR2);

  --Normas Eletricas
  procedure PTN_NORMA_ELETRICA(pPHNODE in VARCHAR2,
                               pFID    in NUMBER,
                               pFNO    in NUMBER);

  --Connectividad
  function PTC_BuscarNodoConductor(pCodigo in VARCHAR2, pNodo in VARCHAR2)
    return number;
  function PTC_BuscarNodoConductorSec(pCodigo in VARCHAR2,
                                      pNodo   in VARCHAR2) return number;

  function PTC_BuscarNodoSwitches(pCodigo      in VARCHAR2,
                                  pLineSection in VARCHAR2,
                                  pX           in NUMBER,
                                  pY           in NUMBER,
                                  fidConduct   in out NUMBER,
                                  nodePos      in out NUMBER) return number;

  function PTC_BuscarNodoTransforAlta(pHNode in VARCHAR2) return number;
  function PTC_BuscarNodoTransforBaja(pLNode in VARCHAR2) return number;
  function PTC_BuscarOwnerIndicador(elnode in VARCHAR) return number;

  procedure CrearReport(pCircuito  in varchar2,
                        pTabla     in varchar2,
                        pRegistros in number,
                        pTiempo    in timestamp);

  procedure PoblarNodosEletricos;

end ETL_TRANSFORMACION_CHEC;
/
create or replace package body ETL_TRANSFORMACION_CHEC is

  function BuscarZetaNodoEletrico(pCodigo varchar) return number is
  
    gZeta number;
  begin
    select height into gZeta from x$conectividad where code = pCodigo;
    if gZeta is null then
      gZeta := 0;
    end if;
    return gZeta;
  exception
    when others then
      return 0;
  end;

  function ConvertPoint2Sigma(longitud number,
                              latitud  number,
                              zeta     number default 0) return sdo_geometry is
    geom21891 sdo_geometry;
    geom4326  sdo_geometry;
    gLongitud number;
    gLatitud  number;
    gZeta     number;
  begin
    geom21891 := SDO_GEOMETRY(3001,
                              21891,
                              NULL,
                              SDO_ELEM_INFO_ARRAY(1, 1, 1, 4, 1, 0),
                              SDO_ORDINATE_ARRAY(longitud,
                                                 latitud,
                                                 zeta,
                                                 0,
                                                 0,
                                                 0));
  
    geom4326 := SDO_CS.TRANSFORM(geom21891, 4326);
  
    select k.x
      into gLongitud
      from table(sdo_util.GetVertices(geom4326)) k
     where k.id = 1;
  
    select k.y
      into gLatitud
      from table(sdo_util.GetVertices(geom4326)) k
     where k.id = 1;
  
    select k.z
      into gZeta
      from table(sdo_util.GetVertices(geom4326)) k
     where k.id = 1;
  
    geom4326.SDO_ORDINATES := sdo_ordinate_array(gLongitud,
                                                 gLatitud,
                                                 gZeta,
                                                 0,
                                                 0,
                                                 0);
  
    return geom4326;
  end;

  function ConvertLine2Sigma(longitud1 number,
                             latitud1  number,
                             zeta1     number,
                             longitud2 number,
                             latitud2  number,
                             zeta2     number) return sdo_geometry is
  
    geom21891 sdo_geometry;
    geom4326  sdo_geometry;
  begin
    geom21891 := SDO_GEOMETRY(3002,
                              21891,
                              NULL,
                              SDO_ELEM_INFO_ARRAY(1, 2, 1),
                              SDO_ORDINATE_ARRAY(longitud1,
                                                 latitud1,
                                                 zeta1,
                                                 longitud2,
                                                 latitud2,
                                                 zeta2));
  
    geom4326 := SDO_CS.TRANSFORM(geom21891, 4326);
    return geom4326;
  end;

  function ConvertPoint2Chec(longitud number, latitud number)
    return sdo_geometry is
    geom21891 sdo_geometry;
    geom4326  sdo_geometry;
  begin
    geom4326 := SDO_GEOMETRY(2001,
                             4326,
                             NULL,
                             SDO_ELEM_INFO_ARRAY(1, 1, 1),
                             SDO_ORDINATE_ARRAY(longitud, latitud));
  
    geom21891 := SDO_CS.TRANSFORM(geom4326, 21891);
  
    return geom21891;
  end;

  procedure ConvertLine2Chec(geometry sdo_geometry,
                             X1       in out number,
                             Y1       in out number,
                             X2       in out number,
                             Y2       in out number) is
    geom21891 sdo_geometry;
  begin
    geom21891 := SDO_CS.TRANSFORM(geometry, 21891);
    select inicial.xinicial, inicial.yinicial
      into X1, Y1
      from (select l.x xinicial, l.y yinicial
              from table(sdo_util.GetVertices(geom21891)) l
             where l.id =
                   (select min(j.id)
                      from table(sdo_util.GetVertices(geom21891)) j)) inicial;
  
    select final.xfinal, final.yfinal
      into X2, Y2
      from (select l.x xfinal, l.y yfinal
              from table(sdo_util.GetVertices(geom21891)) l
             where l.id =
                   (select max(j.id)
                      from table(sdo_util.GetVertices(geom21891)) j)) final;
  
  end;

  function ConvertPoint2SigmaTexto(longitud number,
                                   latitud  number,
                                   zeta     number default 0,
                                   offset   number default 1E-5)
    return sdo_geometry is
  
    point4326 sdo_geometry;
    xpos      number;
    ypos      number;
  begin
  
    point4326 := ConvertPoint2Sigma(longitud, latitud);
  
    xpos := point4326.SDO_ORDINATES(1);
    ypos := point4326.SDO_ORDINATES(2);
  
    point4326.SDO_ORDINATES := sdo_ordinate_array(xpos,
                                                  ypos + offset,
                                                  0,
                                                  0,
                                                  0,
                                                  0);
  
    return point4326;
  
  end;

  function ConvertPoint2Barraje(longitud    number,
                                latitud     number,
                                orientacion number) return sdo_geometry is
    point4326 sdo_geometry;
    line4326  sdo_geometry;
    xpos      number;
    ypos      number;
  begin
  
    point4326 := ConvertPoint2Sigma(longitud, latitud);
  
    xpos := point4326.SDO_ORDINATES(1);
    ypos := point4326.SDO_ORDINATES(2);
  
    if orientacion = 0 then
      --horizontal
      line4326 := SDO_GEOMETRY(3002,
                               4326,
                               NULL,
                               SDO_ELEM_INFO_ARRAY(1, 2, 1),
                               SDO_ORDINATE_ARRAY(xpos - 2E-5,
                                                  ypos,
                                                  0,
                                                  xpos + 2E-5,
                                                  ypos,
                                                  0));
    else
      line4326 := SDO_GEOMETRY(3002,
                               4326,
                               NULL,
                               SDO_ELEM_INFO_ARRAY(1, 2, 1),
                               SDO_ORDINATE_ARRAY(xpos,
                                                  ypos - 2E-5,
                                                  0,
                                                  xpos,
                                                  ypos + 2E-5,
                                                  0));
    end if;
  
    return line4326;
  
  end;

  function ConvertPoint2Subestacion(longitud number,
                                    latitud  number,
                                    distance number default 10)
    return sdo_geometry is
  
    point4326 sdo_geometry;
    area4326  sdo_geometry;
  
    xpos     number;
    ypos     number;
    ortoDist number;
    delta    number;
  begin
  
    ortoDist := distance / sqrt(2);
    delta    := ortoDist * 0.001;
  
    point4326 := ConvertPoint2Sigma(longitud, latitud);
  
    xpos := point4326.SDO_ORDINATES(1);
    ypos := point4326.SDO_ORDINATES(2);
  
    area4326 := SDO_GEOMETRY(3003,
                             4326,
                             NULL,
                             SDO_ELEM_INFO_ARRAY(1, 1003, 1),
                             SDO_ORDINATE_ARRAY(xpos - delta,
                                                ypos + delta,
                                                0,
                                                xpos + delta,
                                                ypos + delta,
                                                0,
                                                xpos + delta,
                                                ypos - delta,
                                                0,
                                                xpos - delta,
                                                ypos - delta,
                                                0,
                                                xpos - delta,
                                                ypos + delta,
                                                0));
  
    return area4326;
  
  end;

  function SpatialDepartamento(geom     sdo_geometry,
                               code     varchar2,
                               tabla    varchar2,
                               circuito varchar2) return varchar2 is
    dept varchar2(80);
  begin
    select departamento
      into dept
      from BDEPART_AR k
     where SDO_RELATE(k.g3e_geometry, geom, 'mask=CONTAINS') = 'TRUE'
       and rownum = 1;
  
    return dept;
  exception
    when others then
      insert into ETL_TRANSFORMACION_LOG
      values
        (tabla,
         'T$CCOMUN',
         code,
         2,
         'No se encontró un departamento',
         circuito,
         sysdate);
      commit;
      return null;
  end;

  function SpatialMunicipio(geom     sdo_geometry,
                            code     varchar2,
                            tabla    varchar2,
                            circuito varchar2) return varchar2 is
    muni varchar2(80);
  begin
    select municipio
      into muni
      from BMUNICIP_AR k
     where SDO_RELATE(k.g3e_geometry, geom, 'mask=CONTAINS') = 'TRUE'
       and rownum = 1;
  
    return muni;
  exception
    when others then
      insert into ETL_TRANSFORMACION_LOG
      values
        (tabla,
         'T$CCOMUN',
         code,
         2,
         'No se encontró un municipio',
         circuito,
         sysdate);
      commit;
      return null;
  end;

  function SpatialSubregion(geom     sdo_geometry,
                            code     varchar2,
                            tabla    varchar2,
                            circuito varchar2) return varchar2 is
    narea varchar2(80);
  begin
    select a.nombre_area
      into narea
      from EARE_FUN_AR g
     inner join EARE_FUN_AT a
        on a.g3e_fid = g.g3e_fid
     where a.tipo_area = 'SUBREGION'
       and SDO_RELATE(g.g3e_geometry, geom, 'mask=CONTAINS') = 'TRUE'
       and rownum = 1;
  
    return narea;
  exception
    when others then
      insert into ETL_TRANSFORMACION_LOG
      values
        (tabla,
         'T$CCOMUN',
         code,
         3,
         'No se encontró una subregion',
         circuito,
         sysdate);
      commit;
      return null;
  end;

  function SpatialClaMercado(geom     sdo_geometry,
                             code     varchar2,
                             tabla    varchar2,
                             circuito varchar2) return varchar2 is
    narea varchar2(80);
  begin
    select a.nombre_area
      into narea
      from EARE_FUN_AR g
     inner join EARE_FUN_AT a
        on a.g3e_fid = g.g3e_fid
     where a.tipo_area = 'URBANA'
       and SDO_RELATE(g.g3e_geometry, geom, 'mask=CONTAINS') = 'TRUE'
       and rownum = 1;
  
    return narea;
  exception
    when others then
      insert into ETL_TRANSFORMACION_LOG
      values
        (tabla,
         'T$CCOMUN',
         code,
         3,
         'No se encontró una Clase Mercado',
         circuito,
         sysdate);
      commit;
      return null;
  end;

  function SpatialVereda(geom     sdo_geometry,
                         code     varchar2,
                         tabla    varchar2,
                         circuito varchar2) return varchar2 is
    vere varchar2(80);
  begin
    select g.vereda
      into vere
      from BVEREDA_AR g
     where SDO_RELATE(g.g3e_geometry, geom, 'mask=CONTAINS') = 'TRUE'
       and rownum = 1;
  
    return vere;
  exception
    when others then
      insert into ETL_TRANSFORMACION_LOG
      values
        (tabla,
         'T$CCOMUN',
         code,
         3,
         'No se encontró una Vereda',
         circuito,
         sysdate);
      commit;
      return null;
  end;

  function DecodeFase(fase in NUMBER) return varchar2 is
  
  begin
    case fase
      when 1 then
        return 'A';
      when 2 then
        return 'B';
      when 3 then
        return 'AB';
      when 4 then
        return 'C';
      when 5 then
        return 'AC';
      when 6 then
        return 'BC';
      when 7 then
        return 'ABC';
      when 8 then
        return '';
      when 9 then
        return 'AN';
      when 10 then
        return 'BN';
      when 11 then
        return 'ABN';
      when 12 then
        return 'CN';
      when 13 then
        return 'ACN';
      when 14 then
        return 'BCN';
      when 15 then
        return 'ABCN';
      else
        return '';
    end case;
  end;

  function DecodeTipoConductorSec(pUso in VARCHAR2) return varchar2 is
  
  begin
    case pUso
      when 'MX' then
        return 'MISTA';
      when 'AP' then
        return 'ALUMBRADO PUBLICO';
      when 'AC' then
        return 'ACOMETIDA';
      when 'RD' then
        return 'DISTRIBUCION';
      else
        return '';
    end case;
  end;

  procedure DesgloseAerMVPHNODE(assembly            in VARCHAR2,
                                material            in out VARCHAR2,
                                apoyos              in out NUMBER,
                                longitud            in out NUMBER,
                                vientos_primarios   in out VARCHAR2,
                                vientos_secundarios in out NUMBER,
                                tierra              in out VARCHAR2) is
  
  begin
    material            := substr(assembly, 2, 1);
    apoyos              := to_number(substr(assembly, 3, 1));
    longitud            := to_number(substr(assembly, 4, 2));
    vientos_primarios   := substr(assembly, 6, 2);
    vientos_secundarios := to_number(substr(assembly, 8, 1));
  
    if length(assembly) = 9 then
      tierra := substr(assembly, 9, 1);
    else
      tierra := '0';
    end if;
  
  exception
    when others then
      raise assembly_fisico_invalido;
  end;

  procedure DesgloseSubMVPHNODE(assembly         in VARCHAR2,
                                dimension        in out VARCHAR2,
                                tipo             in out VARCHAR2,
                                ductos           in out NUMBER,
                                dimension_ductos in out VARCHAR2) is
  
  begin
    dimension        := substr(assembly, 3, 1);
    tipo             := substr(assembly, 4, 1);
    ductos           := to_number(substr(assembly, 5, 2));
    dimension_ductos := substr(assembly, 7, 1);
  
  exception
    when others then
      raise assembly_fisico_invalido;
  end;

  procedure DesglosePriMVELNODE(assembly         in VARCHAR2,
                                tipo_red         in out NUMBER,
                                material         in out VARCHAR2,
                                posicion         in out VARCHAR2,
                                neutro           in out VARCHAR2,
                                fases            in out NUMBER,
                                tipo_estructura  in out VARCHAR2,
                                longitud_cruceta in out VARCHAR2) is
  
  begin
  
    tipo_red         := to_number(substr(assembly, 1, 1));
    material         := substr(assembly, 2, 1);
    posicion         := substr(assembly, 3, 1);
    neutro           := substr(assembly, 4, 1);
    fases            := to_number(substr(assembly, 5, 1));
    tipo_estructura  := substr(assembly, 6, 1);
    longitud_cruceta := substr(assembly, 7);
  
  exception
    when others then
      raise assembly_eletrico_invalido;
  end;

  procedure DesgloseSecMVELNODE(assembly         in VARCHAR2,
                                material         in out VARCHAR2,
                                disposicion      in out VARCHAR2,
                                neutro           in out VARCHAR2,
                                fases            in out NUMBER,
                                perchas          in out VARCHAR2,
                                cantidad_perchas in out NUMBER) is
  
  begin
  
    material         := substr(assembly, 2, 1);
    disposicion      := substr(assembly, 3, 1);
    neutro           := substr(assembly, 4, 1);
    fases            := to_number(substr(assembly, 5, 1));
    perchas          := substr(assembly, 6, 1);
    cantidad_perchas := to_number(substr(assembly, 7));
  
  exception
    when others then
      raise assembly_eletrico_invalido;
  end;

  procedure PT_CAMARA(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN     T$CCOMUN%ROWTYPE;
    vCARREN     T$CARRENDAMIENTO%ROWTYPE;
    vPROP       T$CPROPIETARIO%ROWTYPE;
    vCONN       T$CCONECTIVIDAD_ES%ROWTYPE;
    vECAMARA_AT T$ECAMARA_AT%ROWTYPE;
    vECAMARA_PT T$ECAMARA_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 17400;
  
    vCount number(5);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$CAMARA';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from x$camara) loop
    
      select count(1)
        into vCount
        from x$conectividad
       where phnode = c.code
         and fparent = pCircuito;
    
      if vCount = 0 then
        continue;
      end if;
    
      select count(1)
        into vCount
        from etl_code2fid
       where code = c.code
         and g3e_fno = fno;
    
      if vCount > 0 then
        select g3e_fid
          into fid
          from etl_code2fid
         where code = c.code
           and g3e_fno = fno;
      
        insert into ETL_CODE2FID
        values
          (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      
        commit;
        continue;
      end if;
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_ID         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECAMARA_AT
      begin
        vECAMARA_AT := null;
      
        DesgloseSubMVPHNODE(c.assembly,
                            vECAMARA_AT.DIMENSIONES,
                            vECAMARA_AT.CLA_CAMARA,
                            vECAMARA_AT.CANTIDAD_DUCTOS,
                            vECAMARA_AT.DIMENSION_DUCTO);
      
        if vECAMARA_AT.CLA_CAMARA = 'P' then
          vECAMARA_AT.CLA_CAMARA := 'DE PASO';
        else
          vECAMARA_AT.CLA_CAMARA := 'DE EQUIPO';
        end if;
      
        vECAMARA_AT.NRO_CAMARA := c.CODE;
        vECAMARA_AT.GRUPO      := 'ESTRUCTURAS EYT';
      
        vECAMARA_AT.G3E_FNO := fno;
        vECAMARA_AT.G3E_CNO := fno + 1;
        vECAMARA_AT.G3E_CID := 1;
        vECAMARA_AT.G3E_FID := fid;
        vECAMARA_AT.G3E_id  := ECAMARA_AT_SEQ.NEXTVAL;
      
        insert into T$ECAMARA_AT values vECAMARA_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECAMARA_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECAMARA_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECAMARA_PT
      vECAMARA_PT              := null;
      vECAMARA_PT.G3E_FNO      := fno;
      vECAMARA_PT.G3E_CNO      := fno + 10;
      vECAMARA_PT.G3E_CID      := 1;
      vECAMARA_PT.G3E_FID      := fid;
      vECAMARA_PT.G3E_ID       := ECAMARA_PT_SEQ.NEXTVAL;
      vECAMARA_PT.G3E_GEOMETRY := geom;
    
      insert into T$ECAMARA_PT values vECAMARA_PT;
    
      --CCONECTIVIDAD_ES
      --vCONN
      vCONN             := null;
      vCONN.G3E_FNO     := fno;
      vCONN.G3E_CNO     := 68;
      vCONN.G3E_CID     := 1;
      vCONN.G3E_FID     := fid;
      vCONN.G3E_ID      := CCONECTIVIDAD_ES_SEQ.NEXTVAL;
      vCONN.EST_ESTABLE := 'CLOSED';
      vCONN.NODO1_ID    := 0;
      vCONN.NODO2_ID    := 0;
    
      insert into T$CCONECTIVIDAD_ES values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      --CARRENDAMIENTO
      vCARREN         := null;
      vCARREN.G3E_FNO := fno;
      vCARREN.G3E_CNO := 63;
      vCARREN.G3E_CID := 0;
      vCARREN.G3E_FID := fid;
    
      for carren in (select * from x$arrendamiento where phnode = c.code) loop
        vCARREN.G3E_CID := vCARREN.G3E_CID + 1;
        vCARREN.G3E_ID  := CARRENDAMIENTO_SEQ.NEXTVAL;
      
        vCARREN.USO      := carren.TYPE;
        vCARREN.ACTIVO   := 'SI';
        vCARREN.OPERADOR := carren.COMPANY;
        --carren.cuaadrilla
        --carren.code
        --caren.height
      
        insert into T$CARRENDAMIENTO values vCARREN;
      
      end loop;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_TORRE_TRANSMISION(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vCARREN      T$CARRENDAMIENTO%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vETOR_TRM_AT T$ETOR_TRM_AT%ROWTYPE;
    vETOR_TRM_PT T$ETOR_TRM_PT%ROWTYPE;
    vNORMA       T$NORMA%ROWTYPE;
  
    vASSEMBLY_EL X$CONECTIVIDAD.ASSEMBLY%TYPE;
  
    vAPOYOS  NUMBER(2);
    vVIENTOP VARCHAR2(2);
    vVIENTOS NUMBER(2);
    vTIERRA  VARCHAR2(2);
  
    vCount number(2);
    cid    number(2);
  
    fid number(10);
    fno number(5) := 17000;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$TORRE_TRANSMISION';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from X$TORRE_TRANSMISION) loop
    
      select count(1)
        into vCount
        from x$conectividad
       where phnode = c.code
         and fparent = pCircuito;
    
      if vCount = 0 then
        continue;
      end if;
    
      begin
        select g3e_fid
          into fid
          from etl_code2fid
         where code = c.code
           and g3e_fno = fno;
      
        insert into ETL_CODE2FID
        values
          (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      
        commit;
        continue;
      exception
        when others then
          null;
      end;
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ETOR_TRM_AT
      begin
        vETOR_TRM_AT             := null;
        vETOR_TRM_AT.PROPIETARIO := 'CHEC';
        vETOR_TRM_AT.GRUPO       := 'ESTRUCTURAS EYT';
      
        DesgloseAerMVPHNODE(c.assembly,
                            vETOR_TRM_AT.TIPO_TORRE,
                            vAPOYOS,
                            vETOR_TRM_AT.ALTURA_TORRE,
                            vVIENTOP,
                            vVIENTOS,
                            vTIERRA);
      
        begin
          select assembly
            into vASSEMBLY_EL
            from x$conectividad
           where phnode = c.code
             and rownum = 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$ETOR_TRM_AT',
               c.code,
               1,
               cErrorAssemblyNotFound,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      
        select count(1)
          into vCount
          from etl_code2fid
         where code = substr(vASSEMBLY_EL, 6, 1);
      
        if vCount = 0 then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ETOR_TRM_AT',
             c.code,
             1,
             cErrorAssemblyEletrico || vASSEMBLY_EL,
             pCircuito,
             sysdate);
          commit;
          continue;
        end if;
      
        select valor_carga
          into vETOR_TRM_AT.CLASE_TORRE
          from etl_picklists
         where valor = substr(vASSEMBLY_EL, 6, 1);
      
        vETOR_TRM_AT.G3E_FNO := fno;
        vETOR_TRM_AT.G3E_CNO := fno + 1;
        vETOR_TRM_AT.G3E_CID := 1;
        vETOR_TRM_AT.G3E_FID := fid;
        vETOR_TRM_AT.G3E_ID  := ETOR_TRM_AT_SEQ.NEXTVAL;
      
        insert into T$ETOR_TRM_AT values vETOR_TRM_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ETOR_TRM_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ETOR_TRM_PT
      vETOR_TRM_PT              := null;
      vETOR_TRM_PT.G3E_FNO      := fno;
      vETOR_TRM_PT.G3E_CNO      := fno + 10;
      vETOR_TRM_PT.G3E_CID      := 1;
      vETOR_TRM_PT.G3E_FID      := fid;
      vETOR_TRM_PT.G3E_ID       := ETOR_TRM_AT_SEQ.NEXTVAL;
      vETOR_TRM_PT.G3E_GEOMETRY := geom;
    
      insert into T$ETOR_TRM_PT values vETOR_TRM_PT;
    
      --CPROPIETARIO
    
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      cid := 1;
    
      --NORMA (Apoyos)
      if vAPOYOS != '0' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'APOYOS'
             and g3e_fno = fno
             and valor = vAPOYOS;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorApoyos || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --NORMA (VIENTO PRIMARIO)
      if substr(vVIENTOP, 2, 1) != '0' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'VIENTO PRIMARIO'
             and g3e_fno = fno
             and valor = vVIENTOP;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorVientoPrimario || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --NORMA (VIENTO SECUNDARIO)
      if vVIENTOS != '0' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'VIENTO SECUNDARIO'
             and g3e_fno = fno
             and valor = vVIENTOS;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorVientoSecundario || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --NORMA (TIERRA)
      if vTIERRA = 'T' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'TIERRA'
             and g3e_fno = fno
             and valor = vTIERRA;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorTierra || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --CARRENDAMIENTO
      vCARREN         := null;
      vCARREN.G3E_FNO := fno;
      vCARREN.G3E_CNO := 63;
      vCARREN.G3E_CID := 0;
      vCARREN.G3E_FID := fid;
    
      for carren in (select * from x$arrendamiento where phnode = c.code) loop
        vCARREN.G3E_CID := vCARREN.G3E_CID + 1;
        vCARREN.G3E_ID  := CARRENDAMIENTO_SEQ.NEXTVAL;
      
        vCARREN.USO      := carren.TYPE;
        vCARREN.ACTIVO   := 'SI';
        vCARREN.OPERADOR := carren.COMPANY;
        --carren.cuaadrilla
        --carren.code
        --caren.height
      
        insert into T$CARRENDAMIENTO values vCARREN;
      
      end loop;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_POSTE(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
    vCOUNT NUMBER;
  
    vCCOMUN    T$CCOMUN%ROWTYPE;
    vPROP      T$CPROPIETARIO%ROWTYPE;
    vEPOSTE_AT T$EPOSTE_AT%ROWTYPE;
    vEPOSTE_PT T$EPOSTE_PT%ROWTYPE;
    vCARREN    T$CARRENDAMIENTO%ROWTYPE;
    vNORMA     T$NORMA%ROWTYPE;
  
    vAPOYOS  NUMBER(2);
    vVIENTOP VARCHAR2(2);
    vVIENTOS NUMBER(2);
    vTIERRA  VARCHAR2(2);
  
    fid number(10);
    fno number(5) := 17100;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$POSTE';
  
    cid number(2);
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from x$poste) loop
    
      select count(1)
        into vCount
        from x$conectividad
       where phnode = c.code
         and fparent = pCircuito;
    
      if vCount = 0 then
        continue;
      end if;
    
      begin
        select g3e_fid
          into fid
          from etl_code2fid
         where code = c.code
           and g3e_fno = fno;
      
        insert into ETL_CODE2FID
        values
          (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      
        commit;
        continue;
      exception
        when others then
          null;
      end;
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EPOSTE_AT
      begin
        vEPOSTE_AT             := null;
        vEPOSTE_AT.CODIGO      := c.CODE;
        vEPOSTE_AT.GRUPO       := 'ESTRUCTURAS EYT';
        vEPOSTE_AT.PROPIETARIO := c.owner;
      
        if c.assembly = 'PORTICO' then
          vEPOSTE_AT.ALTURA := 12;
          --vEPOSTE_AT.CLASE    := 'PORTICO';
          vEPOSTE_AT.MATERIAL := 'METALICO';
          vAPOYOS             := 0;
          vVIENTOP            := 0;
          vVIENTOS            := 0;
          vTIERRA             := 0;
        else
        
          DesgloseAerMVPHNODE(c.assembly,
                              vEPOSTE_AT.CLASE,
                              vAPOYOS,
                              vEPOSTE_AT.ALTURA,
                              vVIENTOP,
                              vVIENTOS,
                              vTIERRA);
        
          select MATERIAL
            into vEPOSTE_AT.MATERIAL
            from EMAT_ALT_CLA_POS_CAT
           where clase = vEPOSTE_AT.CLASE
             and rownum = 1;
        end if;
      
        if c.owner is not null then
          vEPOSTE_AT.PROPIETARIO := c.owner;
        
          if c.towner = 'C' and c.owner != 'CHEC' then
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$EPOSTE_AT',
               c.code,
               2,
               cErrorPropietario,
               pCircuito,
               sysdate);
            commit;
            vEPOSTE_AT.PROPIETARIO := 'PARTICULAR';
          end if;
        end if;
      
        vEPOSTE_AT.G3E_FNO := fno;
        vEPOSTE_AT.G3E_CNO := fno + 1;
        vEPOSTE_AT.G3E_CID := 1;
        vEPOSTE_AT.G3E_FID := fid;
        vEPOSTE_AT.G3E_ID  := EPOSTE_AT_SEQ.NEXTVAL;
      
        insert into T$EPOSTE_AT values vEPOSTE_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EPOSTE_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EPOSTE
      vEPOSTE_PT.G3E_FNO      := fno;
      vEPOSTE_PT.G3E_CNO      := fno + 10;
      vEPOSTE_PT.G3E_CID      := 1;
      vEPOSTE_PT.G3E_FID      := fid;
      vEPOSTE_PT.G3E_ID       := EPOSTE_PT_SEQ.NEXTVAL;
      vEPOSTE_PT.G3E_GEOMETRY := geom;
    
      insert into T$EPOSTE_PT values vEPOSTE_PT;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      else
        rollback;
        insert into etl_transformacion_log
        values
          (vTABLA_EXTRACION,
           'T$EPOSTE_AT',
           c.code,
           1,
           cErrorSinPropietario,
           pCircuito,
           sysdate);
        commit;
        continue;
      
      end if;
    
      cid := 1;
    
      --NORMA (Apoyos)
      if vAPOYOS != '0' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'APOYOS'
             and g3e_fno = fno
             and valor = vAPOYOS;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorApoyos || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --NORMA (VIENTO PRIMARIO)
      if substr(vVIENTOP, 2, 1) != '0' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'VIENTO PRIMARIO'
             and g3e_fno = fno
             and valor = vVIENTOP;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorVientoPrimario || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --NORMA (VIENTO SECUNDARIO)
      if vVIENTOS != '0' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'VIENTO SECUNDARIO'
             and g3e_fno = fno
             and valor = vVIENTOS;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorVientoSecundario || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --NORMA (TIERRA)
      if vTIERRA = 'T' then
        vNORMA := null;
        begin
          select norma, cantidad, grupo
            into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
            from etl_normas
           where tipo = 'TIERRA'
             and g3e_fno = fno
             and valor = vTIERRA;
        
          vNORMA.G3E_FNO := fno;
          vNORMA.G3E_CNO := 70;
          vNORMA.G3E_CID := cid;
          vNORMA.G3E_FID := fid;
          vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
        
          insert into T$NORMA values vNORMA;
        
          cid := cid + 1;
        exception
          when others then
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$NORMA',
               c.code,
               1,
               cErrorTierra || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end;
      end if;
    
      --CARRENDAMIENTO
      vCARREN         := null;
      vCARREN.G3E_FNO := fno;
      vCARREN.G3E_CNO := 63;
      vCARREN.G3E_CID := 0;
      vCARREN.G3E_FID := fid;
    
      for carren in (select * from x$arrendamiento where phnode = c.code) loop
        vCARREN.G3E_CID := vCARREN.G3E_CID + 1;
        vCARREN.G3E_ID  := CARRENDAMIENTO_SEQ.NEXTVAL;
      
        vCARREN.USO      := carren.TYPE;
        vCARREN.ACTIVO   := 'SI';
        vCARREN.OPERADOR := carren.COMPANY;
        --carren.cuaadrilla
        --carren.code
        --caren.height
      
        insert into T$CARRENDAMIENTO values vCARREN;
      
      end loop;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_NODO_CONDUCTOR(pCircuito in VARCHAR2) is
    geom   sdo_geometry;
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vCARREN      T$CARRENDAMIENTO%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vENOD_CON_AT T$ENOD_CON_AT%ROWTYPE;
    vENOD_CON_PT T$ENOD_CON_PT%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vELNODE      X$CONECTIVIDAD%ROWTYPE;
  
    fid number(10);
    fno number(5) := 19100;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$NODO_CONDUCTOR';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select p.*
                from x$nodo_conductor p
               inner join x$conectividad c
                  on c.phnode = p.code
               where c.fparent = pCircuito) loop
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN               := null;
      vCCOMUN.G3E_FNO       := fno;
      vCCOMUN.G3E_CNO       := 60;
      vCCOMUN.G3E_CID       := 1;
      vCCOMUN.G3E_FID       := fid;
      vCCOMUN.G3E_id        := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES := c.DESCRIPTIO; --
      vCCOMUN.UBICACION     := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT  := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON  := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO      := c.PROJECT; --
      vCCOMUN.COOR_X        := c.XPOS; --
      vCCOMUN.COOR_Y        := c.YPOS; --
      vCCOMUN.DEPARTAMENTO  := SpatialDepartamento(geom,
                                                   c.code,
                                                   pCircuito,
                                                   vTABLA_EXTRACION); --
    
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ENOD_CON_AT
      begin
        vENOD_CON_AT := null;
      
        vENOD_CON_AT.CODIGO       := c.CODE;
        vENOD_CON_AT.TIPO_EMPALME := 'GOTERA';
      
        vENOD_CON_AT.G3E_FNO := fno;
        vENOD_CON_AT.G3E_CNO := fno + 1;
        vENOD_CON_AT.G3E_CID := 1;
        vENOD_CON_AT.G3E_FID := fid;
        vENOD_CON_AT.G3E_ID  := ENOD_CON_AT_SEQ.NEXTVAL;
      
        insert into T$ENOD_CON_AT values vENOD_CON_AT;
      
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ENOD_CON_AT',
             c.code,
             1,
             cErrorGeneral,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ENOD_CON_PT
      vENOD_CON_PT              := null;
      vENOD_CON_PT.G3E_FNO      := fno;
      vENOD_CON_PT.G3E_CNO      := fno + 10;
      vENOD_CON_PT.G3E_CID      := 1;
      vENOD_CON_PT.G3E_FID      := fid;
      vENOD_CON_PT.G3E_ID       := ENOD_CON_PT_SEQ.NEXTVAL;
      vENOD_CON_PT.G3E_GEOMETRY := geom;
    
      insert into T$ENOD_CON_PT values vENOD_CON_PT;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      --CONNECTIVIDAD
      begin
        select *
          into vELNODE
          from X$CONECTIVIDAD
         where phnode = c.code
           and fparent = pCircuito;
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CONNECTIVIDAD',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
      vCONN       := null;
      vCONN.FASES := DecodeFase(vELNODE.PHASES);
    
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      vCONN.CIRCUITO       := vELNODE.FPARENT;
      vCONN.TENSION        := to_char(vELNODE.KVNOM);
      vCONN.NODO1_ID       := null;
      vCONN.NODO_TRANSFORM := vELNODE.TPARENT;
    
      select nodo
        into vCONN.NODO1_ID
        from (select connt.nodo1_id nodo
                from x$conectividad conn
               inner join x$conductor_primario cp
                  on cp.elnode1 = conn.code
               inner join etl_code2fid etl
                  on etl.code = cp.code
               inner join t$cconectividad_e connt
                  on connt.g3e_fid = etl.g3e_fid
               where connt.g3e_fno = 19000
                 and conn.phnode = c.code
              union
              select connt.nodo2_id nodo
                from x$conectividad conn
               inner join x$conductor_primario cp
                  on cp.elnode2 = conn.code
               inner join etl_code2fid etl
                  on etl.code = cp.code
               inner join t$cconectividad_e connt
                  on connt.g3e_fid = etl.g3e_fid
               where connt.g3e_fno = 19000
                 and conn.phnode = c.code)
       where rownum = 1;
    
      if vCONN.NODO1_ID is null then
        select nodo
          into vCONN.NODO1_ID
          from (select connt.nodo1_id nodo
                  from x$conectividad conn
                 inner join x$conductor_transmision cp
                    on cp.elnode1 = conn.code
                 inner join etl_code2fid etl
                    on etl.code = cp.code
                 inner join t$cconectividad_e connt
                    on connt.g3e_fid = etl.g3e_fid
                 where connt.g3e_fno = 18900
                   and conn.phnode = c.code
                union
                select connt.nodo2_id nodo
                  from x$conectividad conn
                 inner join x$conductor_transmision cp
                    on cp.elnode2 = conn.code
                 inner join etl_code2fid etl
                    on etl.code = cp.code
                 inner join t$cconectividad_e connt
                    on connt.g3e_fid = etl.g3e_fid
                 where connt.g3e_fno = 18900
                   and conn.phnode = c.code)
         where rownum = 1;
      end if;
    
      if vCONN.NODO1_ID is null then
        vCONN.NODO1_ID := 0;
      end if;
    
      vCONN.NODO2_ID := 0;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CARRENDAMIENTO
      vCARREN         := null;
      vCARREN.G3E_FNO := fno;
      vCARREN.G3E_CNO := 63;
      vCARREN.G3E_CID := 0;
      vCARREN.G3E_FID := fid;
    
      for carren in (select * from x$arrendamiento where phnode = c.code) loop
        vCARREN.G3E_CID := vCARREN.G3E_CID + 1;
        vCARREN.G3E_ID  := CARRENDAMIENTO_SEQ.NEXTVAL;
      
        vCARREN.USO      := carren.TYPE;
        vCARREN.ACTIVO   := 'SI';
        vCARREN.OPERADOR := carren.COMPANY;
        --carren.cuaadrilla
        --carren.code
        --caren.height
      
        insert into T$CARRENDAMIENTO values vCARREN;
      
      end loop;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_AISLADERO(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN T$CCOMUN%ROWTYPE;
  
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vEAISLADE_PT T$EAISLADE_PT%ROWTYPE;
    vEAISLADE_AT T$EAISLADE_AT%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 19300;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$AILADERO';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from x$aisladero where fparent = pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EAISLADE_AT
      begin
        vEAISLADE_AT            := null;
        vEAISLADE_AT.CODIGO     := c.CODE;
        vEAISLADE_AT.FABRICANTE := c.MAKER;
      
        case c.ASSEMBLY
          when '1CC' then
            vEAISLADE_AT.TIPO_AISLADERO := 'NORMAL';
          when '2CC' then
            vEAISLADE_AT.TIPO_AISLADERO := 'CAJA VELA';
          when '3CC' then
            vEAISLADE_AT.TIPO_AISLADERO := 'CAJA VELA';
          when 'CR' then
            vEAISLADE_AT.TIPO_AISLADERO := 'CON CAMERA ROMPEARCO';
          when 'ER' then
            vEAISLADE_AT.TIPO_AISLADERO := 'ELETRONICO ROMPEARCO';
          when 'EL' then
            vEAISLADE_AT.TIPO_AISLADERO := 'ELETRONICO';
          when 'CFR' then
            vEAISLADE_AT.TIPO_AISLADERO := 'REPETICION';
          else
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$EAISLADE_AT',
               c.code,
               1,
               cErrorAssemblyFisico || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end case;
      
        if c.TIPOFUSES = 'PUENTE' then
          vEAISLADE_AT.TIPO_FUSIBLE := 'PUENTE';
          vEAISLADE_AT.CAPACIDAD    := 0;
        elsif instr(c.TIPOFUSES, 'D') > 0 then
          vEAISLADE_AT.TIPO_FUSIBLE := 'D';
          vEAISLADE_AT.CAPACIDAD    := to_number(replace(replace(c.TIPOFUSES,
                                                                 'D',
                                                                 null),
                                                         'C',
                                                         ','));
        elsif instr(c.TIPOFUSES, 'H') > 0 then
          vEAISLADE_AT.TIPO_FUSIBLE := 'H';
          vEAISLADE_AT.CAPACIDAD    := to_number(replace(c.TIPOFUSES,
                                                         'H',
                                                         null));
        elsif instr(c.TIPOFUSES, 'K') > 0 then
          vEAISLADE_AT.TIPO_FUSIBLE := 'K';
          vEAISLADE_AT.CAPACIDAD    := to_number(replace(c.TIPOFUSES,
                                                         'K',
                                                         null));
        elsif instr(c.TIPOFUSES, 'T') > 0 then
          vEAISLADE_AT.TIPO_FUSIBLE := 'T';
          vEAISLADE_AT.CAPACIDAD    := to_number(replace(c.TIPOFUSES,
                                                         'T',
                                                         null));
        end if;
      
        if c.owner is not null then
        
          vEAISLADE_AT.PROPIETARIO := c.owner;
        
          if c.towner = 'C' and c.owner != 'CHEC' then
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$EAISLADE_AT',
               c.code,
               2,
               cErrorPropietario,
               pCircuito,
               sysdate);
            commit;
            vEAISLADE_AT.PROPIETARIO := 'PARTICULAR';
          end if;
        else
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EAISLADE_AT',
             c.code,
             1,
             cErrorSinPropietario,
             pCircuito,
             sysdate);
          commit;
          continue;
        
        end if;
      
        vEAISLADE_AT.G3E_FNO := fno;
        vEAISLADE_AT.G3E_CNO := fno + 1;
        vEAISLADE_AT.G3E_CID := 1;
        vEAISLADE_AT.G3E_FID := fid;
        vEAISLADE_AT.G3E_ID  := EAISLADE_AT_SEQ.NEXTVAL;
      
        insert into T$EAISLADE_AT values vEAISLADE_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EAISLADE_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EAISLADE_PT
      vEAISLADE_PT              := null;
      vEAISLADE_PT.G3E_FNO      := fno;
      vEAISLADE_PT.G3E_CNO      := fno + 10;
      vEAISLADE_PT.G3E_CID      := 1;
      vEAISLADE_PT.G3E_FID      := fid;
      vEAISLADE_PT.G3E_ID       := EAISLADE_PT_SEQ.NEXTVAL;
      vEAISLADE_PT.G3E_GEOMETRY := geom;
    
      insert into T$EAISLADE_PT values vEAISLADE_PT;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
      vCONN.TENSION     := to_char(c.KV);
      vCONN.NODO1_ID    := PTC_BuscarNodoSwitches(c.code,
                                                  c.linesectio,
                                                  c.xpos,
                                                  c.ypos,
                                                  fidConduct,
                                                  nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
    
      --if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      --if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      --end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      /*
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito, sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
      */
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_CUCHILLA(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vECUCHILL_AT T$ECUCHILL_AT%ROWTYPE;
    vECUCHILL_PT T$ECUCHILL_PT%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 19400;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$CUCHILLA';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select *
                from X$CUCHILLA
               where fparent = pCircuito
                 and code != pCircuito) loop
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN               := null;
      vCCOMUN.G3E_FNO       := fno;
      vCCOMUN.G3E_CNO       := 60;
      vCCOMUN.G3E_CID       := 1;
      vCCOMUN.G3E_FID       := fid;
      vCCOMUN.G3E_id        := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES := c.DESCRIPTIO; --
      vCCOMUN.UBICACION     := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT  := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON  := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO      := c.PROJECT; --
      vCCOMUN.COOR_X        := c.XPOS; --
      vCCOMUN.COOR_Y        := c.YPOS; --
      vCCOMUN.DEPARTAMENTO  := SpatialDepartamento(geom,
                                                   c.code,
                                                   pCircuito,
                                                   vTABLA_EXTRACION); --
    
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --vECUCHILL_AT 
    
      vECUCHILL_AT            := null;
      vECUCHILL_AT.G3E_FNO    := fno;
      vECUCHILL_AT.G3E_CNO    := fno + 1;
      vECUCHILL_AT.G3E_CID    := 1;
      vECUCHILL_AT.G3E_FID    := fid;
      vECUCHILL_AT.G3E_ID     := ECUCHILL_AT_SEQ.NEXTVAL;
      vECUCHILL_AT.CODIGO     := c.CODE;
      vECUCHILL_AT.FABRICANTE := c.MAKER;
      vECUCHILL_AT.USO        := 'NORMAL';
    
      insert into T$ECUCHILL_AT values vECUCHILL_AT;
    
      --ECUCHILL_PT
      vECUCHILL_PT              := null;
      vECUCHILL_PT.G3E_FNO      := fno;
      vECUCHILL_PT.G3E_CNO      := fno + 10;
      vECUCHILL_PT.G3E_CID      := 1;
      vECUCHILL_PT.G3E_FID      := fid;
      vECUCHILL_PT.G3E_ID       := ECUCHILL_PT_SEQ.NEXTVAL;
      vECUCHILL_PT.G3E_GEOMETRY := geom;
    
      insert into T$ECUCHILL_PT values vECUCHILL_PT;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
      vCONN.ESTADO            := 'OPERACION';
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
      vCONN.TENSION     := to_char(c.KV);
      vCONN.NODO1_ID    := PTC_BuscarNodoSwitches(c.code,
                                                  c.linesectio,
                                                  c.xpos,
                                                  c.ypos,
                                                  fidConduct,
                                                  nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
      -- if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      --  if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_INTERRUPTOR(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN          T$CCOMUN%ROWTYPE;
    vPROP            T$CPROPIETARIO%ROWTYPE;
    vCONN            T$CCONECTIVIDAD_E%ROWTYPE;
    vEINTERRU_AT     T$EINTERRU_AT%ROWTYPE;
    vEINTERRU_PT     T$EINTERRU_PT%ROWTYPE;
    vEINTERRU_CTO_TX T$EINTERRU_CTO_TX%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 18800;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$INTERRUPTOR';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select *
                from X$INTERRUPTOR
               where fparent = pCircuito
                 and code != pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --vEINTERRU_AT  
    
      vEINTERRU_AT                  := null;
      vEINTERRU_AT.G3E_FNO          := fno;
      vEINTERRU_AT.G3E_CNO          := fno + 1;
      vEINTERRU_AT.G3E_CID          := 1;
      vEINTERRU_AT.G3E_FID          := fid;
      vEINTERRU_AT.G3E_ID           := EINTERRU_AT_SEQ.NEXTVAL;
      vEINTERRU_AT.CODIGO           := c.CODE;
      vEINTERRU_AT.FABRICANTE       := c.MAKER;
      vEINTERRU_AT.TIPO_INTERRUPTOR := c.ASSEMBLY;
    
      insert into T$EINTERRU_AT values vEINTERRU_AT;
    
      --ECUCHILL_PT
      vEINTERRU_PT              := null;
      vEINTERRU_PT.G3E_FNO      := fno;
      vEINTERRU_PT.G3E_CNO      := fno + 10;
      vEINTERRU_PT.G3E_CID      := 1;
      vEINTERRU_PT.G3E_FID      := fid;
      vEINTERRU_PT.G3E_ID       := EINTERRU_PT_SEQ.NEXTVAL;
      vEINTERRU_PT.G3E_GEOMETRY := geom;
    
      insert into T$EINTERRU_PT values vEINTERRU_PT;
    
      --EINTERRU_CTO_TX
      vEINTERRU_CTO_TX              := null;
      vEINTERRU_CTO_TX.G3E_FNO      := fno;
      vEINTERRU_CTO_TX.G3E_CNO      := fno + 40;
      vEINTERRU_CTO_TX.G3E_CID      := 1;
      vEINTERRU_CTO_TX.G3E_FID      := fid;
      vEINTERRU_CTO_TX.G3E_ID       := EINTERRU_CTO_TX_SEQ.NEXTVAL;
      vEINTERRU_CTO_TX.G3E_GEOMETRY := ConvertPoint2SigmaTexto(c.xpos,
                                                               c.ypos);
    
      insert into T$EINTERRU_CTO_TX values vEINTERRU_CTO_TX;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
      vCONN.ESTADO            := 'OPERACION';
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
      vCONN.TENSION     := to_char(c.KV);
      vCONN.NODO1_ID    := PTC_BuscarNodoSwitches(c.code,
                                                  c.linesectio,
                                                  c.xpos,
                                                  c.ypos,
                                                  fidConduct,
                                                  nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
      -- if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      --  if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_RECONECTADOR(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vERECONEC_AT T$ERECONEC_AT%ROWTYPE;
    vERECONEC_PT T$ERECONEC_PT%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 19800;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$RECONECTADOR';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select *
                from x$reconectador
               where fparent = pCircuito
                 and code != pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ERECONEC_AT
      begin
        vERECONEC_AT         := null;
        vERECONEC_AT.G3E_FNO := fno;
        vERECONEC_AT.G3E_CNO := fno + 1;
        vERECONEC_AT.G3E_CID := 1;
        vERECONEC_AT.G3E_FID := fid;
        vERECONEC_AT.G3E_ID  := ERECONEC_AT_SEQ.NEXTVAL;
      
        vERECONEC_AT.CODIGO     := c.CODE;
        vERECONEC_AT.FABRICANTE := c.MAKER;
      
        case c.ASSEMBLY
          when '3RL' then
            vERECONEC_AT.USO := 'NORMAL';
          when '3RG' then
            vERECONEC_AT.USO := 'RECONECTADOR CON SALIDA';
          else
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$ERECONEC_AT',
               c.code,
               1,
               cErrorAssemblyFisico || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end case;
      
        insert into T$ERECONEC_AT values vERECONEC_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ERECONEC_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ERECONEC_PT 
      vERECONEC_PT              := null;
      vERECONEC_PT.G3E_FNO      := fno;
      vERECONEC_PT.G3E_CNO      := fno + 10;
      vERECONEC_PT.G3E_CID      := 1;
      vERECONEC_PT.G3E_FID      := fid;
      vERECONEC_PT.G3E_ID       := ERECONEC_PT_SEQ.NEXTVAL;
      vERECONEC_PT.G3E_GEOMETRY := geom;
    
      insert into T$ERECONEC_PT values vERECONEC_PT;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
    
      vCONN.TENSION  := to_char(c.KV);
      vCONN.NODO1_ID := PTC_BuscarNodoSwitches(c.code,
                                               c.linesectio,
                                               c.xpos,
                                               c.ypos,
                                               fidConduct,
                                               nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
      -- if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      -- if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      --end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_SECCIONALIZADOR(pCircuito in VARCHAR2) is
    geom   sdo_geometry;
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vESECCION_AT T$ESECCION_AT%ROWTYPE;
    vESECCION_PT T$ESECCION_PT%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 19800;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$SECCIONALIZADOR';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from x$seccionalizador where fparent = pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ESECCION_AT
      begin
        vESECCION_AT         := null;
        vESECCION_AT.G3E_FNO := fno;
        vESECCION_AT.G3E_CNO := fno + 1;
        vESECCION_AT.G3E_CID := 1;
        vESECCION_AT.G3E_FID := fid;
        vESECCION_AT.G3E_ID  := ESECCION_AT_SEQ.NEXTVAL;
      
        vESECCION_AT.CODIGO     := c.CODE;
        vESECCION_AT.FABRICANTE := c.MAKER;
      
        case c.ASSEMBLY
          when '3SC' then
            vESECCION_AT.TIPO := 'Seccionador operación con carga y cuchillas';
          when '3OC' then
            vESECCION_AT.TIPO := 'Seccionador trifasico de operación con carga';
          else
            rollback;
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$ESECCION_AT',
               c.code,
               1,
               cErrorAssemblyFisico || c.assembly,
               pCircuito,
               sysdate);
            commit;
            continue;
        end case;
      
        insert into T$ESECCION_AT values vESECCION_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ESECCION_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ESECCION_PT
      vESECCION_PT              := null;
      vESECCION_PT.G3E_FNO      := fno;
      vESECCION_PT.G3E_CNO      := fno + 10;
      vESECCION_PT.G3E_CID      := 1;
      vESECCION_PT.G3E_FID      := fid;
      vESECCION_PT.G3E_ID       := ERECONEC_PT_SEQ.NEXTVAL;
      vESECCION_PT.G3E_GEOMETRY := geom;
    
      insert into T$ESECCION_PT values vESECCION_PT;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
      vCONN.TENSION     := to_char(c.KV);
      vCONN.NODO1_ID    := PTC_BuscarNodoSwitches(c.code,
                                                  c.linesectio,
                                                  c.xpos,
                                                  c.ypos,
                                                  fidConduct,
                                                  nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
      -- if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      --if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      --end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_REFERENCIA(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vEREFEREN_PT T$EREFEREN_PT%ROWTYPE;
    vEREFEREN_AT T$EREFEREN_AT%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 19800;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$REFERENCIA';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from x$REFERENCIA where fparent = pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EREFEREN_AT
      begin
      
        vEREFEREN_AT         := null;
        vEREFEREN_AT.G3E_FNO := fno;
        vEREFEREN_AT.G3E_CNO := fno + 1;
        vEREFEREN_AT.G3E_CID := 1;
        vEREFEREN_AT.G3E_FID := fid;
        vEREFEREN_AT.G3E_ID  := EREFEREN_AT_SEQ.NEXTVAL;
        vEREFEREN_AT.CODIGO  := c.CODE;
      
        insert into T$EREFEREN_AT values vEREFEREN_AT;
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EREFEREN_AT',
             c.code,
             1,
             cErrorGeneral,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EREFEREN_PT
      vEREFEREN_PT              := null;
      vEREFEREN_PT.G3E_FNO      := fno;
      vEREFEREN_PT.G3E_CNO      := fno + 10;
      vEREFEREN_PT.G3E_CID      := 1;
      vEREFEREN_PT.G3E_FID      := fid;
      vEREFEREN_PT.G3E_ID       := EREFEREN_PT_SEQ.NEXTVAL;
      vEREFEREN_PT.G3E_GEOMETRY := geom;
    
      insert into T$EREFEREN_PT values vEREFEREN_PT;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
      vCONN.TENSION     := to_char(c.KV);
      vCONN.NODO1_ID    := PTC_BuscarNodoSwitches(c.code,
                                                  c.linesectio,
                                                  c.xpos,
                                                  c.ypos,
                                                  fidConduct,
                                                  nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
    
      --if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      --if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      --end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_SUICHE(pCircuito in VARCHAR2) is
    geom   sdo_geometry;
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN     T$CCOMUN%ROWTYPE;
    vPROP       T$CPROPIETARIO%ROWTYPE;
    vCONN       T$CCONECTIVIDAD_E%ROWTYPE;
    vESUICHE_PT T$ESUICHE_PT%ROWTYPE;
    vESUICHE_AT T$ESUICHE_AT%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 19700;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$SUICHE';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from x$suiche where fparent = pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ESUICHE_AT
      begin
        vESUICHE_AT         := null;
        vESUICHE_AT.G3E_FNO := fno;
        vESUICHE_AT.G3E_CNO := fno + 1;
        vESUICHE_AT.G3E_CID := 1;
        vESUICHE_AT.G3E_FID := fid;
        vESUICHE_AT.G3E_ID  := ESUICHE_AT_SEQ.NEXTVAL;
      
        if c.ASSEMBLY = '3ISA' then
          vESUICHE_AT.AISLAMIENTO := 'ACEITE';
        elsif c.ASSEMBLY = '3IS6' then
          vESUICHE_AT.AISLAMIENTO := 'SF6';
        else
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ESUICHE_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
        end if;
      
        vESUICHE_AT.CODIGO     := c.CODE;
        vESUICHE_AT.OPERACION  := 'MANUAL';
        vESUICHE_AT.FABRICANTE := c.MAKER;
        vESUICHE_AT.USO        := 'SUICHE';
      
        insert into T$ESUICHE_AT values vESUICHE_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ESUICHE_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ESUICHE_PT
      vESUICHE_PT              := null;
      vESUICHE_PT.G3E_FNO      := fno;
      vESUICHE_PT.G3E_CNO      := fno + 10;
      vESUICHE_PT.G3E_CID      := 1;
      vESUICHE_PT.G3E_FID      := fid;
      vESUICHE_PT.G3E_ID       := ESUICHE_PT_SEQ.NEXTVAL;
      vESUICHE_PT.G3E_GEOMETRY := geom;
    
      insert into T$ESUICHE_PT values vESUICHE_PT;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO    := c.FPARENT;
      vCONN.SUBESTACION := substr(c.fparent, 1, 3);
      vCONN.TENSION     := to_char(c.KV);
      vCONN.NODO1_ID    := PTC_BuscarNodoSwitches(c.code,
                                                  c.linesectio,
                                                  c.xpos,
                                                  c.ypos,
                                                  fidConduct,
                                                  nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
    
      --if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      --end if;
    
      -- if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_CONDUCTOR_PRIMARIO(pCircuito in VARCHAR2) is
    geom   sdo_geometry;
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN T$CCOMUN%ROWTYPE;
    --vPROP   T$CPROPIETARIO%ROWTYPE;
    vCONN T$CCONECTIVIDAD_E%ROWTYPE;
  
    vECON_PRI_AT  T$ECON_PRI_AT%ROWTYPE;
    vECON_PRI_LN  T$ECON_PRI_LN%ROWTYPE;
    vCCONTENEDOR  T$CCONTENEDOR%ROWTYPE;
    vCPERTENENCIA T$CPERTENENCIA%ROWTYPE;
  
    fid number(10);
    fno number(5) := 19000;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$CONDUCTOR_PRIMARIO';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
    cursor ElementosSostenidos(cCodigo VARCHAR2) is
      select e.g3e_fid, e.g3e_fno
        from x$conductor_primario cp
       inner join x$conectividad con
          on con.code = cp.elnode1 --elnode1
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 17400)
      union
      select e.g3e_fid, e.g3e_fno
        from x$conductor_primario cp
       inner join x$conectividad con
          on con.code = cp.elnode2 --elnode2
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 17400);
  
  begin
  
    for c in (select * from x$conductor_primario where fparent = pCircuito) loop
    
      geom := ConvertLine2Sigma(c.xpos1,
                                c.ypos1,
                                BuscarZetaNodoEletrico(c.elnode1),
                                c.xpos2,
                                c.ypos2,
                                BuscarZetaNodoEletrico(c.elnode2));
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS1; --
      vCCOMUN.COOR_Y         := c.YPOS1; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECON_PRI_AT
      begin
        vECON_PRI_AT                := null;
        vECON_PRI_AT.CODE_CONDUCTOR := c.CONDUCTOR;
        vECON_PRI_AT.CODIGO         := c.CODE;
        vECON_PRI_AT.CODE_NEUTRAL   := c.NEUTRAL;
        vECON_PRI_AT.GUARDA         := c.GUARDA;
        vECON_PRI_AT.USO            := 'NORMAL';
      
        select calibre, material, aislamento
          into vECON_PRI_AT.CALIBRE,
               vECON_PRI_AT.MATERIAL,
               vECON_PRI_AT.AISLAMIENTO
          from conducto
         where code = c.CONDUCTOR
           and rownum = 1;
      
        vECON_PRI_AT.PROPIETARIO := c.owner;
        if vECON_PRI_AT.PROPIETARIO is not null then
          if c.towner = 'C' and c.owner != 'CHEC' then
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$ECON_PRI_AT',
               c.code,
               2,
               cErrorPropietario,
               pCircuito,
               sysdate);
            commit;
            vECON_PRI_AT.PROPIETARIO := 'PARTICULAR';
          end if;
        end if;
      
        vECON_PRI_AT.G3E_FNO := fno;
        vECON_PRI_AT.G3E_CNO := fno + 1;
        vECON_PRI_AT.G3E_CID := 1;
        vECON_PRI_AT.G3E_FID := fid;
        vECON_PRI_AT.G3E_ID  := ECON_PRI_AT_SEQ.NEXTVAL;
      
        insert into T$ECON_PRI_AT values vECON_PRI_AT;
      
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECON_PRI_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.code,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECON_PRI_LN
      vECON_PRI_LN              := null;
      vECON_PRI_LN.G3E_FNO      := fno;
      vECON_PRI_LN.G3E_CNO      := fno + 20;
      vECON_PRI_LN.G3E_CID      := 1;
      vECON_PRI_LN.G3E_FID      := fid;
      vECON_PRI_LN.G3E_ID       := ECON_PRI_LN_SEQ.NEXTVAL;
      vECON_PRI_LN.G3E_GEOMETRY := geom;
    
      insert into T$ECON_PRI_LN values vECON_PRI_LN;
    
      --CCONECTIVIDAD_E
      vCONN          := null;
      vCONN.NODO1_ID := PTC_BuscarNodoConductor(c.code, c.elnode1);
      vCONN.NODO2_ID := PTC_BuscarNodoConductor(c.code, c.elnode2);
    
      if vCONN.NODO1_ID = 0 then
        vCONN.NODO1_ID := g3e_node_seq.nextval;
        update CHEC_NODOS_ELEC
           set nivel = 'MVELNODE', nodo_mde = vCONN.NODO1_ID
         where code = c.elnode1;
      end if;
    
      if vCONN.NODO2_ID = 0 then
        vCONN.NODO2_ID := g3e_node_seq.nextval;
        update CHEC_NODOS_ELEC
           set nivel = 'MVELNODE', nodo_mde = vCONN.NODO2_ID
         where code = c.elnode2;
      end if;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      vCONN.LOCALIZACION  := c.CLASS;
      vCONN.LONGITUD      := to_number(c.LENGTH);
      vCONN.FASES         := DecodeFase(c.PHASES);
      vCONN.CIRCUITO      := c.FPARENT;
      vCONN.TENSION       := to_char(c.KVNOM * 1000);
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CCONTENEDOR
      vCCONTENEDOR := null;
    
      vCCONTENEDOR.G3E_FNO := fno;
      vCCONTENEDOR.G3E_CNO := 65;
      vCCONTENEDOR.G3E_FID := fid;
    
      for postes in ElementosSostenidos(c.code) loop
      
        select max(G3E_CID)
          into vCCONTENEDOR.G3E_CID
          from T$CCONTENEDOR
         where G3E_OWNERFID = postes.g3e_fid
           and G3E_OWNERFNO = postes.g3e_fno;
      
        if vCCONTENEDOR.G3E_CID is null then
          vCCONTENEDOR.G3E_CID := 0;
        end if;
      
        vCCONTENEDOR.G3E_CID      := vCCONTENEDOR.G3E_CID + 1;
        vCCONTENEDOR.G3E_ID       := CCONTENEDOR_SEQ.NEXTVAL;
        vCCONTENEDOR.G3E_OWNERFID := postes.g3e_fid;
        vCCONTENEDOR.G3E_OWNERFNO := postes.g3e_fno;
      
        insert into T$CCONTENEDOR values vCCONTENEDOR;
      
      end loop;
    
      --CPERTENENCIA
      vCPERTENENCIA := null;
    
      vCPERTENENCIA.G3E_CID := 1;
      vCPERTENENCIA.G3E_ID  := CPERTENENCIA_SEQ.NEXTVAL;
      vCPERTENENCIA.G3E_FNO := fno;
      vCPERTENENCIA.G3E_CNO := 64;
      vCPERTENENCIA.G3E_FID := fid;
    
      insert into T$CPERTENENCIA values vCPERTENENCIA;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_CONDUCTOR_SECUNDARIO(pCircuito in VARCHAR2) is
    geom   sdo_geometry;
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN T$CCOMUN%ROWTYPE;
    --vPROP   T$CPROPIETARIO%ROWTYPE;
    vCONN T$CCONECTIVIDAD_E%ROWTYPE;
  
    vECON_SES_AT  T$ECON_SES_AT%ROWTYPE;
    vECON_SES_LN  T$ECON_SES_LN%ROWTYPE;
    vCCONTENEDOR  T$CCONTENEDOR%ROWTYPE;
    vCPERTENENCIA T$CPERTENENCIA%ROWTYPE;
  
    fid number(10);
    fno number(5) := 21200;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$CONDUCTOR_SECUNDARIO';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
    cursor ElementosSostenidos(cCodigo VARCHAR2) is
      select e.g3e_fid, e.g3e_fno
        from x$conductor_primario cp
       inner join x$conectividad con
          on con.code = cp.elnode1 --elnode1
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 18100)
      union
      select e.g3e_fid, e.g3e_fno
        from x$conductor_primario cp
       inner join x$conectividad con
          on con.code = cp.elnode2 --elnode2
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 18100)
      union
      select e.g3e_fid, e.g3e_fno
        from x$conductor_secundario cp
       inner join x$conectividad con
          on con.code = cp.elnode1 --elnode1
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 18100)
      union
      select e.g3e_fid, e.g3e_fno
        from x$conductor_secundario cp
       inner join x$conectividad con
          on con.code = cp.elnode2 --elnode2
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 18100);
  
  begin
  
    for c in (select * from x$conductor_secundario where fparent = pCircuito) loop
      geom := ConvertLine2Sigma(c.xpos1,
                                c.ypos1,
                                BuscarZetaNodoEletrico(c.elnode1),
                                c.xpos2,
                                c.ypos2,
                                BuscarZetaNodoEletrico(c.elnode2));
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS1; --
      vCCOMUN.COOR_Y         := c.YPOS1; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECON_SES_AT
      begin
        vECON_SES_AT                := null;
        vECON_SES_AT.CODE_CONDUCTOR := c.CONDUCTOR;
        vECON_SES_AT.CODIGO         := c.CODE;
        vECON_SES_AT.CODE_NEUTRAL   := c.NEUTRAL;
        vECON_SES_AT.TIPO           := DecodeTipoConductorSec(c.USO);
        vECON_SES_AT.CLASE_RED_SEC  := DecodeTipoConductorSec(c.USO); --MIRAR Este cmapo
      
        select calibre, material, aislamento
          into vECON_SES_AT.CALIBRE,
               vECON_SES_AT.MATERIAL,
               vECON_SES_AT.AISLAMIENTO
          from conducto
         where code = c.CONDUCTOR
           and rownum = 1;
      
        if vECON_SES_AT.MATERIAL is null then
          rollback;
        
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECON_SES_AT',
             c.code,
             1,
             cErrorMaterial,
             pCircuito,
             sysdate);
          commit;
          continue;
        
        end if;
      
        vECON_SES_AT.PROPIETARIO := c.owner;
        if vECON_SES_AT.PROPIETARIO is not null then
          if c.towner = 'C' and c.owner != 'CHEC' then
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$ECON_SES_AT',
               c.code,
               2,
               cErrorPropietario,
               pCircuito,
               sysdate);
            commit;
            vECON_SES_AT.PROPIETARIO := 'PARTICULAR';
          end if;
        end if;
      
        vECON_SES_AT.G3E_FNO := fno;
        vECON_SES_AT.G3E_CNO := fno + 1;
        vECON_SES_AT.G3E_CID := 1;
        vECON_SES_AT.G3E_FID := fid;
        vECON_SES_AT.G3E_ID  := ECON_SES_AT_SEQ.NEXTVAL;
      
        insert into T$ECON_SES_AT values vECON_SES_AT;
      
      exception
        when others then
          rollback;
          vERROR := SUBSTR(SQLERRM, 1, 64);
        
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECON_SES_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECON_SES_LN
      vECON_SES_LN              := null;
      vECON_SES_LN.G3E_FNO      := fno;
      vECON_SES_LN.G3E_CNO      := fno + 20;
      vECON_SES_LN.G3E_CID      := 1;
      vECON_SES_LN.G3E_FID      := fid;
      vECON_SES_LN.G3E_ID       := ECON_SES_LN_SEQ.NEXTVAL;
      vECON_SES_LN.G3E_GEOMETRY := geom;
    
      insert into T$ECON_SES_LN values vECON_SES_LN;
    
      --CCONECTIVIDAD_E
      vCONN          := null;
      vCONN.NODO1_ID := PTC_BuscarNodoConductorSec(c.code, c.elnode1);
      vCONN.NODO2_ID := PTC_BuscarNodoConductorSec(c.code, c.elnode2);
    
      if vCONN.NODO1_ID = 0 then
        vCONN.NODO1_ID := g3e_node_seq.nextval;
        update CHEC_NODOS_ELEC
           set nivel = 'LVELNODE', nodo_mde = vCONN.NODO1_ID
         where code = c.elnode1;
      end if;
    
      if vCONN.NODO2_ID = 0 then
        vCONN.NODO2_ID := g3e_node_seq.nextval;
        update CHEC_NODOS_ELEC
           set nivel = 'LVELNODE', nodo_mde = vCONN.NODO2_ID
         where code = c.elnode2;
      end if;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      vCONN.LOCALIZACION       := c.CLASS;
      vCONN.LONGITUD           := to_number(c.LENGTH);
      vCONN.FASES              := DecodeFase(c.PHASES);
      vCONN.CIRCUITO           := c.FPARENT;
      vCONN.TENSION_SECUNDARIA := to_char(c.KVNOM * 1000);
      vCONN.EST_ESTABLE        := 'CLOSED';
      vCONN.EST_OPERATIVO      := 'CLOSED';
      vCONN.NODO_TRANSFORM     := c.TPARENT;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CCONTENEDOR
      vCCONTENEDOR := null;
      select max(G3E_CID)
        into vCCONTENEDOR.G3E_CID
        from T$CCONTENEDOR
       where G3E_FID = fid;
    
      if vCCONTENEDOR.G3E_CID is null then
        vCCONTENEDOR.G3E_CID := 0;
      end if;
    
      vCCONTENEDOR.G3E_FNO := fno;
      vCCONTENEDOR.G3E_CNO := 65;
      vCCONTENEDOR.G3E_FID := fid;
    
      for postes in ElementosSostenidos(c.code) loop
      
        vCCONTENEDOR.G3E_CID      := vCCONTENEDOR.G3E_CID + 1;
        vCCONTENEDOR.G3E_ID       := CCONTENEDOR_SEQ.NEXTVAL;
        vCCONTENEDOR.G3E_OWNERFID := postes.g3e_fid;
        vCCONTENEDOR.G3E_OWNERFNO := postes.g3e_fno;
      
        insert into T$CCONTENEDOR values vCCONTENEDOR;
      
      end loop;
    
      --CPERTENENCIA
      vCPERTENENCIA := null;
    
      vCPERTENENCIA.G3E_CID := 1;
      vCPERTENENCIA.G3E_ID  := CPERTENENCIA_SEQ.NEXTVAL;
      vCPERTENENCIA.G3E_FNO := fno;
      vCPERTENENCIA.G3E_CNO := 64;
      vCPERTENENCIA.G3E_FID := fid;
    
      insert into T$CPERTENENCIA values vCPERTENENCIA;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_CONDUCTOR_TRANSMISION(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN T$CCOMUN%ROWTYPE;
  
    vCONN         T$CCONECTIVIDAD_E%ROWTYPE;
    vECON_TRA_AT  T$ECON_TRA_AT%ROWTYPE;
    vECON_TRA_LN  T$ECON_TRA_LN%ROWTYPE;
    vCCONTENEDOR  T$CCONTENEDOR%ROWTYPE;
    vCPERTENENCIA T$CPERTENENCIA%ROWTYPE;
  
    fid number(10);
    fno number(5) := 18900;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$CONDUCTOR_TRANSMISION';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
    cursor ElementosSostenidos(cCodigo VARCHAR2) is
      select e.g3e_fid, e.g3e_fno
        from x$conductor_transmision cp
       inner join x$conectividad con
          on con.code = cp.elnode1 --elnode1
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 17400)
      union
      select e.g3e_fid, e.g3e_fno
        from x$conductor_transmision cp
       inner join x$conectividad con
          on con.code = cp.elnode2 --elnode2
       inner join etl_code2fid e
          on e.code = con.phnode --mvphonde
       where cp.code = cCodigo
         and e.g3e_fno in (17100, 17000, 17400);
  
  begin
  
    for c in (select *
                from x$conductor_transmision
               where fparent = pCircuito) loop
      geom := ConvertLine2Sigma(c.xpos1,
                                c.ypos1,
                                BuscarZetaNodoEletrico(c.elnode1),
                                c.xpos2,
                                c.ypos2,
                                BuscarZetaNodoEletrico(c.elnode2));
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS1; --
      vCCOMUN.COOR_Y         := c.YPOS1; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECON_TRA_AT
      begin
      
        vECON_TRA_AT := null;
      
        vECON_TRA_AT.CODE_CONDUCTOR := c.CONDUCTOR;
        vECON_TRA_AT.CODIGO         := c.CODE;
        vECON_TRA_AT.CODE_GUARDA    := c.GUARDA;
      
        select calibre, material
          into vECON_TRA_AT.CALIBRE, vECON_TRA_AT.MATERIAL
          from conducto
         where code = c.CONDUCTOR
           and rownum = 1;
      
        vECON_TRA_AT.CALIBRE_CABLEG  := null;
        vECON_TRA_AT.MATERIAL_CABLEG := null;
      
        if vECON_TRA_AT.CODE_GUARDA is not null then
          select calibre, material
            into vECON_TRA_AT.CALIBRE_CABLEG, vECON_TRA_AT.MATERIAL_CABLEG
            from conducto
           where code = c.GUARDA
             and rownum = 1;
        end if;
      
        vECON_TRA_AT.PROPIETARIO := c.owner;
        if vECON_TRA_AT.PROPIETARIO is not null then
          if c.towner = 'C' and c.owner != 'CHEC' then
            insert into etl_transformacion_log
            values
              (vTABLA_EXTRACION,
               'T$ECON_TRA_AT',
               c.code,
               2,
               cErrorPropietario,
               pCircuito,
               sysdate);
            commit;
            vECON_TRA_AT.PROPIETARIO := 'PARTICULAR';
          end if;
        end if;
      
        vECON_TRA_AT.G3E_FNO := fno;
        vECON_TRA_AT.G3E_CNO := fno + 1;
        vECON_TRA_AT.G3E_CID := 1;
        vECON_TRA_AT.G3E_FID := fid;
        vECON_TRA_AT.G3E_ID  := ECON_TRA_AT_SEQ.NEXTVAL;
      
        insert into T$ECON_TRA_AT values vECON_TRA_AT;
      
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECON_TRA_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.code,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ECON_TRA_LN
      vECON_TRA_LN              := null;
      vECON_TRA_LN.G3E_FNO      := fno;
      vECON_TRA_LN.G3E_CNO      := fno + 20;
      vECON_TRA_LN.G3E_CID      := 1;
      vECON_TRA_LN.G3E_FID      := fid;
      vECON_TRA_LN.G3E_ID       := ECON_TRA_LN_SEQ.NEXTVAL;
      vECON_TRA_LN.G3E_GEOMETRY := geom;
    
      insert into T$ECON_TRA_LN values vECON_TRA_LN;
    
      --CCONECTIVIDAD_E
      vCONN          := null;
      vCONN.NODO1_ID := PTC_BuscarNodoConductor(c.code, c.elnode1);
      vCONN.NODO2_ID := PTC_BuscarNodoConductor(c.code, c.elnode2);
    
      if vCONN.NODO1_ID = 0 then
        vCONN.NODO1_ID := g3e_node_seq.nextval;
        update CHEC_NODOS_ELEC
           set nivel = 'MVELNODE', nodo_mde = vCONN.NODO1_ID
         where code = c.elnode1;
      end if;
    
      if vCONN.NODO2_ID = 0 then
        vCONN.NODO2_ID := g3e_node_seq.nextval;
        update CHEC_NODOS_ELEC
           set nivel = 'MVELNODE', nodo_mde = vCONN.NODO2_ID
         where code = c.elnode2;
      end if;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      vCONN.LOCALIZACION  := c.CLASS;
      vCONN.LONGITUD      := to_number(c.LENGTH);
      vCONN.FASES         := DecodeFase(c.PHASES);
      vCONN.CIRCUITO      := c.FPARENT;
      vCONN.TENSION       := to_char(c.KVNOM * 1000);
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CCONTENEDOR
      vCCONTENEDOR := null;
    
      select max(G3E_CID)
        into vCCONTENEDOR.G3E_CID
        from T$CCONTENEDOR
       where G3E_FID = fid;
    
      if vCCONTENEDOR.G3E_CID is null then
        vCCONTENEDOR.G3E_CID := 0;
      end if;
    
      vCCONTENEDOR.G3E_FNO := fno;
      vCCONTENEDOR.G3E_CNO := 65;
      vCCONTENEDOR.G3E_FID := fid;
    
      for torres in ElementosSostenidos(c.code) loop
      
        vCCONTENEDOR.G3E_CID      := vCCONTENEDOR.G3E_CID + 1;
        vCCONTENEDOR.G3E_ID       := CCONTENEDOR_SEQ.NEXTVAL;
        vCCONTENEDOR.G3E_OWNERFID := torres.g3e_fid;
        vCCONTENEDOR.G3E_OWNERFNO := torres.g3e_fno;
      
        insert into T$CCONTENEDOR values vCCONTENEDOR;
      
      end loop;
    
      --CPERTENENCIA
      vCPERTENENCIA := null;
    
      vCPERTENENCIA.G3E_CID := 1;
      vCPERTENENCIA.G3E_ID  := CPERTENENCIA_SEQ.NEXTVAL;
      vCPERTENENCIA.G3E_FNO := fno;
      vCPERTENENCIA.G3E_CNO := 64;
      vCPERTENENCIA.G3E_FID := fid;
    
      insert into T$CPERTENENCIA values vCPERTENENCIA;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_BARRAJE(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vEBARRAJE_AT T$EBARRAJE_AT%ROWTYPE;
    vEBARRAJE_LN T$EBARRAJE_LN%ROWTYPE;
  
    fid number(10);
    fno number(5) := 18700;
  
    vCount NUMBER(3);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$BARRAJE';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select *
                from x$barraje
               where substation = substr(pCircuito, 1, 3)) loop
    
      select count(1) into vCount from T$EBARRAJE_AT where codigo = c.code;
      if vCount > 0 then
        continue;
      end if;
      geom := ConvertPoint2Barraje(c.xpos, c.ypos, c.orientatio);
    
      fid := g3e_fid_seq.nextval;
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EBARRAJE_AT
      begin
        vEBARRAJE_AT         := null;
        vEBARRAJE_AT.G3E_FNO := fno;
        vEBARRAJE_AT.G3E_CNO := fno + 1;
        vEBARRAJE_AT.G3E_CID := 1;
        vEBARRAJE_AT.G3E_FID := fid;
        vEBARRAJE_AT.G3E_ID  := EBARRAJE_AT_SEQ.NEXTVAL;
      
        vEBARRAJE_AT.CODIGO     := c.CODE;
        vEBARRAJE_AT.MVA3PH_SCC := c.MVA3PH_SCC;
        vEBARRAJE_AT.MVA1PH_SCC := c.MVA1PH_SCC;
      
        insert into T$EBARRAJE_AT values vEBARRAJE_AT;
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EBARRAJE_AT',
             c.code,
             1,
             cErrorGeneral,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EBARRAJE_LN
      vEBARRAJE_LN              := null;
      vEBARRAJE_LN.G3E_FNO      := fno;
      vEBARRAJE_LN.G3E_CNO      := fno + 20;
      vEBARRAJE_LN.G3E_CID      := 1;
      vEBARRAJE_LN.G3E_FID      := fid;
      vEBARRAJE_LN.G3E_ID       := EBARRAJE_LN_SEQ.NEXTVAL;
      vEBARRAJE_LN.G3E_GEOMETRY := geom;
    
      insert into T$EBARRAJE_LN values vEBARRAJE_LN;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
    
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      vCONN.CIRCUITO := null;
      vCONN.TENSION  := to_char(c.KV);
      vCONN.NODO1_ID := 0;
      vCONN.NODO2_ID := 0;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_SUBESTACION(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vESUBESTA_AT T$ESUBESTA_AT%ROWTYPE;
    vESUBESTA_AR T$ESUBESTA_AR%ROWTYPE;
  
    fid number(10);
    fno number(5) := 17700;
  
    vCount NUMBER(3);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$SUBESTACION';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select *
                from x$subestacion
               where code = substr(pCircuito, 1, 3)) loop
    
      select count(1) into vCount from T$ESUBESTA_AT where codigo = c.code;
      if vCount > 0 then
        continue;
      end if;
      geom := ConvertPoint2Subestacion(c.xpos, c.ypos, 0.15);
    
      fid := g3e_fid_seq.nextval;
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_id         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ESUBESTA_AT
      begin
        vESUBESTA_AT         := null;
        vESUBESTA_AT.G3E_FNO := fno;
        vESUBESTA_AT.G3E_CNO := fno + 1;
        vESUBESTA_AT.G3E_CID := 1;
        vESUBESTA_AT.G3E_FID := fid;
        vESUBESTA_AT.G3E_ID  := ESUBESTA_AT_SEQ.NEXTVAL;
      
        vESUBESTA_AT.CODIGO     := c.CODE;
        vESUBESTA_AT.NOMBRE_SUB := c.DESCRIPTIO;
      
        insert into T$ESUBESTA_AT values vESUBESTA_AT;
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ESUBESTA_AT',
             c.code,
             1,
             cErrorGeneral,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ESUBESTA_AR
      vESUBESTA_AR              := null;
      vESUBESTA_AR.G3E_FNO      := fno;
      vESUBESTA_AR.G3E_CNO      := fno + 30;
      vESUBESTA_AR.G3E_CID      := 1;
      vESUBESTA_AR.G3E_FID      := fid;
      vESUBESTA_AR.G3E_ID       := ESUBESTA_AR_SEQ.NEXTVAL;
      vESUBESTA_AR.G3E_GEOMETRY := geom;
    
      insert into T$ESUBESTA_AR values vESUBESTA_AR;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_TRANSF_POT(pCircuito in varchar2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
  
    vCCOMUN       T$CCOMUN%ROWTYPE;
    vETRA_PTNC_AT T$ETRA_PTNC_AT%ROWTYPE;
    vETRA_PTNC_PT T$ETRA_PTNC_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 29000;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$TRANSF_POT';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from X$TRANSF_POT) loop
      geom := ConvertPoint2Subestacion(c.xpos, c.ypos, 15);
    
      fid := g3e_fid_seq.nextval;
      --CCOMUN
      vCCOMUN         := null;
      vCCOMUN.G3E_FNO := fno;
      vCCOMUN.G3E_CNO := 60;
      vCCOMUN.G3E_CID := 1;
      vCCOMUN.G3E_FID := fid;
      vCCOMUN.G3E_id  := CCOMUN_SEQ.NEXTVAL;
      --vCCOMUN.OBSERVACIONES := c.DESCRIPTIO; --
      --vCCOMUN.UBICACION     := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON := geom.SDO_ORDINATES(1); --
      --vCCOMUN.PROYECTO      := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ETRA_PTNC_AT
      begin
        vETRA_PTNC_AT         := null;
        vETRA_PTNC_AT.G3E_FNO := fno;
        vETRA_PTNC_AT.G3E_CNO := fno + 1;
        vETRA_PTNC_AT.G3E_CID := 1;
        vETRA_PTNC_AT.G3E_FID := fid;
        vETRA_PTNC_AT.G3E_ID  := ETRA_PTNC_AT_SEQ.NEXTVAL;
      
        vETRA_PTNC_AT.CODIGO_PT  := c.CODE;
        vETRA_PTNC_AT.FABRICANTE := c.MAKER;
        vETRA_PTNC_AT.PRECISION  := c.PRESS;
        vETRA_PTNC_AT.RELACION   := c.RELAS;
        vETRA_PTNC_AT.REL_ACTUAL := c.RACTUAL;
        vETRA_PTNC_AT.IMPEDANCIA := c.BURDEN;
        vETRA_PTNC_AT.CLASE      := c.CLASE;
        vETRA_PTNC_AT.TIPO_PT    := c.TIPO;
        vETRA_PTNC_AT.ERROR_FASE := c.EFASE;
        vETRA_PTNC_AT.ERROR_MAGN := c.EMAG;
      
        insert into T$ETRA_PTNC_AT values vETRA_PTNC_AT;
      exception
        when others then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ETRA_PTNC_AT',
             c.code,
             1,
             cErrorGeneral,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ETRA_PTNC_PT
      vETRA_PTNC_PT              := null;
      vETRA_PTNC_PT.G3E_FNO      := fno;
      vETRA_PTNC_PT.G3E_CNO      := fno + 10;
      vETRA_PTNC_PT.G3E_CID      := 1;
      vETRA_PTNC_PT.G3E_FID      := fid;
      vETRA_PTNC_PT.G3E_ID       := ETRA_PTNC_PT_SEQ.NEXTVAL;
      vETRA_PTNC_PT.G3E_GEOMETRY := geom;
    
      insert into T$ETRA_PTNC_PT values vETRA_PTNC_PT;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, null);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_TRANSFORMADOR(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vETRANSFO_AT T$ETRANSFO_AT%ROWTYPE;
    vETRANSFO_PT T$ETRANSFO_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 20400;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$TRANSFORMADOR';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from X$TRANSFORMADOR where fparent = pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                   := null;
      vCCOMUN.G3E_FNO           := fno;
      vCCOMUN.G3E_CNO           := 60;
      vCCOMUN.G3E_CID           := 1;
      vCCOMUN.G3E_FID           := fid;
      vCCOMUN.G3E_ID            := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES     := c.DESCRIPTIO; --
      vCCOMUN.UBICACION         := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT      := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON      := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO          := c.PROJECT; --
      vCCOMUN.FECHA_REMPLAZO    := c.DATE_REM;
      vCCOMUN.FECHA_FABRICACION := c.DATE_FAB;
      vCCOMUN.FECHA_OPERACION   := c.DATE_INST;
      vCCOMUN.COOR_X            := c.XPOS; --
      vCCOMUN.COOR_Y            := c.YPOS; --
      vCCOMUN.DEPARTAMENTO      := SpatialDepartamento(geom,
                                                       c.code,
                                                       pCircuito,
                                                       vTABLA_EXTRACION); --
    
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ETRANSFO_AT
    
      begin
        vETRANSFO_AT                  := null;
        vETRANSFO_AT.CODIGO           := c.CODE;
        vETRANSFO_AT.TIPO_PROPIETARIO := c.OWNER1;
        case c.AUTOPROT
          when 'S' then
            vETRANSFO_AT.TIPO_PROTECCION := 'Autoprotegido';
          when 'N' then
            vETRANSFO_AT.TIPO_PROTECCION := 'Convencional';
          else
            vETRANSFO_AT.TIPO_PROTECCION := null;
        end case;
      
        vETRANSFO_AT.IMPEDANCIA        := c.IMPEDANCE;
        vETRANSFO_AT.NRO_TRANSFORMADOR := -1;
        vETRANSFO_AT.FABRICANTE_TRAFO  := c.MARCA;
        vETRANSFO_AT.TIPO_SUBESTACION  := c.TIPOSUB;
        vETRANSFO_AT.AREA              := c.ZONE; --MIRAR ESEte
        vETRANSFO_AT.USO               := c.CASO;
        if vETRANSFO_AT.USO is null then
          vETRANSFO_AT.USO := 'NORMAL';
        end if;
      
        vETRANSFO_AT.DESCNX_CON_REGLETA := 'NO';
      
        vETRANSFO_AT.BANCO        := c.NUM_TRFS;
        vETRANSFO_AT.SERIAL       := c.SERIAL;
        vETRANSFO_AT.TENSION_CREG := c.NCALIDAD;
        vETRANSFO_AT.ZONA_RIESGO  := 'DESCONOCIDO';
      
        if vCCOMUN.DEPARTAMENTO is null or vCCOMUN.MUNICIPIO is null then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ETRANSFO_AT',
             c.code,
             1,
             cErrorDepMun,
             pCircuito,
             sysdate);
          commit;
          continue;
        end if;
      
        vETRANSFO_AT.DEPARTAMENTO := vCCOMUN.DEPARTAMENTO;
        vETRANSFO_AT.MUNICIPIO    := vCCOMUN.MUNICIPIO;
      
        vETRANSFO_AT.G3E_FNO := fno;
        vETRANSFO_AT.G3E_CNO := fno + 1;
        vETRANSFO_AT.G3E_CID := 1;
        vETRANSFO_AT.G3E_FID := fid;
        vETRANSFO_AT.G3E_id  := ETRANSFO_AT_SEQ.NEXTVAL;
      
        insert into T$ETRANSFO_AT values vETRANSFO_AT;
      
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ETRANSFO_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ETRANSFO_PT
      vETRANSFO_PT              := null;
      vETRANSFO_PT.G3E_FNO      := fno;
      vETRANSFO_PT.G3E_CNO      := fno + 10;
      vETRANSFO_PT.G3E_CID      := 1;
      vETRANSFO_PT.G3E_FID      := fid;
      vETRANSFO_PT.G3E_ID       := ETRANSFO_PT_SEQ.NEXTVAL;
      vETRANSFO_PT.G3E_GEOMETRY := geom;
    
      insert into T$ETRANSFO_PT values vETRANSFO_PT;
    
      --CCONECTIVIDAD_E
      vCONN := null;
      -- vCONN.NODO_TRANSFORM := c.CODE;
      vCONN.FASES    := c.PHASES;
      vCONN.CIRCUITO := c.FPARENT;
      case c.TIPO_RED
        when 'S' then
          vCONN.LOCALIZACION := 'SUBTERRANEO';
        when 'A' then
          vCONN.LOCALIZACION := 'AEREO';
        else
          vCONN.LOCALIZACION := null;
      end case;
    
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      select KVA
        into VCONN.CAPACIDAD_NOMINAL
        from trftypes
       where code = c.trftype
         and rownum = 1;
    
      vCONN.CIRCUITO := c.FPARENT;
      -- vCONN.TENSION  := to_char(c.KV);
      vCONN.NODO1_ID := PTC_BuscarNodoTransforAlta(c.elnode);
      vCONN.NODO2_ID := PTC_BuscarNodoTransforBaja(c.lvelnode);
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        /*
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito, sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
        */
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_PARARRAYO(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vCONN        T$CCONECTIVIDAD_E%ROWTYPE;
    vEPARARRA_AT T$EPARARRA_AT%ROWTYPE;
    vEPARARRA_PT T$EPARARRA_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 20100;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$PARARRAYOS';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select p.*,
                     c.owner,
                     c.towner,
                     c.isconnecte,
                     c.spacing,
                     c.height,
                     c.xpos,
                     c.ypos,
                     c.phases,
                     c.assembly,
                     c.fparent,
                     c.code elnode
                from x$pararrayos p
               inner join x$conectividad c
                  on c.prayos = p.code
               where c.fparent = pCircuito) loop
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos, c.height);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN              := null;
      vCCOMUN.G3E_FNO      := fno;
      vCCOMUN.G3E_CNO      := 60;
      vCCOMUN.G3E_CID      := 1;
      vCCOMUN.G3E_FID      := fid;
      vCCOMUN.G3E_ID       := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.COOR_GPS_LAT := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON := geom.SDO_ORDINATES(1); --
    
      vCCOMUN.COOR_X       := c.XPOS; --
      vCCOMUN.COOR_Y       := c.YPOS; --
      vCCOMUN.DEPARTAMENTO := SpatialDepartamento(geom,
                                                  c.code,
                                                  pCircuito,
                                                  vTABLA_EXTRACION); --
    
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EPARARRA_AT
      begin
        vEPARARRA_AT                   := null;
        vEPARARRA_AT.TENSION_OPERACION := c.VNOM;
        vEPARARRA_AT.TIPO_PARARRAYOS   := c.CLASE;
        vEPARARRA_AT.CODIGO            := c.CODE;
      
        select t.id_cat
          into vEPARARRA_AT.CARACTERISTICAS_TECNICAS
          from EPARARRA_TECNICA t
         where t.vcebado = c.vcebado
           and t.vmax = c.vmax
           and t.bil = c.bil;
      
        vEPARARRA_AT.G3E_FNO := fno;
        vEPARARRA_AT.G3E_CNO := fno + 1;
        vEPARARRA_AT.G3E_CID := 1;
        vEPARARRA_AT.G3E_FID := fid;
        vEPARARRA_AT.G3E_id  := EPARARRA_AT_SEQ.NEXTVAL;
      
        insert into T$EPARARRA_AT values vEPARARRA_AT;
      
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EPARARRA_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EPARARRA_PT
      vEPARARRA_PT              := null;
      vEPARARRA_PT.G3E_FNO      := fno;
      vEPARARRA_PT.G3E_CNO      := fno + 10;
      vEPARARRA_PT.G3E_CID      := 1;
      vEPARARRA_PT.G3E_FID      := fid;
      vEPARARRA_PT.G3E_ID       := EPARARRA_PT_SEQ.NEXTVAL;
      vEPARARRA_PT.G3E_GEOMETRY := geom;
    
      insert into T$EPARARRA_PT values vEPARARRA_PT;
    
      --CCONECTIVIDAD_E
      vCONN := null;
      -- vCONN.NODO_TRANSFORM := c.CODE;
      vCONN.FASES    := DecodeFase(c.PHASES);
      vCONN.CIRCUITO := c.FPARENT;
      --vCONN.LOCALIZACION   := c.OWNER2;
    
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      vCONN.CIRCUITO := c.FPARENT;
      -- vCONN.TENSION  := to_char(c.KV);
      vCONN.NODO1_ID := PTC_BuscarNodoTransforAlta(c.elnode);
      vCONN.NODO2_ID := 0;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_INDICADOR(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
  
    vCCOMUN       T$CCOMUN%ROWTYPE;
    vPROP         T$CPROPIETARIO%ROWTYPE;
    vCPERTENENCIA T$CPERTENENCIA%ROWTYPE;
    vEINDICA_AT   T$EINDICA_AT%ROWTYPE;
    vEINDICA_PT   T$EINDICA_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 20500;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$INDICADOR_FALLA';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select p.*,
                     c.owner,
                     c.towner,
                     c.isconnecte,
                     c.spacing,
                     c.height,
                     c.xpos,
                     c.ypos,
                     c.phases,
                     c.assembly,
                     c.fparent
                from x$indicador_falla p
               inner join x$conectividad c
                  on c.ifalla = p.code
               where c.fparent = pCircuito) loop
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos, c.height);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN              := null;
      vCCOMUN.G3E_FNO      := fno;
      vCCOMUN.G3E_CNO      := 60;
      vCCOMUN.G3E_CID      := 1;
      vCCOMUN.G3E_FID      := fid;
      vCCOMUN.G3E_ID       := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.COOR_GPS_LAT := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON := geom.SDO_ORDINATES(1); --
    
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EPARARRA_AT
      begin
        vEINDICA_AT                     := null;
        vEINDICA_AT.FABRICANTE          := c.MAKER;
        vEINDICA_AT.TIPO_EQUIPO         := c.TIPO;
        vEINDICA_AT.CODIGO              := c.CODE;
        vEINDICA_AT.CORRIENTE_ACTUACION := c.IFALLA;
        --vEINDICA_AT.INOM            := c.INOM;
      
        vEINDICA_AT.G3E_FNO := fno;
        vEINDICA_AT.G3E_CNO := fno + 1;
        vEINDICA_AT.G3E_CID := 1;
        vEINDICA_AT.G3E_FID := fid;
        vEINDICA_AT.G3E_Id  := EINDICA_AT_SEQ.NEXTVAL;
      
        insert into T$EINDICA_AT values vEINDICA_AT;
      
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$EPARARRA_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --EINDICA_PT
      vEINDICA_PT              := null;
      vEINDICA_PT.G3E_FNO      := fno;
      vEINDICA_PT.G3E_CNO      := fno + 10;
      vEINDICA_PT.G3E_CID      := 1;
      vEINDICA_PT.G3E_FID      := fid;
      vEINDICA_PT.G3E_ID       := EINDICA_PT_SEQ.NEXTVAL;
      vEINDICA_PT.G3E_GEOMETRY := geom;
    
      insert into T$EINDICA_PT values vEINDICA_PT;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      --CPERTENENCIA
      vCPERTENENCIA := null;
    
      vCPERTENENCIA.G3E_CID     := 1;
      vCPERTENENCIA.G3E_ID      := CPERTENENCIA_SEQ.NEXTVAL;
      vCPERTENENCIA.G3E_FNO     := fno;
      vCPERTENENCIA.G3E_CNO     := 64;
      vCPERTENENCIA.G3E_FID     := fid;
      vCPERTENENCIA.G3E_OWNERID := PTC_BuscarOwnerIndicador(c.elnode);
    
      insert into T$CPERTENENCIA values vCPERTENENCIA;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_FEEDER(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN          T$CCOMUN%ROWTYPE;
    vPROP            T$CPROPIETARIO%ROWTYPE;
    vCONN            T$CCONECTIVIDAD_E%ROWTYPE;
    vEINTERRU_AT     T$EINTERRU_AT%ROWTYPE;
    vEINTERRU_PT     T$EINTERRU_PT%ROWTYPE;
    vEINTERRU_CTO_TX T$EINTERRU_CTO_TX%ROWTYPE;
  
    fid        number(10);
    fno        number(5) := 18800;
    fidConduct number(10);
    nodePos    number(2);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$FEEDERS';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select g.*
                from x$feeders f
               inner join (select 'INTERRUPTOR' as TIPO_ALIMENTADOR, h.*
                            from x$interruptor h
                          union
                          select 'RECONECTADOR' as TIPO_ALIMENTADOR, h.*
                            from x$reconectador h
                          union
                          select 'CUCHILLA' as TIPO_ALIMENTADOR, h.*
                            from x$cuchilla h) g
                  on g.code = f.code
               where f.fparent = pCircuito) loop
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN               := null;
      vCCOMUN.G3E_FNO       := fno;
      vCCOMUN.G3E_CNO       := 60;
      vCCOMUN.G3E_CID       := 1;
      vCCOMUN.G3E_FID       := fid;
      vCCOMUN.G3E_id        := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES := c.DESCRIPTIO; --
      vCCOMUN.UBICACION     := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT  := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON  := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO      := c.PROJECT; --
      vCCOMUN.COOR_X        := c.XPOS; --
      vCCOMUN.COOR_Y        := c.YPOS; --
      vCCOMUN.DEPARTAMENTO  := SpatialDepartamento(geom,
                                                   c.code,
                                                   pCircuito,
                                                   vTABLA_EXTRACION); --
    
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --vEINTERRU_AT  
    
      vEINTERRU_AT                  := null;
      vEINTERRU_AT.G3E_FNO          := fno;
      vEINTERRU_AT.G3E_CNO          := fno + 1;
      vEINTERRU_AT.G3E_CID          := 1;
      vEINTERRU_AT.G3E_FID          := fid;
      vEINTERRU_AT.G3E_ID           := EINTERRU_AT_SEQ.NEXTVAL;
      vEINTERRU_AT.CODIGO           := c.CODE;
      vEINTERRU_AT.FABRICANTE       := c.MAKER;
      vEINTERRU_AT.TIPO_INTERRUPTOR := c.ASSEMBLY;
    
      insert into T$EINTERRU_AT values vEINTERRU_AT;
    
      --EINTERRU_PT
      vEINTERRU_PT              := null;
      vEINTERRU_PT.G3E_FNO      := fno;
      vEINTERRU_PT.G3E_CNO      := fno + 10;
      vEINTERRU_PT.G3E_CID      := 1;
      vEINTERRU_PT.G3E_FID      := fid;
      vEINTERRU_PT.G3E_ID       := EINTERRU_PT_SEQ.NEXTVAL;
      vEINTERRU_PT.G3E_GEOMETRY := geom;
    
      insert into T$EINTERRU_PT values vEINTERRU_PT;
    
      --EINTERRU_CTO_TX
      vEINTERRU_CTO_TX              := null;
      vEINTERRU_CTO_TX.G3E_FNO      := fno;
      vEINTERRU_CTO_TX.G3E_CNO      := fno + 40;
      vEINTERRU_CTO_TX.G3E_CID      := 1;
      vEINTERRU_CTO_TX.G3E_FID      := fid;
      vEINTERRU_CTO_TX.G3E_ID       := EINTERRU_CTO_TX_SEQ.NEXTVAL;
      vEINTERRU_CTO_TX.G3E_GEOMETRY := ConvertPoint2SigmaTexto(c.xpos,
                                                               c.ypos);
    
      insert into T$EINTERRU_CTO_TX values vEINTERRU_CTO_TX;
    
      --CCONECTIVIDAD_E
      vCONN                   := null;
      vCONN.CAPACIDAD_NOMINAL := c.AMP;
      vCONN.FASES             := DecodeFase(c.PHASES);
      vCONN.ESTADO            := 'OPERACION';
    
      if c.STATE = 0 then
        vCONN.EST_ESTABLE   := 'OPEN';
        vCONN.EST_OPERATIVO := 'OPEN';
      else
        vCONN.EST_ESTABLE   := 'CLOSED';
        vCONN.EST_OPERATIVO := 'CLOSED';
      end if;
    
      vCONN.CIRCUITO := c.FPARENT;
      vCONN.TENSION  := to_char(c.KV);
      vCONN.NODO1_ID := PTC_BuscarNodoSwitches(c.code,
                                               c.linesectio,
                                               c.xpos,
                                               c.ypos,
                                               fidConduct,
                                               nodePos);
    
      vCONN.NODO2_ID := g3e_node_seq.nextval;
    
      begin
        select TPARENT
          into vCONN.NODO_TRANSFORM
          from x$conectividad
         where phnode = c.code
           and tparent is not null
           and rownum = 1;
      exception
        when others then
          null;
      end;
    
      -- if nodePos = 1 then
      update T$CCONECTIVIDAD_E
         set nodo2_id = vCONN.NODO2_ID
       where nodo2_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
    
      --  if nodePos = 2 then
      update T$CCONECTIVIDAD_E
         set nodo1_id = vCONN.NODO2_ID
       where nodo1_id = vCONN.NODO1_ID
         and g3e_fid != fidConduct;
      -- end if;
      commit;
    
      vCONN.G3E_FNO := fno;
      vCONN.G3E_CNO := 61;
      vCONN.G3E_CID := 1;
      vCONN.G3E_FID := fid;
      vCONN.G3E_ID  := CCONECTIVIDAD_E_SEQ.NEXTVAL;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
    
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_LUMINARIA(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
  
    vCCOMUN       T$CCOMUN%ROWTYPE;
    vCONN         T$CCONECTIVIDAD_E%ROWTYPE;
    vPROP         T$CPROPIETARIO%ROWTYPE;
    vCPERTENENCIA T$CPERTENENCIA%ROWTYPE;
    vELUMINAR_AT  T$ELUMINAR_AT%ROWTYPE;
    vELUMINAR_PT  T$ELUMINAR_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 21400;
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$LUMINARIA';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select p.*,
                     c.towner,
                     c.isconnecte,
                     c.spacing,
                     c.height,
                     c.assembly,
                     c.fparent
                from x$luminaria p
               inner join x$conectividad c
                  on c.code = p.elnode
               where c.fparent = pCircuito) loop
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos, c.height);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN              := null;
      vCCOMUN.G3E_FNO      := fno;
      vCCOMUN.G3E_CNO      := 60;
      vCCOMUN.G3E_CID      := 1;
      vCCOMUN.G3E_FID      := fid;
      vCCOMUN.G3E_ID       := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.COOR_GPS_LAT := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON := geom.SDO_ORDINATES(1); --
    
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ELUMINAR_AT
      begin
        vELUMINAR_AT                       := null;
        vELUMINAR_AT.CODIGO_IDENTIFICACION := c.CODE;
        vELUMINAR_AT.NUMERO_LUMINARIA      := c.ETIQUETA;
        vELUMINAR_AT.HORAS_ACTIVA          := c.F_UTILIZACION * 24;
        vELUMINAR_AT.POTENCIA_ANTERIOR     := c.KW;
        vELUMINAR_AT.POTENCIA              := c.KW;
        vELUMINAR_AT.PERDIDAS              := c.PERDIDAS;
        vELUMINAR_AT.LOCALIZACION          := 'POSTE';
        vELUMINAR_AT.FECHA_INST_BOMB       := TO_DATE('1800-01-01',
                                                      'YYYY-MM-DD');
      
        vELUMINAR_AT.G3E_FNO := fno;
        vELUMINAR_AT.G3E_CNO := fno + 1;
        vELUMINAR_AT.G3E_CID := 1;
        vELUMINAR_AT.G3E_FID := fid;
        vELUMINAR_AT.G3E_ID  := ELUMINAR_AT_SEQ.NEXTVAL;
      
        insert into T$ELUMINAR_AT values vELUMINAR_AT;
      
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ELUMINAR_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --ELUMINAR_PT
      vELUMINAR_PT              := null;
      vELUMINAR_PT.G3E_FNO      := fno;
      vELUMINAR_PT.G3E_CNO      := fno + 10;
      vELUMINAR_PT.G3E_CID      := 1;
      vELUMINAR_PT.G3E_FID      := fid;
      vELUMINAR_PT.G3E_ID       := ELUMINAR_PT_SEQ.NEXTVAL;
      vELUMINAR_PT.G3E_GEOMETRY := geom;
    
      insert into T$ELUMINAR_PT values vELUMINAR_PT;
    
      --CCONECTIVIDAD_E
      vCONN.FASES    := DecodeFase(c.PHASES);
      vCONN.CIRCUITO := c.FPARENT;
    
      vCONN.EST_ESTABLE   := 'CLOSED';
      vCONN.EST_OPERATIVO := 'CLOSED';
    
      vCONN.CIRCUITO := c.FPARENT;
      -- vCONN.TENSION  := to_char(c.KV);
      vCONN.NODO1_ID := PTC_BuscarNodoTransforAlta(c.elnode);
      vCONN.NODO2_ID := PTC_BuscarNodoTransforBaja(c.elnode);
    
      vCONN.G3E_FNO        := fno;
      vCONN.G3E_CNO        := 61;
      vCONN.G3E_CID        := 1;
      vCONN.G3E_FID        := fid;
      vCONN.G3E_ID         := CCONECTIVIDAD_E_SEQ.NEXTVAL;
      vCONN.NODO_TRANSFORM := c.TPARENT;
    
      insert into T$CCONECTIVIDAD_E values vCONN;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      --CPERTENENCIA
      vCPERTENENCIA := null;
    
      vCPERTENENCIA.G3E_CID     := 1;
      vCPERTENENCIA.G3E_ID      := CPERTENENCIA_SEQ.NEXTVAL;
      vCPERTENENCIA.G3E_FNO     := fno;
      vCPERTENENCIA.G3E_CNO     := 64;
      vCPERTENENCIA.G3E_FID     := fid;
      vCPERTENENCIA.G3E_OWNERID := PTC_BuscarOwnerIndicador(c.elnode);
    
      insert into T$CPERTENENCIA values vCPERTENENCIA;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_CAJA_DIS(pCircuito in VARCHAR2) is
    geom sdo_geometry;
  
    vERROR VARCHAR2(80);
    vUSER  VARCHAR2(100);
  
    vCCOMUN      T$CCOMUN%ROWTYPE;
    vPROP        T$CPROPIETARIO%ROWTYPE;
    vECAJ_DIS_AT T$ECAJ_DIS_AT%ROWTYPE;
    vECAJ_DIS_PT T$ECAJ_DIS_PT%ROWTYPE;
  
    fid number(10);
    fno number(5) := 18100;
  
    vCount number(5);
  
    vTABLA_EXTRACION VARCHAR2(80) := 'X$CAJA_DISTRIBUICION';
  
    vTime      timestamp := CURRENT_TIMESTAMP;
    vRegistros number(8) := 0;
  
  begin
  
    for c in (select * from X$CAJA_DISTRIBUICION) loop
    
      select count(1)
        into vCount
        from x$conectividad
       where phnode = c.code
         and fparent = pCircuito;
    
      if vCount = 0 then
        continue;
      end if;
    
      geom := ConvertPoint2Sigma(c.xpos, c.ypos);
    
      fid := g3e_fid_seq.nextval;
    
      --CCOMUN
      vCCOMUN                := null;
      vCCOMUN.G3E_FNO        := fno;
      vCCOMUN.G3E_CNO        := 60;
      vCCOMUN.G3E_CID        := 1;
      vCCOMUN.G3E_FID        := fid;
      vCCOMUN.G3E_ID         := CCOMUN_SEQ.NEXTVAL;
      vCCOMUN.OBSERVACIONES  := c.DESCRIPTIO; --
      vCCOMUN.UBICACION      := c.ADDRESS; --
      vCCOMUN.COOR_GPS_LAT   := geom.SDO_ORDINATES(2); --
      vCCOMUN.COOR_GPS_LON   := geom.SDO_ORDINATES(1); --
      vCCOMUN.PROYECTO       := c.PROJECT; --
      vCCOMUN.COOR_X         := c.XPOS; --
      vCCOMUN.COOR_Y         := c.YPOS; --
      vCCOMUN.DEPARTAMENTO   := SpatialDepartamento(geom,
                                                    c.code,
                                                    pCircuito,
                                                    vTABLA_EXTRACION); --
      vCCOMUN.MUNICIPIO      := SpatialMunicipio(geom,
                                                 c.code,
                                                 pCircuito,
                                                 vTABLA_EXTRACION); --
      vCCOMUN.ESTADO         := 'OPERACION'; --
      vCCOMUN.EMPRESA_ORIGEN := 'CHEC';
    
      vUSER := c.USER_;
    
      vCCOMUN.CONTRATO_INSTALACION := null;
      vCCOMUN.TRABAJO_PROGRAMADO   := null;
      vCCOMUN.TIPO_PROYECTO        := null;
    
      if instr(vUSER, '/') > 0 then
        vCCOMUN.TIPO_PROYECTO := substr(vUSER, 1, instr(vUSER, '/') - 1);
        vUSER                 := substr(vUSER, instr(vUSER, '/') + 1);
        if instr(vUSER, '/') > 0 then
          vCCOMUN.CONTRATO_INSTALACION := substr(vUSER,
                                                 1,
                                                 instr(vUSER, '/') - 1);
          vCCOMUN.TRABAJO_PROGRAMADO   := substr(vUSER,
                                                 instr(vUSER, '/') + 1);
        else
          vCCOMUN.CONTRATO_INSTALACION := null;
          vCCOMUN.TRABAJO_PROGRAMADO   := null;
          vCCOMUN.TIPO_PROYECTO        := null;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             2,
             cErrorUser,
             pCircuito,
             sysdate);
          commit;
        end if;
      end if;
    
      if c.POBLACION = 'U' then
        vCCOMUN.CLASIFICACION_MERCADO := 'URBANO';
      end if;
    
      if c.POBLACION = 'R' then
        vCCOMUN.CLASIFICACION_MERCADO := 'RURAL';
      end if;
    
      begin
        insert into T$CCOMUN values vCCOMUN;
      exception
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CCOMUN',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --vECAJ_DIS_AT
      begin
        vECAJ_DIS_AT := null;
      
        DesgloseSubMVPHNODE(c.assembly,
                            vECAJ_DIS_AT.DIMENSIONES,
                            vECAJ_DIS_AT.CLA_CAJA,
                            vECAJ_DIS_AT.CANTIDAD_DUCTOS,
                            vECAJ_DIS_AT.DIMENSION_DUCTO);
      
        vECAJ_DIS_AT.NRO_CAJA := c.CODE;
        vECAJ_DIS_AT.GRUPO    := 'ESTRUCTURAS EYT';
      
        vECAJ_DIS_AT.G3E_FNO := fno;
        vECAJ_DIS_AT.G3E_CNO := fno + 1;
        vECAJ_DIS_AT.G3E_CID := 1;
        vECAJ_DIS_AT.G3E_FID := fid;
        vECAJ_DIS_AT.G3E_id  := ECAJ_DIS_AT_SEQ.NEXTVAL;
      
        insert into T$ECAJ_DIS_AT values vECAJ_DIS_AT;
      
      exception
        when assembly_fisico_invalido then
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECAJ_DIS_AT',
             c.code,
             1,
             cErrorAssemblyFisico || c.assembly,
             pCircuito,
             sysdate);
          commit;
          continue;
        when others then
          vERROR := SUBSTR(SQLERRM, 1, 64);
          rollback;
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$ECAJ_DIS_AT',
             c.code,
             1,
             vERROR,
             pCircuito,
             sysdate);
          commit;
          continue;
      end;
    
      --vECAJ_DIS_PT
      vECAJ_DIS_PT              := null;
      vECAJ_DIS_PT.G3E_FNO      := fno;
      vECAJ_DIS_PT.G3E_CNO      := fno + 10;
      vECAJ_DIS_PT.G3E_CID      := 1;
      vECAJ_DIS_PT.G3E_FID      := fid;
      vECAJ_DIS_PT.G3E_ID       := ECAJ_DIS_PT_SEQ.NEXTVAL;
      vECAJ_DIS_PT.G3E_GEOMETRY := geom;
    
      insert into T$ECAJ_DIS_PT values vECAJ_DIS_PT;
    
      --CPROPIETARIO
      if c.owner is not null then
        vPROP                   := null;
        vPROP.PROPIETARIO_1     := c.owner;
        vPROP.PORCENTAJE_PROP_1 := 100;
      
        if c.towner = 'C' and c.owner != 'CHEC' then
          insert into etl_transformacion_log
          values
            (vTABLA_EXTRACION,
             'T$CPROPIETARIO',
             c.code,
             2,
             cErrorPropietario,
             pCircuito,
             sysdate);
          commit;
          vPROP.PROPIETARIO_1 := 'PARTICULAR';
        end if;
      
        vPROP.G3E_FNO := fno;
        vPROP.G3E_CNO := 69;
        vPROP.G3E_CID := 1;
        vPROP.G3E_FID := fid;
        vPROP.G3E_ID  := CPROPIETARIO_SEQ.NEXTVAL;
      
        insert into T$CPROPIETARIO values vPROP;
      
      end if;
    
      vRegistros := vRegistros + 1;
      insert into ETL_CODE2FID
      values
        (vTABLA_EXTRACION, c.code, fno, fid, pCircuito);
      commit;
    end loop;
    CrearReport(pCircuito, vTABLA_EXTRACION, vRegistros, vTime);
  end;

  procedure PT_FLUJO_CIR(pCircuito in VARCHAR2) is
  
  begin
  
    delete from etl_transformacion_log where circuito = pCircuito;
    commit;
  
    delete from etl_code2fid where circuito = pCircuito;
    commit;
  
    etl_transformacion_chec.pt_subestacion(pCircuito);
    etl_transformacion_chec.pt_barraje(pCircuito);
    etl_transformacion_chec.pt_poste(pCircuito);
    etl_transformacion_chec.pt_torre_transmision(pCircuito);
    etl_transformacion_chec.pt_camara(pCircuito);
    etl_transformacion_chec.pt_caja_dis(pCircuito);
  
    begin
      execute immediate 'drop index idx_t$conn_fid';
    exception
      when others then
        null;
    end;
  
    etl_transformacion_chec.pt_conductor_transmision(pCircuito);
    etl_transformacion_chec.pt_conductor_primario(pCircuito);
    etl_transformacion_chec.pt_conductor_secundario(pCircuito);
  
    execute immediate 'create index idx_t$conn_fid on t$cconectividad_e (g3e_fid)';
    execute immediate 'analyze table t$cconectividad_e compute statistics';
  
    etl_transformacion_chec.pt_nodo_conductor(pCircuito);
    etl_transformacion_chec.pt_feeder(pCircuito);
    etl_transformacion_chec.pt_interruptor(pCircuito);
    etl_transformacion_chec.pt_cuchilla(pCircuito);
    etl_transformacion_chec.pt_aisladero(pCircuito);
    etl_transformacion_chec.pt_reconectador(pCircuito);
    etl_transformacion_chec.pt_referencia(pCircuito);
    etl_transformacion_chec.pt_seccionalizador(pCircuito);
    etl_transformacion_chec.pt_suiche(pCircuito);
  
    etl_transformacion_chec.pt_transformador(pCircuito);
  
    etl_transformacion_chec.pt_pararrayo(pCircuito);
    etl_transformacion_chec.pt_indicador(pCircuito);
    etl_transformacion_chec.pt_luminaria(pCircuito);
  
  end;

  procedure PTN_NORMA_ELETRICA(pPHNODE in VARCHAR2,
                               pFID    in NUMBER,
                               pFNO    in NUMBER) is
    vNORMA T$NORMA%ROWTYPE;
    vEL1   varchar2(2);
    vEL2   varchar2(2);
    vEL3   varchar2(2);
    vEL4   varchar2(2);
    vEL5   varchar2(2);
    vEL6   varchar2(2);
    vEL7   varchar2(2);
  
    vAssembly varchar2(16);
  
  begin
  
    select assembly
      into vAssembly
      from x$conectividad
     where phNode = pPHNODE
       and assembly is not null
       and rownum = 1;
  
    vEL1 := substr(vAssembly, 1, 1);
    vEL2 := substr(vAssembly, 2, 1);
    vEL3 := substr(vAssembly, 3, 1);
    vEL4 := substr(vAssembly, 4, 1);
    vEL5 := substr(vAssembly, 5, 1);
    vEL6 := substr(vAssembly, 6, 1);
    vEL7 := substr(vAssembly, 7);
  
    vNORMA := null;
  
    begin
      select max(nvl(g3e_cid, 0)) + 1
        into vNORMA.G3E_CID
        from T$NORMA
       where g3e_fno = pFNO
         and g3e_fid = pFID;
    exception
      when others then
        vNORMA.G3E_CID := 1;
    end;
  
    --GRUPO 1
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 1'
         and g3e_fno = pFNO
         and valor = vEL1;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
    --GRUPO 2
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 2'
         and g3e_fno = pFNO
         and valor = vEL2;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
    --GRUPO 3
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 3'
         and g3e_fno = pFNO
         and valor = vEL3;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
    --GRUPO 4
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 4'
         and g3e_fno = pFNO
         and valor = vEL4;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
    --GRUPO 5
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 5'
         and g3e_fno = pFNO
         and valor = vEL5;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
    --GRUPO 6
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 6'
         and g3e_fno = pFNO
         and valor = vEL6;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
    --GRUPO 7
    begin
      select norma, cantidad, grupo
        into vNORMA.NORMA, vNORMA.CANTIDAD, vNORMA.GRUPO
        from etl_normas
       where tipo = 'ELETRICO GRUPO 7'
         and g3e_fno = pFNO
         and valor = vEL7;
    
      vNORMA.G3E_FNO := pFNO;
      vNORMA.G3E_CNO := 70;
      vNORMA.G3E_FID := pFID;
      vNORMA.G3E_ID  := NORMA_SEQ.NEXTVAL;
    
      insert into T$NORMA values vNORMA;
    
      vNORMA.G3E_CID := vNORMA.G3E_CID + 1;
    exception
      when others then
        null;
    end;
  
  exception
    when others then
      null;
  end;

  function PTC_BuscarNodoConductor(pCodigo in VARCHAR2, pNodo in VARCHAR2)
    return number is
  
    cursor ConductoresConectados(cCodigo VARCHAR2, cNodo VARCHAR2) is
      select code, decode(elnode1, cNodo, 1, 2) pos
        from x$conductor_primario
       where (elnode1 = cNodo or elnode2 = cNodo)
         and code != cCodigo;
  
    vNodo NUMBER := 0;
    vFID  NUMBER := 0;
  
  begin
  
    for conductor in ConductoresConectados(pCodigo, pNodo) loop
    
      begin
        select g3e_fid
          into vFID
          from etl_code2fid
         where code = conductor.code
           and g3e_fno in (19000, 18900);
      
        if conductor.pos = 1 then
          select nodo1_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        else
          select nodo2_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        end if;
      
        if vNodo != 0 then
        
          return vNodo;
        end if;
      
      exception
        when others then
          continue;
      end;
    
    end loop;
  
    return vNodo;
  
  end;

  function PTC_BuscarNodoConductorSec(pCodigo in VARCHAR2,
                                      pNodo   in VARCHAR2) return number is
  
    cursor ConductoresConectados(cCodigo VARCHAR2, cNodo VARCHAR2) is
      select code, decode(elnode1, cNodo, 1, 2) pos
        from x$conductor_secundario
       where (elnode1 = cNodo or elnode2 = cNodo)
         and code != cCodigo;
  
    vNodo NUMBER := 0;
    vFID  NUMBER := 0;
  
  begin
  
    for conductor in ConductoresConectados(pCodigo, pNodo) loop
    
      begin
        select g3e_fid
          into vFID
          from etl_code2fid
         where code = conductor.code
           and g3e_fno = 21200;
      
        if conductor.pos = 1 then
          select nodo1_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        else
          select nodo2_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        end if;
      
        if vNodo != 0 then
        
          return vNodo;
        end if;
      
      exception
        when others then
          continue;
      end;
    
    end loop;
  
    return vNodo;
  
  end;

  function PTC_BuscarNodoSwitches(pCodigo      in VARCHAR2,
                                  pLineSection in VARCHAR2,
                                  pX           in NUMBER,
                                  pY           in NUMBER,
                                  fidConduct   in out NUMBER,
                                  nodePos      in out NUMBER) return number is
  
    geomPuntoSwitch sdo_geometry;
    geomPuntoCondN1 sdo_geometry;
    geomPuntoCondN2 sdo_geometry;
  
    vCX1 NUMBER;
    vCY1 NUMBER;
    vCX2 NUMBER;
    vCY2 NUMBER;
  
    vNodo NUMBER;
    vFID  NUMBER;
  
    vDist1 NUMBER;
    vDist2 NUMBER;
  
  begin
  
    --Buscar las coordinadas del conductor asociado
  
    select XPOS1, YPOS1, XPOS2, YPOS2
      into vCX1, vCY1, vCX2, vCY2
      from (select code, XPOS1, YPOS1, XPOS2, YPOS2
              from x$conductor_primario
            union all
            select code, XPOS1, YPOS1, XPOS2, YPOS2
              from x$conductor_transmision)
     where code = pLineSection
       and rownum = 1;
  
    geomPuntoSwitch := SDO_GEOMETRY(2001,
                                    21891,
                                    NULL,
                                    SDO_ELEM_INFO_ARRAY(1, 1, 1),
                                    SDO_ORDINATE_ARRAY(pX, pY));
  
    geomPuntoCondN1 := SDO_GEOMETRY(2001,
                                    21891,
                                    NULL,
                                    SDO_ELEM_INFO_ARRAY(1, 1, 1),
                                    SDO_ORDINATE_ARRAY(vCX1, vCY1));
  
    geomPuntoCondN2 := SDO_GEOMETRY(2001,
                                    21891,
                                    NULL,
                                    SDO_ELEM_INFO_ARRAY(1, 1, 1),
                                    SDO_ORDINATE_ARRAY(vCX2, vCY2));
  
    vDist1 := SDO_GEOM.SDO_DISTANCE(geomPuntoCondN1, geomPuntoSwitch, 0.005);
    vDist2 := SDO_GEOM.SDO_DISTANCE(geomPuntoCondN2, geomPuntoSwitch, 0.005);
  
    select g3e_fid into vFID from etl_code2fid where code = pLineSection;
  
    if vDist1 < vDist2 then
      select nodo1_id
        into vNodo
        from t$cconectividad_e
       where g3e_fid = vFid;
      nodePos := 1;
    else
      select nodo2_id
        into vNodo
        from t$cconectividad_e
       where g3e_fid = vFid;
      nodePos := 2;
    end if;
  
    fidConduct := vFid;
    return vNodo;
  exception
    when others then
      return 0;
    
  end;

  function PTC_BuscarNodoTransforAlta(pHNode in VARCHAR2) return number is
  
    vNodo NUMBER := 0;
    vFID  NUMBER := 0;
  
    cursor ConductoresConectados(cNodo VARCHAR2) is
      select code, decode(elnode1, cNodo, 1, 2) pos
        from x$conductor_primario
       where (elnode1 = cNodo or elnode2 = cNodo);
  
  begin
  
    for conductor in ConductoresConectados(pHNode) loop
    
      begin
        select g3e_fid
          into vFID
          from etl_code2fid
         where code = conductor.code;
      
        if conductor.pos = 1 then
          select nodo1_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        else
          select nodo2_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        end if;
      
        if vNodo != 0 then
        
          return vNodo;
        end if;
      
      exception
        when others then
          continue;
      end;
    
    end loop;
  
    return vNodo;
  
  end;

  function PTC_BuscarNodoTransforBaja(pLNode in VARCHAR2) return number is
  
    vNodo NUMBER := 0;
    vFID  NUMBER := 0;
  
    cursor ConductoresConectados(cNodo VARCHAR2) is
      select code, decode(elnode1, cNodo, 1, 2) pos
        from x$conductor_secundario
       where (elnode1 = cNodo or elnode2 = cNodo);
  
  begin
  
    for conductor in ConductoresConectados(pLNode) loop
    
      begin
        select g3e_fid
          into vFID
          from etl_code2fid
         where code = conductor.code;
      
        if conductor.pos = 1 then
          select nodo1_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        else
          select nodo2_id
            into vNodo
            from T$CCONECTIVIDAD_E
           where g3e_fid = vFID;
        end if;
      
        if vNodo != 0 then
        
          return vNodo;
        end if;
      
      exception
        when others then
          continue;
      end;
    
    end loop;
  
    return vNodo;
  
  end;

  function PTC_BuscarOwnerIndicador(elnode in VARCHAR) return number is
  
    vNodo NUMBER := 0;
    vFID  NUMBER := 0;
  
    cursor ConductoresConectados(cNodo VARCHAR2) is
      select code, decode(elnode1, cNodo, 1, 2) pos
        from x$conductor_secundario
       where (elnode1 = cNodo or elnode2 = cNodo)
      union
      select code, decode(elnode1, cNodo, 1, 2) pos
        from x$conductor_transmision
       where (elnode1 = cNodo or elnode2 = cNodo);
  
  begin
  
    for conductor in ConductoresConectados(elnode) loop
    
      begin
        select g3e_fid
          into vFID
          from etl_code2fid
         where code = conductor.code;
      
        select g3e_id into vNodo from T$CPERTENENCIA where g3e_fid = vFID;
      
        if vNodo != 0 then
        
          return vNodo;
        end if;
      
      exception
        when others then
          continue;
      end;
    
    end loop;
  
    return vNodo;
  
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
  
    delete from etl_transformacion_report
     where circuito = pCircuito
       and elemento = pTabla;
  
    select extract(second from vDelta) + extract(minute from vDelta) * 60
      into vTiempo
      from dual;
  
    select count(1)
      into vReg
      from etl_transformacion_log
     where circuito = pCircuito
       and tabla = pTabla
       and preoredad = 1;
  
    dbms_output.put_line('Tabla: ' || pTabla || ' - Circuito: ' ||
                         pCircuito || ' - Reg:' || vReg);
  
    insert into etl_transformacion_report
    values
      (pCircuito, pTabla, vTiempo, pRegistros, vReg, sysdate);
    commit;
  
  end;

  procedure PoblarNodosEletricos is
  
  begin
    insert into CHEC_NODOS_ELEC
      select null, conn.code, conn.phnode, conn.fparent, 0
        from x$conectividad conn
       where not exists
       (select code from CHEC_NODOS_ELEC where code = conn.code);
    commit;
  end;

end ETL_TRANSFORMACION_CHEC;
/



--***************************************************************************************
--ETL_CARGA
--***************************************************************************************
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
  procedure ControlIndexes(pDisable in number);
  procedure CrearIndexesFIDFNO;
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

  procedure ControlIndexes(pDisable in number) is
  
  begin
  
    if pDisable = 1 then
      delete from etl_indexes;
      commit;
    
      for tabla in (select distinct table_name
                      from all_all_tables
                     where owner = 'GENERGIA'
                       and table_name like 'T$%') loop
      
        for idx in (select index_name, table_name
                      from all_indexes
                     where table_name =
                           replace(tabla.table_name, 'T$', 'B$')) loop
        
          insert into etl_indexes
          values
            (idx.index_name,
             idx.table_name,
             to_char(dbms_metadata.get_ddl('INDEX', idx.index_name)));
          commit;
        
          execute immediate 'drop index ' || idx.index_name;
        
        end loop;
      
      end loop;
    else
      for idx in (select * from etl_indexes) loop
        begin
          execute immediate idx.index_ddl;
        exception
          when others then
            dbms_output.put_line('Error on creating index: ' ||
                                 idx.index_name);
        end;
      end loop;
    end if;
  
  end;

  procedure CrearIndexesFIDFNO is
  
  begin
  
    for c in (select 'create index IDX_' || table_name || 'FID_FNO ON ' ||
                     table_name || '(G3E_FID, G3E_FNO)' as cmd
                from etl_indexes
               group by table_name) loop
    
      begin
        execute immediate c.cmd;
      exception
        when others then
          null;
      end;
    
    end loop;
  
  end;

end ETL_CARGA_CHEC;
/
