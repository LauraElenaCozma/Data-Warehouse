CREATE TABLE REZERVARE 
     (nr_pasageri NUMBER(2) CONSTRAINT ck_rezervare_nr_pasageri CHECK (nr_pasageri between 1 and 10),
     nr_pasageri_femei NUMBER(2) CONSTRAINT ck_rezervare_nr_pasageri_femei CHECK (nr_pasageri_femei between 0 and 10),
     nr_pasageri_barbati NUMBER(2) CONSTRAINT ck_rezervare_nr_pasageri_barbati CHECK (nr_pasageri_barbati between 0 and 10),
     suma_totala NUMBER(8) CONSTRAINT ck_rezervare_suma_totala CHECK (suma_totala IS NOT NULL),
     client_id NUMBER(8) CONSTRAINT ck_rezervare_client_id CHECK (client_id IS NOT NULL),
     data_rezervare_id TIMESTAMP,
     data_plecare_id TIMESTAMP,
     data_sosire_id TIMESTAMP, 
     locatie_plecare_id VARCHAR2(4), 
     locatie_sosire_id VARCHAR2(4),  
     operator_id VARCHAR2(3),
     zbor_id NUMBER(8),
     clasa_zbor_id NUMBER(2),
     metoda_plata_id NUMBER(2));
     
     
CREATE TABLE DESTINATIE 
     (destinatie_id VARCHAR2(4),
     oras VARCHAR2(60) CONSTRAINT ck_destinatie_oras CHECK(oras IS NOT NULL),
     stat VARCHAR2(5) CONSTRAINT ck_destinatie_stat CHECK(stat IS NOT NULL));
     
CREATE TABLE ZBOR
    (zbor_id NUMBER(8),
     aeronava_id VARCHAR2(7),
     durata NUMBER(4) CONSTRAINT ck_zbor_durata CHECK(durata IS NOT NULL),
     distanta NUMBER(4) CONSTRAINT ck_zbor_distanta CHECK(distanta IS NOT NULL),
     total_locuri NUMBER(4) CONSTRAINT ck_zbor_total_locuri CHECK(total_locuri IS NOT NULL), 
     anulat NUMBER(1) CONSTRAINT ck_zbor_anulat CHECK(anulat IN (0, 1)));
     
CREATE TABLE OPERATOR_ZBOR
    (operator_id VARCHAR2(3),
     nume VARCHAR2(50) CONSTRAINT ck_operator_nume CHECK(nume IS NOT NULL));
     
CREATE TABLE METODA_PLATA
    (metoda_plata_id NUMBER(2),
     denumire VARCHAR2(30) CONSTRAINT ck_metoda_plata_denumire CHECK(denumire IS NOT NULL));
    
CREATE TABLE CLASA_ZBOR
    (clasa_zbor_id NUMBER(2),
     denumire VARCHAR2(20) CONSTRAINT ck_clasa_denumire CHECK(denumire IS NOT NULL));

CREATE TABLE TIMP(
     timp_id TIMESTAMP,
     zi_an NUMBER(3) CONSTRAINT ck_timp_zi_an CHECK(zi_an IS NOT NULL),
     zi_luna NUMBER(2) CONSTRAINT ck_timp_zi_luna CHECK(zi_luna IS NOT NULL),
     zi_saptamana NUMBER(1) CONSTRAINT ck_timp_zi_saptamana CHECK(zi_saptamana IS NOT NULL),
     luna NUMBER(2) CONSTRAINT ck_timp_luna CHECK(luna IS NOT NULL),
     an NUMBER(4) CONSTRAINT ck_timp_an CHECK(an IS NOT NULL),
     weekend NUMBER(1) CONSTRAINT ck_timp_weekend CHECK(weekend IN (0, 1)),
     ora NUMBER(2) CONSTRAINT ck_timp_ora CHECK(ora IS NOT NULL),
     minut NUMBER(2) CONSTRAINT ck_timp_minut CHECK(minut IS NOT NULL));
     
     
--- mutarea datelor

CREATE OR REPLACE PROCEDURE creeaza_tabela_timp AS
  l_current_date TIMESTAMP;
  l_end_date TIMESTAMP;
BEGIN
  l_current_date := to_timestamp('2015-01-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss');
  l_end_date := to_timestamp('2015-12-31 23:59:00', 'YYYY-MM-DD hh24:mi:ss');
  WHILE l_current_date <= l_end_date LOOP
    INSERT INTO warehouse_admin.timp(timp_id, zi_an, zi_luna, zi_saptamana, luna, an, weekend, ora, minut)
    VALUES
      (l_current_date, 
      TO_CHAR(l_current_date, 'DDD'),
      TO_CHAR(l_current_date, 'DD'),
      TO_CHAR(l_current_date, 'D'),
      TO_CHAR(l_current_date, 'MM'),
      TO_CHAR(l_current_date, 'YYYY'),
      CASE WHEN(TO_CHAR(l_current_date, 'D') BETWEEN 2 AND 6) THEN 0 ELSE 1 END,
      TO_CHAR(l_current_date, 'hh24'),
      TO_CHAR(l_current_date, 'mi'));
      
    l_current_date := l_current_date + interval '1' minute;
  END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE etl AS
BEGIN
    INSERT INTO warehouse_admin.ZBOR SELECT zbor_id, aeronava_id, durata, distanta, total_locuri, anulat FROM zbor;
    INSERT INTO warehouse_admin.REZERVARE 
        (SELECT nr_pasageri, nr_pasageri_femei, nr_pasageri_barbati, suma_totala, client_id, data_rezervare, 
        data_plecare, data_sosire, locatie_plecare_id, locatie_sosire_id, operator_id, a.zbor_id, clasa_zbor_id, metoda_plata_id
         FROM rezervare a
         JOIN zbor b ON (a.zbor_id = b.zbor_id));
    INSERT INTO warehouse_admin.OPERATOR_ZBOR SELECT * FROM oltp_admin.OPERATOR_ZBOR;
    INSERT INTO warehouse_admin.METODA_PLATA SELECT * FROM METODA_PLATA;
    INSERT INTO warehouse_admin.CLASA_ZBOR SELECT * FROM CLASA_ZBOR;
    INSERT INTO warehouse_admin.DESTINATIE SELECT * FROM DESTINATIE;
    creeaza_tabela_timp;
    COMMIT;
END;
/


CREATE OR REPLACE TRIGGER insert_plata_warehouse AFTER INSERT ON metoda_plata FOR EACH ROW
BEGIN
  INSERT INTO warehouse_admin.metoda_plata VALUES (:NEW.metoda_plata_id, :NEW.denumire);
END;
/

CREATE OR REPLACE TRIGGER insert_destinatie_warehouse AFTER INSERT ON destinatie FOR EACH ROW
BEGIN
  INSERT INTO warehouse_admin.destinatie VALUES (:NEW.destinatie_id, :NEW.oras, :NEW.stat);
END;
/

CREATE OR REPLACE TRIGGER insert_clasa_warehouse AFTER INSERT ON clasa_zbor FOR EACH ROW
BEGIN
  INSERT INTO warehouse_admin.clasa_zbor VALUES (:NEW.clasa_zbor_id, :NEW.denumire);
END;
/

CREATE OR REPLACE TRIGGER insert_operator_warehouse AFTER INSERT ON operator_zbor FOR EACH ROW
BEGIN
  INSERT INTO warehouse_admin.operator_zbor VALUES (:NEW.operator_id, :NEW.nume);
END;
/

-- Procedura care insereaza in tabela timp in cazul in care inregistrarea nu exista deja
CREATE OR REPLACE PROCEDURE proc_add_into_timp(p_data DATE) AS 
    v_zi_an NUMBER;
    v_zi_luna NUMBER;
    v_zi_saptamana NUMBER;
    v_luna NUMBER;
    v_an NUMBER;
    v_weekend NUMBER;
    v_ora NUMBER;
    v_minut NUMBER;
    v_count NUMBER;
BEGIN

-- Extrage fiecare camp din data
    SELECT TO_CHAR(p_data, 'DDD') INTO v_zi_an
    FROM DUAL;
    
    SELECT TO_CHAR(p_data, 'DD') INTO v_zi_luna
    FROM DUAL;
    
    SELECT TO_CHAR(p_data, 'D') INTO v_zi_saptamana
    FROM DUAL;
    
    SELECT EXTRACT(MONTH FROM p_data) INTO v_luna
    FROM DUAL;
    
    SELECT EXTRACT(YEAR FROM p_data) INTO v_an
    FROM DUAL;
    
    IF v_zi_saptamana BETWEEN 6 AND 7 THEN
        v_weekend := 1;
    ELSE
        v_weekend := 0;
    END IF;
    
    SELECT TO_CHAR(p_data, 'hh24') INTO v_ora
    FROM DUAL;
    
    SELECT TO_CHAR(p_data, 'mi') INTO v_minut
    FROM DUAL;
    
-- Se verfica daca aceasta data exista deja, iar in caz contrar este inserata in tabela Timp 
    SELECT COUNT(*) INTO v_count FROM warehouse_admin.timp t 
    WHERE t.an = v_an 
    AND t.luna = v_luna 
    AND t.zi_luna = v_zi_luna
    AND t.ora = v_ora
    AND t.minut = v_minut;
    
    IF v_count = 0 THEN
        INSERT INTO warehouse_admin.timp VALUES(p_data, v_zi_an, v_zi_luna, v_zi_saptamana, v_luna, v_an, v_weekend, v_ora, v_minut);
    END IF;
    
END;
/

CREATE OR REPLACE TRIGGER insert_rezervare_warehouse AFTER INSERT ON rezervare FOR EACH ROW
DECLARE
    data_plecare_id warehouse_admin.rezervare.data_plecare_id%type;
    data_sosire_id warehouse_admin.rezervare.data_sosire_id%type;
    locatie_plecare_id warehouse_admin.rezervare.locatie_plecare_id%type;
    locatie_sosire_id warehouse_admin.rezervare.locatie_sosire_id%type;
    operator_id warehouse_admin.rezervare.operator_id%type;
BEGIN    
    select data_plecare
    into data_plecare_id
    from zbor z
    where z.zbor_id = :NEW.zbor_id;
    
    select data_sosire
    into data_sosire_id
    from zbor z
    where z.zbor_id = :NEW.zbor_id;
    
    select locatie_plecare_id
    into locatie_plecare_id
    from zbor z
    where z.zbor_id = :NEW.zbor_id;
    
    select locatie_sosire_id
    into locatie_sosire_id
    from zbor z
    where z.zbor_id = :NEW.zbor_id;
    
    select operator_id
    into operator_id
    from zbor z
    where z.zbor_id = :NEW.zbor_id;
    
-- se adauga datele in tabela Timp daca acestea nu exista deja
  proc_add_into_timp(:NEW.data_rezervare);
  proc_add_into_timp(data_sosire_id);
  proc_add_into_timp(data_plecare_id);
    
  INSERT INTO warehouse_admin.rezervare VALUES (:NEW.nr_pasageri, :NEW.nr_pasageri_femei, :NEW.nr_pasageri_barbati,
  :NEW.suma_totala, :NEW.client_id, :NEW.data_rezervare, data_plecare_id, data_sosire_id, locatie_plecare_id, locatie_sosire_id,
  operator_id, :NEW.zbor_id, :NEW.clasa_zbor_id, :NEW.metoda_plata_id);
END;
/

CREATE OR REPLACE TRIGGER insert_zbor_warehouse AFTER INSERT ON zbor FOR EACH ROW
BEGIN
  INSERT INTO warehouse_admin.zbor VALUES (:NEW.zbor_id, :NEW.aeronava_id, :NEW.durata, :NEW.distanta, :NEW.total_locuri, :NEW.anulat);
END;
/

SELECT * FROM rezervare where client_id = 1 
AND zbor_id = 2
AND clasa_zbor_id = 1
AND suma_totala = 6000
AND metoda_plata_id = 1;


--- constrangeri

-- pk rezervare
ALTER TABLE rezervare
ADD CONSTRAINT pk_rezervare
PRIMARY KEY(client_id, data_rezervare_id, data_plecare_id, data_sosire_id, locatie_plecare_id, locatie_sosire_id, operator_id, zbor_id, clasa_zbor_id, metoda_plata_id)
DISABLE VALIDATE;

-- fk pk pe destinatii
ALTER TABLE destinatie
ADD CONSTRAINT pk_destinatie
PRIMARY KEY(destinatie_id)
ENABLE VALIDATE;

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_locatie_plecare
FOREIGN KEY(locatie_plecare_id)
REFERENCES DESTINATIE(destinatie_id)
ENABLE NOVALIDATE;

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_locatie_sosire
FOREIGN KEY(locatie_sosire_id)
REFERENCES DESTINATIE(destinatie_id)
ENABLE NOVALIDATE;

-- fk pk pe timp
ALTER TABLE timp
ADD CONSTRAINT pk_timp
PRIMARY KEY(timp_id)
RELY DISABLE NOVALIDATE;

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_data_plecare
FOREIGN KEY(data_plecare_id)
REFERENCES TIMP(timp_id)
RELY DISABLE NOVALIDATE;

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_data_sosire
FOREIGN KEY(data_sosire_id)
REFERENCES TIMP(timp_id)
RELY DISABLE NOVALIDATE;

-- FK pentru data_rezervare
ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_data_rezervare
FOREIGN KEY(data_rezervare_id)
REFERENCES TIMP(timp_id)
RELY DISABLE NOVALIDATE;

-- pk fk oeprator zbor
ALTER TABLE operator_zbor
ADD CONSTRAINT pk_operator_zbor
PRIMARY KEY(operator_id);

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_operator_id
FOREIGN KEY(operator_id)
REFERENCES OPERATOR_ZBOR(operator_id)
ENABLE NOVALIDATE;

-- pk fk zbor
ALTER TABLE zbor
ADD CONSTRAINT pk_zbor
PRIMARY KEY(zbor_id);

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_zbor_id
FOREIGN KEY(zbor_id)
REFERENCES ZBOR(zbor_id)
ENABLE NOVALIDATE;

-- pk fk clasa zbor
ALTER TABLE clasa_zbor
ADD CONSTRAINT pk_clasa_zbor
PRIMARY KEY(clasa_zbor_id);

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_clasa_id
FOREIGN KEY(clasa_zbor_id)
REFERENCES CLASA_ZBOR(clasa_zbor_id)
ENABLE NOVALIDATE;

-- pk fk metoda_plata
ALTER TABLE metoda_plata
ADD CONSTRAINT pk_metoda_plata
PRIMARY KEY(metoda_plata_id);

ALTER TABLE rezervare
ADD CONSTRAINT fk_rezervare_metoda_plata
FOREIGN KEY(metoda_plata_id)
REFERENCES METODA_PLATA(metoda_plata_id)
ENABLE NOVALIDATE;

SELECT a.table_name, a.column_name, b.constraint_name, generated, b.constraint_type, search_condition, 
delete_rule, r_constraint_name, status, validated, rely
FROM user_cons_columns a, user_constraints b
WHERE a.constraint_name = b.constraint_name
AND a.table_name IN ('REZERVARE', 'DESTINATIE', 'CLASA_ZBOR', 'OPERATOR_ZBOR', 'METODA_PLATA', 'ZBOR', 'TIMP')
AND b.constraint_name NOT LIKE 'CK%';


--- indecsi
SELECT COUNT(*)
FROM rezervare r
JOIN zbor z ON r.zbor_id = z.zbor_id
WHERE z.anulat = 1  AND r.metoda_plata_id = 2;

CREATE BITMAP INDEX idx_rezervare_metoda_plata 
ON rezervare(metoda_plata_id);

CREATE BITMAP INDEX idx_join_zbor_rezervare ON rezervare(z.anulat)
FROM rezervare r, zbor z
WHERE r.zbor_id = z.zbor_id;

ANALYZE INDEX idx_join_zbor_rezervare COMPUTE STATISTICS;
ANALYZE INDEX idx_rezervare_metoda_plata COMPUTE STATISTICS;

EXPLAIN PLAN
SET STATEMENT_ID = 's6' FOR 
SELECT COUNT(*) /*+INDEX(r idx_join_zbor_rezervare) +INDEX(r idx_rezervare_metoda_plata)*/
FROM rezervare r
JOIN zbor z ON r.zbor_id = z.zbor_id
WHERE z.anulat = 1  AND r.metoda_plata_id = 2;

SELECT plan_table_output
FROM table(dbms_xplan.display('plan_table', 's6','serial'));

SELECT COUNT(*)
FROM rezervare r
JOIN zbor z ON r.zbor_id = z.zbor_id
WHERE z.anulat = 1  AND r.metoda_plata_id = 2;

--- dimensiuni

create dimension timp_dim
level zi is (timp.timp_id)
level luna is (timp.luna)
level an is (timp.an)
hierarchy h
  (zi child of
   luna child of
   an)
ATTRIBUTE zi DETERMINES
            (TIMP.zi_saptamana,
             TIMP.zi_luna,
             TIMP.zi_an);

set serveroutput on;
EXECUTE DEMO_DIM.PRINT_DIM ('timp_dim');
EXECUTE DBMS_OUTPUT.ENABLE(10000);
EXECUTE DEMO_DIM.PRINT_ALLDIMS;

select dimension_name, invalid, compile_state
from   user_dimensions;

EXECUTE DBMS_DIMENSION.VALIDATE_DIMENSION(UPPER('timp_dim'),FALSE,TRUE,'st_id2');

SELECT count(*)
  FROM   timp
  WHERE  ROWID IN (SELECT BAD_ROWID
                   FROM DIMENSION_EXCEPTIONS
                   WHERE STATEMENT_ID = 'st_id2');
                   
-- partitii
SELECT COUNT(data_rezervare_id) FROM rezervare
WHERE operator_id = 'HA'
AND data_plecare_id BETWEEN TO_DATE('2015-01-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-01-31 00:00:00', 'YYYY-MM-DD hh24:mi:ss');
       
CREATE TABLE rezervare_ord_data_plecare
PARTITION BY RANGE(data_plecare_id)
( PARTITION rezervari_jan2015
VALUES LESS THAN(TO_TIMESTAMP('2015-02-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')),
PARTITION rezervari_feb2015
VALUES LESS THAN(TO_DATE('2015-03-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')),
PARTITION rezervari_mar2015
VALUES LESS THAN (MAXVALUE))
AS
SELECT *
FROM rezervare;

ANALYZE TABLE rezervare_ord_data_plecare COMPUTE STATISTICS;

EXPLAIN PLAN
SET STATEMENT_ID = 'st_rezervare_data_1'
FOR 
SELECT COUNT(*)
FROM   rezervare_ord_data_plecare
WHERE  data_plecare_id BETWEEN TO_DATE('2015-01-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-01-31 00:00:00', 'YYYY-MM-DD hh24:mi:ss');

SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_rezervare_data_1','serial'));

EXPLAIN PLAN
SET STATEMENT_ID = 'st_rezervare_data_2'
FOR 
SELECT COUNT(*)
FROM   rezervare_ord_data_plecare
WHERE  data_plecare_id BETWEEN TO_DATE('2015-01-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-01-31 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
AND OPERATOR_ID = 'HA';

SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_rezervare_data_2','serial'));

-- partitionare list
CREATE TABLE rezervare_lista_operator
    PARTITION BY LIST(operator_id)(
    PARTITION rezervari_ua VALUES('UA'),
    PARTITION rezervari_aa VALUES('AA'),
    PARTITION rezervari_us VALUES('US'),
    PARTITION rezervari_f9 VALUES('F9'),
    PARTITION rezervari_b6 VALUES('B6'),
    PARTITION rezervari_oo VALUES('OO'),
    PARTITION rezervari_as VALUES('AS'),
    PARTITION rezervari_nk VALUES('NK'),
    PARTITION rezervari_wn VALUES('WN'),
    PARTITION rezervari_dl VALUES('DL'),
    PARTITION rezervari_ev VALUES('EV'),
    PARTITION rezervari_ha VALUES('HA'),
    PARTITION rezervari_mq VALUES('MQ'),
    PARTITION rezervari_vx VALUES('VX'),
    PARTITION rezervari_rest VALUES(DEFAULT))
    AS
    SELECT *
    FROM rezervare;

ANALYZE TABLE rezervare_lista_operator COMPUTE STATISTICS;

EXPLAIN PLAN
SET STATEMENT_ID = 'st_rezervare_lista_1'
FOR 
SELECT COUNT(*)
FROM   rezervare_lista_operator
WHERE operator_id = 'HA';

SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_rezervare_lista_1','serial'));

EXPLAIN PLAN
SET STATEMENT_ID = 'st_rezervare_lista_2'
FOR 
SELECT COUNT(*)
FROM   rezervare_lista_operator
WHERE operator_id = 'HA'
AND  data_plecare_id BETWEEN TO_DATE('2015-01-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-01-31 00:00:00', 'YYYY-MM-DD hh24:mi:ss');

SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_rezervare_lista_2','serial'));

-- partitionare compusa
CREATE TABLE rezervari_lunar_operator
  PARTITION BY RANGE (data_plecare_id)
    SUBPARTITION BY LIST (operator_id)
      (PARTITION rezervari_jan2015 VALUES LESS THAN (TO_TIMESTAMP('2015-02-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss'))
        (SUBPARTITION rezervari_jan2015_ua VALUES('UA'),
        SUBPARTITION rezervari_jan2015_aa VALUES('AA'),
        SUBPARTITION rezervari_jan2015_us VALUES('US'),
        SUBPARTITION rezervari_jan2015_f9 VALUES('F9'),
        SUBPARTITION rezervari_jan2015_b6 VALUES('B6'),
        SUBPARTITION rezervari_jan2015_oo VALUES('OO'),
        SUBPARTITION rezervari_jan2015_as VALUES('AS'),
        SUBPARTITION rezervari_jan2015_nk VALUES('NK'),
        SUBPARTITION rezervari_jan2015_wn VALUES('WN'),
        SUBPARTITION rezervari_jan2015_dl VALUES('DL'),
        SUBPARTITION rezervari_jan2015_ev VALUES('EV'),
        SUBPARTITION rezervari_jan2015_ha VALUES('HA'),
        SUBPARTITION rezervari_jan2015_mq VALUES('MQ'),
        SUBPARTITION rezervari_jan2015_vx VALUES('VX'),
        SUBPARTITION rezervari_jan2015_rest VALUES(DEFAULT)),
    PARTITION rezervari_feb2015 VALUES LESS THAN (TO_TIMESTAMP('2015-03-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss'))
        (SUBPARTITION rezervari_feb2015_ua VALUES('UA'),
        SUBPARTITION rezervari_feb2015_aa VALUES('AA'),
        SUBPARTITION rezervari_feb2015_us VALUES('US'),
        SUBPARTITION rezervari_feb2015_f9 VALUES('F9'),
        SUBPARTITION rezervari_feb2015_b6 VALUES('B6'),
        SUBPARTITION rezervari_feb2015_oo VALUES('OO'),
        SUBPARTITION rezervari_feb2015_as VALUES('AS'),
        SUBPARTITION rezervari_feb2015_nk VALUES('NK'),
        SUBPARTITION rezervari_feb2015_wn VALUES('WN'),
        SUBPARTITION rezervari_feb2015_dl VALUES('DL'),
        SUBPARTITION rezervari_feb2015_ev VALUES('EV'),
        SUBPARTITION rezervari_feb2015_ha VALUES('HA'),
        SUBPARTITION rezervari_feb2015_mq VALUES('MQ'),
        SUBPARTITION rezervari_feb2015_vx VALUES('VX'),
        SUBPARTITION rezervari_feb2015_rest VALUES(DEFAULT)),
    PARTITION rezervari_mar2015 VALUES LESS THAN (MAXVALUE)
        (SUBPARTITION rezervari_mar2015_ua VALUES('UA'),
        SUBPARTITION rezervari_mar2015_aa VALUES('AA'),
        SUBPARTITION rezervari_mar2015_us VALUES('US'),
        SUBPARTITION rezervari_mar2015_f9 VALUES('F9'),
        SUBPARTITION rezervari_mar2015_b6 VALUES('B6'),
        SUBPARTITION rezervari_mar2015_oo VALUES('OO'),
        SUBPARTITION rezervari_mar2015_as VALUES('AS'),
        SUBPARTITION rezervari_mar2015_nk VALUES('NK'),
        SUBPARTITION rezervari_mar2015_wn VALUES('WN'),
        SUBPARTITION rezervari_mar2015_dl VALUES('DL'),
        SUBPARTITION rezervari_mar2015_ev VALUES('EV'),
        SUBPARTITION rezervari_mar2015_ha VALUES('HA'),
        SUBPARTITION rezervari_mar2015_mq VALUES('MQ'),
        SUBPARTITION rezervari_mar2015_vx VALUES('VX'),
        SUBPARTITION rezervari_mar2015_rest VALUES(DEFAULT)))
    AS SELECT * FROM rezervare;
    
ANALYZE TABLE rezervari_lunar_operator COMPUTE STATISTICS;

EXPLAIN PLAN
SET STATEMENT_ID = 'st_rezervare_lunar_operator_1'
FOR 
SELECT COUNT(*)
FROM   rezervari_lunar_operator
WHERE operator_id = 'HA'
AND  data_plecare_id BETWEEN TO_DATE('2015-01-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-01-31 00:00:00', 'YYYY-MM-DD hh24:mi:ss');

SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_rezervare_lunar_operator_1','serial'));

EXPLAIN PLAN
SET STATEMENT_ID = 'st_rezervare_lunar_operator_2'
FOR 
SELECT COUNT(*)
FROM   rezervari_lunar_operator SUBPARTITION (rezervari_jan2015_ha);
-- cost

SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_rezervare_lunar_operator_2','serial'));

---- cerere sql complexa
SELECT operator_id, c.denumire, SUM(suma_totala) "Suma de restituit", SUM(nr_pasageri) "Nr pasageri afectati"
FROM rezervare r
JOIN zbor z ON (r.zbor_id = z.zbor_id)
JOIN clasa_zbor c ON (c.clasa_zbor_id = r.clasa_zbor_id)
WHERE  data_plecare_id BETWEEN TO_DATE('2015-02-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-02-28 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
AND z.anulat = 1
GROUP BY GROUPING SETS ((operator_id, c.denumire),(operator_id),())
ORDER BY operator_id, c.denumire;

EXPLAIN PLAN
SET STATEMENT_ID = 'st_cerere_complexa_1'
FOR 
SELECT operator_id, c.denumire, SUM(suma_totala) "Suma de restituit", SUM(nr_pasageri) "Nr pasageri afectati"
FROM rezervare_ord_data_plecare r
JOIN zbor z ON (r.zbor_id = z.zbor_id)
JOIN clasa_zbor c ON (c.clasa_zbor_id = r.clasa_zbor_id)
WHERE  data_plecare_id BETWEEN TO_DATE('2015-02-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-02-28 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
AND z.anulat = 1
GROUP BY GROUPING SETS ((operator_id, c.denumire),(operator_id),())
ORDER BY operator_id, c.denumire;


SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_cerere_complexa_1','serial'));


CREATE BITMAP INDEX idx_zbor_anulat ON zbor(anulat);
ANALYZE INDEX idx_zbor_anulat compute statistics;

EXPLAIN PLAN
SET STATEMENT_ID = 'st_cerere_complexa_2'
FOR 
SELECT operator_id, c.denumire, SUM(suma_totala) "Suma de restituit", SUM(nr_pasageri) "Nr pasageri afectati"
FROM rezervare_ord_data_plecare r
JOIN zbor z ON (r.zbor_id = z.zbor_id)
JOIN clasa_zbor c ON (c.clasa_zbor_id = r.clasa_zbor_id)
WHERE  data_plecare_id BETWEEN TO_DATE('2015-02-01 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
       AND TO_DATE('2015-02-28 00:00:00', 'YYYY-MM-DD hh24:mi:ss')
AND z.anulat = 1
GROUP BY GROUPING SETS ((operator_id, c.denumire),(operator_id),())
ORDER BY operator_id, c.denumire;


SELECT plan_table_output
FROM   table(dbms_xplan.display('plan_table','st_cerere_complexa_2','serial'));


-- cele 5 cereri sql

---1

SELECT operator_id, TO_CHAR(data_rezervare_id, 'DD-MM-YYYY') data_rezervare, 
SUM(suma_totala) incasari,
ROUND(AVG(SUM(suma_totala))
    OVER (PARTITION BY operator_id ORDER BY data_rezervare_id
        RANGE BETWEEN INTERVAL '3' DAY PRECEDING
        AND INTERVAL '3' DAY FOLLOWING), 2) AS medie_centrata_7_zile
FROM   rezervare r, timp t
WHERE  r.data_rezervare_id =t.timp_id
AND    an = 2015
AND    luna = 2
AND operator_id = 'UA'
GROUP BY operator_id, data_rezervare_id;

-- 2
SELECT 'Pasageri pe zi', data_plecare_id, nr_pasageri_total,
COALESCE(nr_pasageri_total - LAG(nr_pasageri_total, 1) OVER (ORDER BY data_plecare_id), 0) AS diferenta_pasageri_total
FROM (SELECT data_plecare_id, 
        SUM(nr_pasageri) AS nr_pasageri_total
            FROM   rezervare r, timp t
            WHERE  r.data_plecare_id=t.timp_id
            AND zi_luna <= 15
            AND    luna= 1
            AND    an = 2015
            GROUP BY data_plecare_id);
            
-- 3
SELECT o.nume, 
MIN(r.data_rezervare_id) KEEP (DENSE_RANK FIRST ORDER BY z.distanta) "Data minima cea mai scurta distanta",
MIN(r.data_rezervare_id) KEEP (DENSE_RANK LAST ORDER BY z.distanta) "Data minima cea mai lunga distanta",
MIN(z.distanta) "DISTANTA MINIMA",
MAX(r.data_rezervare_id) KEEP (DENSE_RANK FIRST ORDER BY z.distanta) "Data maxima cea mai scurta distanta",
MAX(r.data_rezervare_id) KEEP (DENSE_RANK LAST ORDER BY z.distanta) "Data maxima cea mai lunga distanta",
MAX(z.distanta) "DISTANTA MAXIMA"
FROM zbor z
JOIN rezervare r on z.zbor_id = r.zbor_id
JOIN operator_zbor o on o.operator_id = r.operator_id
GROUP BY o.nume;

-- 4
SELECT nume AS operator, oras, valoare
FROM(SELECT nume, oras,
            SUM(suma_totala) AS valoare,
            MAX(SUM(suma_totala))
              OVER (PARTITION BY nume) AS max_val
     FROM   rezervare r, operator_zbor o, destinatie d
     WHERE  r.locatie_sosire_id=d.destinatie_id
     AND    r.operator_id=o.operator_id
     GROUP BY nume, oras)
WHERE valoare= max_val;

-- 5
SELECT denumire,
      SUM(suma_totala)
          VANZARI,
      SUM(SUM(suma_totala)) 
          OVER () 
          AS TOTAL_VANZ,
      round(RATIO_TO_REPORT(
          SUM(suma_totala)) 
          OVER (), 6)
           AS RATIO_REP
FROM  metoda_plata m, rezervare r
WHERE  r.metoda_plata_id=m.metoda_plata_id
GROUP BY denumire;

-- 6
SELECT client_id, luna, SUM(r.suma_totala) SUMA, SUM(r.nr_pasageri) "Nr pasageri in luna X",
SUM(SUM(r.nr_pasageri)) OVER(PARTITION BY client_id ORDER BY client_id, luna ROWS UNBOUNDED PRECEDING) "Nr pasageri pana in luna X"
FROM rezervare r, timp t
WHERE r.data_rezervare_id = t.timp_id AND nr_pasageri > 7 AND nr_pasageri_femei > 5 AND client_id IN (1,2,3)
GROUP BY client_id, luna;