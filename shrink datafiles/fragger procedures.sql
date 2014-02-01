CREATE OR REPLACE PROCEDURE FRAGMENT_TS(TABLESPACE_NAME IN VARCHAR2, segment_size_m IN NUMBER DEFAULT 10, fragmentation_type IN VARCHAR2 DEFAULT 'BOTH') AS
BEGIN
	FILLUP_TS(TABLESPACE_NAME);
	HOLLOWOUT_TS(TABLESPACE_NAME, fragmentation_type);
END;
/


CREATE OR REPLACE PROCEDURE FILLUP_TS(TABLESPACE_NAME IN VARCHAR2, segment_size_m IN NUMBER DEFAULT 10) AS
	i				NUMBER := 1;
	TS_NAME			VARCHAR(30);
	ts_free_k		NUMBER;
	tab_size_k		NUMBER;
	ddl				VARCHAR2(4000);
	dml				VARCHAR2(4000);
	e_tsfull		exception;
	e_tableexists	exception;
	e_tempfull		exception;
	e_cant_create_table exception;
	PRAGMA EXCEPTION_INIT(e_tempfull, -01652);
	PRAGMA EXCEPTION_INIT(e_tsfull, -01653);
	PRAGMA EXCEPTION_INIT(e_cant_create_table, -01658);
	PRAGMA EXCEPTION_INIT(e_tableexists, -00955);
BEGIN
	TS_NAME := UPPER(TABLESPACE_NAME); 
	select NVL(sum(bytes),0)/1024 into ts_free_k from user_free_space where tablespace_name = ts_name;
	IF ts_free_k < 100 THEN
		DBMS_OUTPUT.PUT_LINE('Tablespace ' || ts_name || ' ALREADY filled to brim');
	ELSE
		WHILE ts_free_k > 100 LOOP
			BEGIN
				ddl := 'create table fragment_ts_' || i || ' tablespace ' || TS_NAME || ' as select * from user_objects';
				EXECUTE IMMEDIATE ddl;
				
				select NVL(sum(bytes)/1024,0) into tab_size_k from user_segments where segment_name = 'fragment_ts_' || i;
				--DBMS_OUTPUT.PUT_LINE('tab_size_k '|| tab_size_k);
				WHILE tab_size_k <= segment_size_m * 1024 LOOP
					dml := 'insert into fragment_ts_' || i || ' select * from fragment_ts_' || i ;
					EXECUTE IMMEDIATE dml;
					COMMIT;
					select NVL(sum(bytes)/1024,0) into tab_size_k from user_segments where segment_name = upper('fragment_ts_' || i);
				END LOOP;
			EXCEPTION
				WHEN e_tableexists THEN
					NULL;
			END;
			i := i + 1;
		END LOOP;
	
	END IF;
EXCEPTION
	WHEN e_tsfull THEN
		DBMS_OUTPUT.PUT_LINE('DONE: Tablespace ' || ts_name || ' filled to brim');
	WHEN e_tempfull THEN
		DBMS_OUTPUT.PUT_LINE('DONE: Tablespace ' || ts_name || ' ALMOST filled to brim.');
END;
/

--
-- exec HOLLOWOUT_TS('TEST','BOTH');
-- exec HOLLOWOUT_TS('TEST','TABLESPACE');
-- exec HOLLOWOUT_TS('TEST','SEGMENT');

CREATE OR REPLACE PROCEDURE HOLLOWOUT_TS(tablespace_name IN VARCHAR2, fragmentation_type IN VARCHAR2 DEFAULT 'BOTH') AS
	x 		NUMBER;
	minx 	NUMBER;
	maxx 	NUMBER;
	TOGGLE	BOOLEAN:= FALSE;
	TS_NAME VARCHAR2(30);
	
	CURSOR v_seg_cur IS
		SELECT TABLE_NAME FROM DBA_TABLES WHERE TABLESPACE_NAME = TS_NAME AND TABLE_NAME LIKE 'FRAGMENT_TS_%';
BEGIN
	TS_NAME := UPPER(TABLESPACE_NAME);
	FOR segrow IN v_seg_cur LOOP
		IF fragmentation_type = 'TABLESPACE' OR fragmentation_type = 'BOTH' THEN
			IF TOGGLE THEN
				execute immediate 'drop table ' || segrow.TABLE_NAME || ' purge';
				TOGGLE := FALSE;
			ELSE
				TOGGLE := TRUE;
			END IF;
		ELSIF fragmentation_type = 'SEGMENT' OR fragmentation_type = 'BOTH' THEN
			IF TOGGLE THEN
				execute immediate 'select min(object_id),max(object_id) from ' || segrow.TABLE_NAME into minx,maxx;
				x := minx;
				WHILE x < maxx LOOP
					execute immediate 'DELETE FROM ' || segrow.TABLE_NAME || ' WHERE OBJECT_ID IN (SELECT OBJECT_ID FROM ' || segrow.TABLE_NAME || ' WHERE OBJECT_ID BETWEEN ' || X || ' AND ' || TO_CHAR(X + 1000) || ')';
					commit;
					x := x + 2000;
				END LOOP;
			ELSE
				TOGGLE := TRUE;
			END IF;
		END IF;
	END LOOP;
	DBMS_OUTPUT.PUT_LINE('DONE: Tablespace ' || ts_name || ' hollowed out');
END;
/


CREATE OR REPLACE PROCEDURE create_fragged_test_tbs(DATAFILE_COUNT IN NUMBER DEFAULT 2, DATAFILE_SIZE_M IN NUMBER DEFAULT 100, fragmentation_type IN VARCHAR2 DEFAULT 'BOTH') AS
	paddedi			NUMBER;
	toggle			BOOLEAN := TRUE;
	e_tbs_noexist	EXCEPTION;
	PRAGMA EXCEPTION_INIT(e_tbs_noexist, -00959);
BEGIN
	BEGIN
		EXECUTE IMMEDIATE 'DROP TABLESPACE TEST INCLUDING CONTENTS AND DATAFILES';
	EXCEPTION
		WHEN e_tbs_noexist THEN
			NULL;
	END;
	
	EXECUTE IMMEDIATE 'CREATE TABLESPACE TEST DATAFILE ''/db/oradata/ora11g/ora11g/test01.dbf'' size '|| DATAFILE_SIZE_M || 'm';

	FILLUP_TS('TEST');
	IF DATAFILE_COUNT < 2 THEN
		HOLLOWOUT_TS('TEST', fragmentation_type);
	END IF;
	FOR i IN 2..DATAFILE_COUNT LOOP
		select LPAD(i,2,0) INTO paddedi from dual;
		EXECUTE IMMEDIATE 'ALTER TABLESPACE TEST ADD DATAFILE ''/db/oradata/ora11g/ora11g/test0' || paddedi || '.dbf'' size '|| DATAFILE_SIZE_M || 'm';
		FILLUP_TS('TEST');
		IF toggle THEN
			HOLLOWOUT_TS('TEST', fragmentation_type);
			toggle := FALSE;
		ELSE
			toggle := TRUE;
		END IF;
	END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE test_fragger AS
BEGIN
	create_fragged_test_tbs(5,100,'TABLESPACE');
END;
/
