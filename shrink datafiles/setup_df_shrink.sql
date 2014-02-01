CREATE DIRECTORY TMP_DIR AS '/tmp';

CREATE OR REPLACE PROCEDURE ANALYZE_DATAFILE_SHRINKABILITY(	ANALYZE_OBJECT_TYPE IN VARCHAR2 DEFAULT 'TABLESPACE',
															ANALYZE_OBJECT_NAME IN VARCHAR2 DEFAULT 'ALL_TABLESPACES', 
															RESIZE_DATAFILE_TO IN VARCHAR2 DEFAULT NULL) AS
	v_analyze_obj_type	VARCHAR2(10) := UPPER(ANALYZE_OBJECT_TYPE);
	v_analyze_obj_name	VARCHAR2(1000) := ANALYZE_OBJECT_NAME;
	v_tablespace_name	VARCHAR2(1000);
	v_start_time		VARCHAR2(20);
	file_handle 		UTL_FILE.FILE_TYPE;
	v_todir				BOOLEAN := TRUE;
	v_dir_path			VARCHAR(1000);
	v_file_extn			VARCHAR(1000);
	
	v_dir_name			VARCHAR(1000)	:=	'TMP_DIR';
	v_file_name			VARCHAR(1000)	:=	'defrag';
	
	CURSOR cur_tablespaces IS
		SELECT TABLESPACE_NAME FROM DBA_TABLESPACES;
	CURSOR cur_datafiles IS
		SELECT FILE_NAME FROM DBA_DATA_FILES where tablespace_name = UPPER(v_tablespace_name);
BEGIN
	SELECT TO_CHAR(sysdate,'dd/mm/yyyy hh24:mi:ss') into v_start_time from dual;
	IF v_analyze_obj_type = 'TABLESPACE' THEN
		BEGIN
			SELECT DIRECTORY_PATH INTO v_dir_path FROM dba_directories WHERE DIRECTORY_NAME = v_dir_name;
			v_file_name := 'defrag_' || v_analyze_obj_name || '.sql';
			file_handle := UTL_FILE.FOPEN(v_dir_name,v_file_name,'W',32000);
			UTL_FILE.PUT_LINE(file_handle, '--Script Generated at ' || v_start_time);
			UTL_FILE.FCLOSE(file_handle);
		EXCEPTION WHEN OTHERS THEN
			v_todir := FALSE;
			v_file_name := NULL;
			DBMS_OUTPUT.PUT_LINE('-- Script Generated at ' || v_start_time);
			DBMS_OUTPUT.PUT_LINE('-- Run the following to be able to shrink the datafile(s) to minimum possible size.');
		END;
		IF v_analyze_obj_name = 'ALL_TABLESPACES' THEN
			FOR row_tablespaces IN cur_tablespaces LOOP
				v_tablespace_name := row_tablespaces.tablespace_name;
				FOR row_datafiles IN cur_datafiles LOOP
					ANALYZE_DATAFILE(row_datafiles.file_name, v_dir_name, v_file_name);
				END LOOP;
			END LOOP;
		ELSE
			v_tablespace_name := UPPER(v_analyze_obj_name);
			FOR row_datafiles IN cur_datafiles LOOP
				ANALYZE_DATAFILE(row_datafiles.file_name, v_dir_name, v_file_name, resize_datafile_to);
			END LOOP;
		END IF;
	ELSIF v_analyze_obj_type = 'DATAFILE' THEN
		BEGIN
			v_file_extn := SUBSTR(v_file_name, LENGTH(v_file_name)-3, 4);
			IF lower(v_file_extn) != '.sql' THEN
				v_file_name := v_file_name || '.sql';
			END IF;
			
			SELECT DIRECTORY_PATH INTO v_dir_path FROM dba_directories WHERE DIRECTORY_NAME = v_dir_name;
			file_handle := UTL_FILE.FOPEN(v_dir_name,v_file_name,'W',32000);
			UTL_FILE.PUT_LINE(file_handle, '--Script Generated at ' || v_start_time);
			UTL_FILE.FCLOSE(file_handle);
		EXCEPTION WHEN OTHERS THEN
			v_todir := FALSE;
			DBMS_OUTPUT.PUT_LINE('-- Script Generated at ' || v_start_time);
			DBMS_OUTPUT.PUT_LINE('-- Run the following to be able to shrink the datafile to minimum possible size.');
		END;
		ANALYZE_DATAFILE(v_analyze_obj_name, v_dir_name, v_file_name, resize_datafile_to);
	END IF;
	DBMS_OUTPUT.PUT_LINE('--');
	IF v_todir THEN
		DBMS_OUTPUT.PUT_LINE('Defragmentation script generated : ' || v_dir_path || '/' || v_file_name);
	ELSE
		DBMS_OUTPUT.PUT_LINE('Shrinkability Analysis ended successfully');
	END IF;

END;
/

CREATE OR REPLACE PROCEDURE ANALYZE_DATAFILE(
    DATAFILE_NAME IN VARCHAR2,
    OUTPUT_DIR    IN VARCHAR2 DEFAULT 'TMP_DIR',
    OUTPUT_FILE   IN VARCHAR2 DEFAULT 'defrag.sql',
	RESIZE_TO     IN VARCHAR2 DEFAULT NULL)
AS
  v_datafile_name VARCHAR(1000) := DATAFILE_NAME;
BEGIN
  DECLARE
    file_blocks NUMBER;
    v_file_id   NUMBER;
    file_name dba_data_files.file_name%type:=NULL;
    file_size         VARCHAR2(100);
    file_block_size   NUMBER :=0;
    yesno             BOOLEAN:=TRUE;
    invalid_file_id   EXCEPTION;
    invalid_file_size EXCEPTION;
    CURSOR c_blocksize (fno IN NUMBER)
    IS
      SELECT t.block_size,
        d.file_name
      FROM dba_tablespaces t,
        dba_data_files d
      WHERE d.file_id       =fno
      AND d.tablespace_name =t.tablespace_name;
    CURSOR c_segments (fno IN NUMBER, block IN NUMBER)
    IS
      SELECT DISTINCT owner,
        segment_name,
        partition_name,
        segment_type
      FROM dba_extents
      WHERE file_id =fno
      AND block_id >= block
      ORDER BY owner,
        segment_type,
        segment_name;
    CURSOR c_recyclebin (fno IN NUMBER, block IN NUMBER)
    IS
      SELECT username,
        original_name,
        partition_name
      FROM recyclebin$,
        dba_users
      WHERE file# =fno
      AND block# >= block
      AND owner#  =user_id;
    file_handle UTL_FILE.FILE_TYPE;
    v_recycle_header_printed BOOLEAN := FALSE;
    v_todir                  BOOLEAN := TRUE;
    e_nodir                  EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_nodir, -29280);
  BEGIN
    DBMS_OUTPUT.PUT_LINE('-- Analyzing datafile : ' || v_datafile_name);
    SELECT file_id
    INTO v_file_id
    FROM dba_data_files
    WHERE file_name = v_datafile_name;
	
    FOR c_check_blocksize IN c_blocksize (v_file_id)
	LOOP
	  file_block_size:=c_check_blocksize.block_size;
	  file_name      :=c_check_blocksize.file_name;
	END LOOP;
	
	IF resize_to IS NULL THEN
		-- ideal size of datafile
		SELECT ddf.bytes - ( SELECT NVL(SUM(bytes),0) FROM dba_free_space WHERE file_id = ddf.file_id )
		INTO file_size
		FROM dba_data_files ddf
		WHERE file_id = v_file_id;
		/* check if correct file is provided and what blocksize is of tablespace */
		IF file_name IS NULL THEN
		  raise invalid_file_id;
		END IF;
	ELSE
		file_size := resize_to;
	END IF;
	
	IF (length(file_size) != instr(upper(file_size),'G')) 
		AND (length(file_size) != instr(upper(file_size),'M'))
		AND (length(file_size) != instr(upper(file_size),'K'))
		AND (NOT yesno) then
			RAISE invalid_file_size;
	ELSE
		CASE UPPER(SUBSTR(file_size,length(file_size),1))
			when 'G' then
				file_blocks:=floor((substr(file_size,1,length(file_size)-1)*power(2,30))/file_block_size);
			when 'M' then
				file_blocks:=floor((substr(file_size,1,length(file_size)-1)*power(2,20))/file_block_size);
			when 'K' then
				file_blocks:=floor((substr(file_size,1,length(file_size)-1)*power(2,10))/file_block_size);
			else
				file_blocks:=floor(file_size/file_block_size); 
		END CASE;
	END IF;

	
    BEGIN
		-- Turn on for debugging
		-- DBMS_OUTPUT.PUT_LINE('file_size '|| file_size);
		-- DBMS_OUTPUT.PUT_LINE('file_blocks '|| file_blocks);
		
      file_handle := UTL_FILE.FOPEN(OUTPUT_DIR, OUTPUT_FILE,'A',32000);
      UTL_FILE.PUT_LINE(file_handle, '-- Analyzing datafile : ' || v_datafile_name);
      FOR c_seg IN c_segments(v_file_id,file_blocks) LOOP
        IF c_seg.segment_type LIKE '%UNDO%' THEN
          NULL;
        ELSIF c_seg.segment_type = 'TABLE' THEN
          UTL_FILE.PUT_LINE(file_handle, 'alter table "' || c_seg.owner ||'"."'||c_seg.segment_name||'" move;');
        ELSIF c_seg.segment_type = 'INDEX' THEN
          UTL_FILE.PUT_LINE(file_handle, 'alter index "' || c_seg.owner ||'"."'||c_seg.segment_name||'" rebuild;');
        ELSIF c_seg.segment_type = 'INDEX PARTITION' THEN
          UTL_FILE.PUT_LINE(file_handle, 'alter index "' || c_seg.owner ||'"."'||c_seg.segment_name||'" rebuild partition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type = 'TABLE PARTITION' THEN
          UTL_FILE.PUT_LINE(file_handle, 'alter table "' || c_seg.owner ||'"."'||c_seg.segment_name||'" move partition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type = 'INDEX SUBPARTITION' THEN
          UTL_FILE.PUT_LINE(file_handle, 'alter index "' || c_seg.owner ||'"."'||c_seg.segment_name||'" rebuild subpartition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type = 'TABLE SUBPARTITION' THEN
          UTL_FILE.PUT_LINE(file_handle, 'alter table "' || c_seg.owner ||'"."'||c_seg.segment_name||'" move subpartition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type LIKE '%PARTITION%' THEN
			DBMS_OUTPUT.PUT_LINE('-- Some Segments need to be handled manually');
          UTL_FILE.PUT_LINE(file_handle, '-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' Partition: ' || c_seg.partition_name || ' : Segment Type: ' || c_seg.segment_type);
        ELSE
			DBMS_OUTPUT.PUT_LINE('-- Some Segments need to be handled manually');
          UTL_FILE.PUT_LINE(file_handle, '-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' : Segment Type: ' || c_seg.segment_type);
        END IF;
      END LOOP;
      UTL_FILE.FCLOSE(file_handle);
    EXCEPTION
    WHEN e_nodir THEN
      v_todir   := FALSE;
      FOR c_seg IN c_segments(v_file_id,file_blocks)
      LOOP
        IF c_seg.segment_type LIKE '%UNDO%' THEN
          NULL;
        ELSIF c_seg.segment_type = 'TABLE' THEN
          DBMS_OUTPUT.PUT_LINE('alter table "' || c_seg.owner ||'"."'||c_seg.segment_name||'" move;');
        ELSIF c_seg.segment_type = 'INDEX' THEN
          DBMS_OUTPUT.PUT_LINE('alter index "' || c_seg.owner ||'"."'||c_seg.segment_name||'" rebuild;');
        ELSIF c_seg.segment_type = 'INDEX PARTITION' THEN
          DBMS_OUTPUT.PUT_LINE('alter index "' || c_seg.owner ||'"."'||c_seg.segment_name||'" rebuild partition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type = 'TABLE PARTITION' THEN
          DBMS_OUTPUT.PUT_LINE('alter table "' || c_seg.owner ||'"."'||c_seg.segment_name||'" move partition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type = 'INDEX SUBPARTITION' THEN
          DBMS_OUTPUT.PUT_LINE('alter index "' || c_seg.owner ||'"."'||c_seg.segment_name||'" rebuild subpartition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type = 'TABLE SUBPARTITION' THEN
          DBMS_OUTPUT.PUT_LINE('alter table "' || c_seg.owner ||'"."'||c_seg.segment_name||'" move subpartition "' || c_seg.partition_name || '";');
        ELSIF c_seg.segment_type LIKE '%PARTITION%' THEN
          DBMS_OUTPUT.PUT_LINE('-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' Partition: ' || c_seg.partition_name || ' : Segment Type: ' || c_seg.segment_type);
        ELSE
          DBMS_OUTPUT.PUT_LINE('-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' : Segment Type: ' || c_seg.segment_type);
        END IF;
      END LOOP;
    END;
    FOR c_recyc IN c_recyclebin(v_file_id,file_blocks)
    LOOP
      IF NOT v_recycle_header_printed THEN
        DBMS_OUTPUT.PUT_LINE('Following segments found in the recycle bin. You might want to purge these.');
        v_recycle_header_printed := TRUE;
      END IF;
      DBMS_OUTPUT.PUT_LINE(c_recyc.username||' '||c_recyc.original_name||' '||c_recyc.partition_name);
    END LOOP;
  EXCEPTION
  WHEN invalid_file_size THEN
    dbms_output.put_line('Invalid filesize provided: '||file_size);
    dbms_output.put_line('Filesize is either: nnG|nnM|nnK|nn');
  WHEN invalid_file_id THEN
    dbms_output.put_line('Invalid file_id provided: '||v_file_id);
    dbms_output.put_line('File not found in dba_data_files');
  END;
END;
/
