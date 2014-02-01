CREATE OR REPLACE PROCEDURE ANALYZE_DATAFILE(DATAFILE_NAME IN VARCHAR2, OUTPUT_DIR IN VARCHAR2 DEFAULT 'TMP_DIR', OUTPUT_FILE IN VARCHAR2 DEFAULT 'defrag.sql') AS
	v_datafile_name VARCHAR(1000) := DATAFILE_NAME;
BEGIN
	DECLARE 
		file_blocks number;
		v_file_id number;
		file_name dba_data_files.file_name%type:=NULL;
		file_size varchar2(100);
		file_block_size number:=0;
		yesno boolean:=TRUE;

		invalid_file_id exception;
		invalid_file_size exception;

		cursor c_blocksize (fno in number) is
		select t.block_size, d.file_name
		from dba_tablespaces t, dba_data_files d
		where d.file_id=fno
		and   d.tablespace_name=t.tablespace_name;

		cursor c_segments (fno in number, block in number) is
		select distinct owner,segment_name,partition_name,segment_type
		from   dba_extents
		where  file_id=fno
		and    block_id >= block
		order  by owner,segment_type,segment_name;

		cursor c_recyclebin (fno in number, block in number) is
		select username,original_name,partition_name
		from   recyclebin$, dba_users
		where  file#=fno
		and    block# >= block
		and    owner#=user_id;
		
		file_handle					UTL_FILE.FILE_TYPE;
		v_recycle_header_printed	BOOLEAN	:=	FALSE;
		v_todir						BOOLEAN :=	TRUE;
		e_nodir						EXCEPTION;
		PRAGMA EXCEPTION_INIT(e_nodir, -29280);
	BEGIN
		DBMS_OUTPUT.PUT_LINE('-- Analyzing datafile : ' || v_datafile_name);
		
		select file_id into v_file_id from dba_data_files where file_name = v_datafile_name;

		-- ideal size of datafile
		select 	ddf.bytes - ( select sum(bytes) from dba_free_space where file_id = ddf.file_id)
		into	file_size
		from 	dba_data_files ddf
		where	file_id = v_file_id;

		/* check if correct file is provided and what blocksize is of tablespace */
		for c_check_blocksize in c_blocksize (v_file_id) loop
			file_block_size:=c_check_blocksize .block_size;
			file_name:=c_check_blocksize.file_name;
		end loop; 
	  
		IF file_name is NULL THEN
			raise invalid_file_id;
		END IF;

		file_blocks:=floor(file_size/file_block_size); 
		
		BEGIN
			file_handle := UTL_FILE.FOPEN(OUTPUT_DIR, OUTPUT_FILE,'A',32000);
			UTL_FILE.PUT_LINE(file_handle, '-- Analyzing datafile : ' || v_datafile_name);
			for c_seg in c_segments(v_file_id,file_blocks)
			loop
				IF c_seg.segment_type = 'TABLE' THEN
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
					UTL_FILE.PUT_LINE(file_handle, '-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' Partition: ' || c_seg.partition_name || ' : Segment Type: c_seg.segment_type');
				ELSE
					UTL_FILE.PUT_LINE(file_handle, '-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' : Segment Type: c_seg.segment_type');
				END IF;
			end loop;
			UTL_FILE.FCLOSE(file_handle);
		EXCEPTION
			WHEN e_nodir THEN
				v_todir := FALSE;
				FOR c_seg IN c_segments(v_file_id,file_blocks) LOOP
					IF c_seg.segment_type = 'TABLE' THEN
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
						DBMS_OUTPUT.PUT_LINE('-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' Partition: ' || c_seg.partition_name || ' : Segment Type: c_seg.segment_type');
					ELSE
						DBMS_OUTPUT.PUT_LINE('-- Manually Handle Segment "' || c_seg.owner ||'"."'||c_seg.segment_name|| ' : Segment Type: c_seg.segment_type');
					END IF;
				END LOOP;
		END;	
		
		FOR c_recyc in c_recyclebin(v_file_id,file_blocks) LOOP
			IF NOT v_recycle_header_printed THEN
				DBMS_OUTPUT.PUT_LINE('Following segments found in the recycle bin. You might want to purge these.');
				v_recycle_header_printed := TRUE;
			END IF;
			DBMS_OUTPUT.PUT_LINE(c_recyc.username||' '||c_recyc.original_name||' '||c_recyc.partition_name);
		END LOOP;

	exception
	  when invalid_file_size then
		dbms_output.put_line('Invalid filesize provided: '||file_size);
		dbms_output.put_line('Filesize is either: nnG|nnM|nnK|nn');
	  when invalid_file_id then
		dbms_output.put_line('Invalid file_id provided: '||v_file_id);
		dbms_output.put_line('File not found in dba_data_files');
	end;
END;
/
