REM Showing segments which do have extents above specified size
REM
REM No block_id/location in file is shown due to fact that the
REM segment itself is blocking and not a specific extent.
REM 
REM Input parameters:
REM &file_id - file_id of file to check
REM &file_size - desired size of file to shrink to (nnG/nnM/nnK/nn)
REM
REM Script meant for Oracle 10 and higher
REM in case of version lower then Oracle 10 comment out the lines as indicated


set serveroutput on size 1000000

declare 
  file_blocks number;
  file_id number;
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
order  by owner,segment_name;

/* in case of version lower then Oracle 10 comment out following 6 lines!! */
cursor c_recyclebin (fno in number, block in number) is
select username,original_name,partition_name
from   recyclebin$, dba_users
where  file#=fno
and    block# >= block
and    owner#=user_id;

begin
  file_id:=&file_id;
  file_size:='&file_size_char';
  
  dbms_output.put_line('.');

  /* check if correct file is provided and what blocksize is of tablespace */
  for c_check_blocksize in c_blocksize (file_id)
  loop
    file_block_size:=c_check_blocksize .block_size;
    file_name:=c_check_blocksize.file_name;
  end loop; 
  
  if file_name is NULL
  then
    raise invalid_file_id;
  end if;

  /* check if a correct filesize is provided */
  <<isnumber>>
  for i in 1..length(file_size)
  loop
    if (ascii(substr(file_size,i,1)) between 48 and 57)
    then
       null; /* number */
    else 
      yesno:=FALSE;
      exit isnumber;
    end if;
   end loop;
   
  if (length(file_size) != instr(upper(file_size),'G')) 
     AND (length(file_size) != instr(upper(file_size),'M'))
     AND (length(file_size) != instr(upper(file_size),'K'))
     AND (NOT yesno)
  then
     raise invalid_file_size;
  else
     case upper(substr(file_size,length(file_size),1))
       when 'G' then
         file_blocks:=floor((substr(file_size,1,length(file_size)-1)*power(2,30))/file_block_size);
       when 'M' then
         file_blocks:=floor((substr(file_size,1,length(file_size)-1)*power(2,20))/file_block_size);
       when 'K' then
         file_blocks:=floor((substr(file_size,1,length(file_size)-1)*power(2,10))/file_block_size);
       else
         file_blocks:=floor(file_size/file_block_size); 
     end case;
  end if;
  
  /* time to show the segments having extents at/above desired size of file */
  dbms_output.put_line('Overview of segments in file:' ||file_name);
  dbms_output.put_line('having extents above size: '||file_size||' block: '||file_blocks);
  dbms_output.put_line('.');
  dbms_output.put_line('owner segment_name partition_name segment_type');

  for c_seg in c_segments(file_id,file_blocks)
  loop
    dbms_output.put_line(c_seg.owner||' '||c_seg.segment_name||' '||c_seg.partition_name||' '||c_seg.segment_type);
   end loop;

   /* possible segments present in recyclebin */
   /* in case of version lower then Oracle 10 comment out following 7 lines!! */
   dbms_output.put_line('.');
   dbms_output.put_line('Segments as found in recycle bin');
   dbms_output.put_line('owner segment_name partition_name');
   for c_recyc in c_recyclebin(file_id,file_blocks)
   loop
     dbms_output.put_line(c_recyc.username||' '||c_recyc.original_name||' '||c_recyc.partition_name);
   end loop;

exception
  when invalid_file_size then
    dbms_output.put_line('Invalid filesize provided: '||file_size);
    dbms_output.put_line('Filesize is either: nnG|nnM|nnK|nn');
  when invalid_file_id then
    dbms_output.put_line('Invalid file_id provided: '||file_id);
    dbms_output.put_line('File not found in dba_data_files');
  when others then
    null;

end;
/
