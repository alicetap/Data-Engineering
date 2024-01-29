CREATE OR REPLACE PROCEDURE "admin".update_tb_pg_tables()
	LANGUAGE plpgsql
AS $$
	                                                                                                                                                                                                                              
DECLARE                                                                                                                                                                                                                            
  list record;                                                                                                                                                                                                                     
  table_name varchar(max) := 'admin.tb_pg_tables';                                                                                                                                                                           
  command1 varchar(max) = 'SELECT schemaname, tablename, tableowner FROM pg_tables';       
  tmp varchar(max) := 'INSERT INTO ' || table_name || ' VALUES ';                                                                                                                                                                  
  tmp2 varchar(max) = '';                                                                                                                                                                                                          
  stmt varchar(max) := '';                                                                                                                                                                                                         
  counter int := 0;                                                                                                                                                                                                                
  count_table int;                                                                                                                                                                                                                 
BEGIN	                                                                                                                                                                                                                           
  SELECT COUNT(1) INTO count_table FROM pg_tables;                                                                                                                                                                      
  --EXECUTE 'TRUNCATE TABLE ' || table_name;                                                                                                                                                                                         
  FOR list IN EXECUTE command1 LOOP                                                                                                                                                                                                
    tmp2 := tmp2 || '(' || quote_literal(list.schemaname::VARCHAR)                                                                                                                                                              
                 || ',' || quote_literal(list.tablename::VARCHAR)                                                                                                                                                                
                 || ',' || quote_literal(list.tableowner::VARCHAR)       
                 || '), ';                                                                                                                                                                                                         
    counter := counter + 1;                                                                                                                                                                                                        
    IF counter % 200 = 0 OR counter = count_table THEN                                                                                                                                                                             
      tmp2 := rtrim(tmp2, ', ');                                                                                                                                                                                                   
      stmt := tmp || tmp2;                                                                                                                                                                                                         
      RAISE INFO '% rows were loaded.', counter;                                                                                                                                                                            
	  IF tmp2 <> '' THEN                                                                                                                                                                                                           
        EXECUTE stmt;                                                                                                                                                                                                              
      END IF;                                                                                                                                                                                                                      
      tmp2 := '';                                                                                                                                                                                                                  
    END IF;                                                                                                                                                                                                                        
  END LOOP;                                                                                                                                                                                                                        
END;                                                                                                                                                                                                                               

$$
;