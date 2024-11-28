--Task 2
				
		-- Create table ‘table_to_delete’ and fill it with the following query
		
				
               CREATE TABLE table_to_delete AS
               SELECT 'veeeeeeery_long_string' || x AS col
               FROM generate_series(1,(10^7)::int) x; -- generate_series() creates 10^7 rows of sequential numbers from 1 to 10000000 (10^7)

		--it took 23 second to read the data and create the table. 
               
               
               
      --Lookup how much space this table consumes with the following query:
      	
               
                SELECT *, pg_size_pretty(total_bytes) AS total,
                                    pg_size_pretty(index_bytes) AS INDEX,
                                    pg_size_pretty(toast_bytes) AS toast,
                                    pg_size_pretty(table_bytes) AS TABLE
               FROM ( SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
                               FROM (SELECT c.oid,nspname AS table_schema,
                                                               relname AS TABLE_NAME,
                                                              c.reltuples AS row_estimate,
                                                              pg_total_relation_size(c.oid) AS total_bytes,
                                                              pg_indexes_size(c.oid) AS index_bytes,
                                                              pg_total_relation_size(reltoastrelid) AS toast_bytes
                                              FROM pg_class c
                                              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                                              WHERE relkind = 'r'
                                              ) a
                                    ) a
               WHERE table_name LIKE '%table_to_delete%';
              
              
              
             -- under the row 'table' it is written 575MB so it takes 575MB space.
              
              
               --3. Issue the following DELETE operation on ‘table_to_delete’: 
              
              DELETE FROM table_to_delete
               WHERE REPLACE(col, 'veeeeeeery_long_string','')::int % 3 = 0; 
              
              --a)it took 11 seconds to delete 1/3 rows of this table
              --b)I rerunned the code above and it still takes 575MB which. I thought that now it would take 575/3MB but i was wrong
              
               VACUUM FULL VERBOSE table_to_delete
              /*d) now table takes only 383MB space.so based on this information I can say that DELETE just deletes the rows but
               * the space that table consumes remains unchanged.so VACUUM FULL VERBOSE just deletes the space that is not used
               * and nothing is written in it.
               * it took 8 seconds to perform this query.
               */
               --e) I used-drop table table_to_delete and then recreated it.
               
          --4. Issue the following TRUNCATE operation:     
               
               TRUNCATE table_to_delete;
              
              --a) it took only  1 second
              /*b)truncate takes less time to perform statement,it deletes the whole table data in 1 second
               * when delete took 11 seconds to delete 1/3 of rows. so truncate is way more faster. but if we don't want
               * to delete whole data truncate will not help us.
               */
               --c) space consumption now is zero, so it deletes data and also the space that the data took.