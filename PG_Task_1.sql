create database test_db;

select d.oid, d.datname, d.datistemplate, d.datallowconn, t.spcname 
from pg_database d 
join pg_tablespace t on t.oid = d.dattablespace 


/* datname shows the name of the database,datistemplate shows if database is a template or not,datallowconn tells usage 
if database has connections and spcname shows the name of tablespace where it is located.*/

CREATE TABLESPACE mytablespace 
LOCATION 'C:/Program Files/PostgreSQL/15/data/tblspc_test/';


select * 
from pg_tablespace ;


ALTER DATABASE test_db SET TABLESPACE mytablespace;
/* after running this command the name of the tablespace in which test_db is located changes to mytablespace.*/

create schema if not exists labs;

CREATE table if not exists labs.person ( 
id integer NOT NULL,  
name varchar(15) 
);


SELECT schemaname, tablename FROM pg_tables 
WHERE tablename = 'person'; 

/* schemaname shows 'labs' and tablename shows 'person'*/

INSERT INTO person VALUES(1, 'Bob'); 
INSERT INTO person VALUES(2, 'Alice'); 
INSERT INTO person VALUES(3, 'Robert'); 


show search_path;
/* its output is public, public, "$user"'*/


SET search_path TO labs;
/* with the help of this i can insert new values in the tables without specifing the schema name.*/



CREATE EXTENSION pageinspect; 


select p.id, p.name, p.ctid, p.xmin, p.xmax from person p; 
SELECT t_xmin, t_xmax, t_ctid, 
tuple_data_split('labs.person'::regclass, t_data, t_infomask, 
t_infomask2, t_bits)  
FROM heap_page_items(get_raw_page('labs.person', 0)); 


INSERT INTO person VALUES(4, 'John'); 
UPDATE person set name = 'Alex' where id = 2; 
DELETE FROM person WHERE id = 3; 
INSERT INTO person VALUES(999, 'Test');
DELETE FROM person WHERE id = 999; 



/* So, based on my understanding, t_xmin represents the transaction ID that inserted the row, and t_xmax represents the transaction ID that deleted the row. Initially, when we insert rows,
   t_xmax is set to 0. After a delete operation, both the insert ID (t_xmin) and delete ID (t_xmax)
   are recorded, which could potentially allow us to still see the deleted value.

   An interesting aspect of updates is that PostgreSQL preserves the history of changes.
   When I performed an update on the "name" field, a new row was inserted with a new t_xmin value (2573),
   and this value was assigned to the previous row where id = 2. This allows us to track if the attribute values
   have been updated. However, I'm not entirely sure whether we can access the previous value of the attribute,
   as only the new version appears to be stored in the row.*/



vacuum labs.person;
/* After executing the operation, the deleted rows were successfully removed, and all values were replaced with NULL. 
 An interesting observation is that it not only deleted the values of the rows we explicitly deleted, 
 but it also removed the values of the rows that were updated. 
 As a result, the previous values of those updated rows were lost as well. */

INSERT INTO person VALUES(5, 'Sarah'); 

/*New values were inserted without any issues, but there's an interesting observation. Each insert had a unique t_xmin value,
 and the value increased sequentially. I deleted some of the inserted values, and their t_xmin values should no longer be present.
 However, when inserting "Sarah," the t_xmin value should have been the next sequential number (one higher than the last inserted t_xmin). 
 But as it turns out, PostgreSQL retains the history of previous t_xmin values, and the new t_xmin value for "Sarah" 
 was different from what I expected.*/

vacuum full labs.person;

/* using this null values are dissapeared and only real data is stored here */

