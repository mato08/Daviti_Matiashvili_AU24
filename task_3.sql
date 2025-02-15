--prerequisite task

show search_path
set search_path to labs

drop table if exists person;

CREATE TABLE labs.person ( 
id integer NOT NULL,  
name varchar(15) 
);

INSERT INTO person VALUES(1, 'Bob'); 
INSERT INTO person VALUES(2, 'Alice'); 
INSERT INTO person VALUES(3, 'Robert'); 


--task 2


--2.1
create table if not exists test_simple(
		a int,
		b int 
);

insert into test_simple values (generate_series(1,1000000));
insert into test_simple values (generate_series(1,5000000)); 

create unlogged table test_unlogged(
		a int,
		b int
);

insert into test_unlogged values (generate_series(1,1000000)); --it only took 1 second 

insert into test_simple values (generate_series(1,5000000)); --it took 7-8 seconds 



--2.2

CREATE TABLE labs.users(
		user_id INT generated  BY DEFAULT AS IDENTITY PRIMARY KEY,
  		login VARCHAR(30)
) INHERITS (person);


INSERT INTO users(id,name,login) VALUES (1, 'new Bob', 'NewLogin'); 
INSERT INTO users(id,name,login) VALUES (999, 'TestUser', 'TestLogin');

/* when it was written like this ->INSERT INTO users VALUES (1, 'new Bob', 'NewLogin'); 
it gave me an error because it was expecting an integer and I was giving varchar so I specified column names */


SELECT * FROM users; -- shows these 2 added rows. new bob and testuser
SELECT * FROM person; --now these outputs the rows that we inserted in the persons table and also these 2 rows that we added in users
SELECT * FROM ONLY person;  -- now this only shows 3 rows that we added in person table


UPDATE person 
SET name = 'not Bob' 
where id = 1;  -- this updated 2 rows because there were 2 rows with the id=1 in the persons table.


update only  person 
SET name = 'not Bob' 
where id = 1; -- now this query onlt updates the row which belongs to person table only.


ALTER TABLE person ADD COLUMN status integer DEFAULT 0; -- these added column named status to all the rows in person and in user
ALTER TABLE person ADD CONSTRAINT status CHECK (status in (0,1)) NO 
INHERIT;  -- these checks that status value is 0 or 1 and it only goes for persons table and not for  inherited tables
ALTER TABLE person ADD CONSTRAINT id UNIQUE (id, name);-- these constraint ensures that pair of id,name is unique in whole table


-- task 3

--3.1
CREATE TABLE labs.test_index ( 
		num float NOT NULL, 
		load_date timestamptz NOT NULL 
); 

--3.2
INSERT INTO test_index(num, load_date) 
SELECT random(), x 
FROM generate_series('2017-01-01 0:00'::timestamptz, 
'2021-12-31 23:59:59'::timestamptz, '10 seconds'::interval) x; 


SELECT pg_size_pretty(pg_relation_size('test_index')); --table size is 666mb


--3.3

SELECT date_trunc('year', load_date), max(num) 
FROM test_index 
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59' 
GROUP BY 1 
ORDER BY 1;

--it ran like for 3 seconds and output of max(num) is 0.99999978283 i don't know if it's interesting info.

--3.4

CREATE INDEX btree_idx ON test_index USING btree (load_date);
/*creation took like 16 seconds. repeating the step 3  took much less time before the last time since we have index 
 * on load_date.
 */
SELECT pg_size_pretty(pg_relation_size('btree_idx')); --index size is 338mb

DROP INDEX btree_idx;


--3.5

CREATE INDEX idx_test_brin ON test_index USING brin (load_date);
--creation took 2 seconds only and repeating the step 3 again took much less time before we indexed the load_date

SELECT pg_size_pretty(pg_relation_size('idx_test_brin')); --only 40kb. that's impressive :)

drop index idx_test_brin;

drop table test_index;


--3.2


--1 task
CREATE TABLE test_index AS SELECT id, md5(id::text) as t_hash 
FROM generate_series(1, 10000000) AS id; 

SELECT pg_size_pretty(pg_relation_size('test_index')); --size is 652mb

--2 task
SELECT * FROM test_index WHERE t_hash LIKE '%ceea167a5a%'; -- it took 5 seconds and returned only one row

-- 3 task
CREATE EXTENSION pg_trgm;
CREATE INDEX idx_text_index_gist ON test_index USING gist(t_hash 
gist_trgm_ops);	
--creation of this index took 14 minutes so I can say that creation tooks lot of time especially if we have big data

SELECT * FROM test_index WHERE t_hash LIKE '%ceea167a5a%'; --now it took 18 seconds to return
SELECT pg_size_pretty(pg_relation_size('idx_text_index_gist')); -- size is 1770mb. so almost triple size of the table and I don't think thats good


drop index idx_text_index_gist;


--4 task

CREATE INDEX idx_text_index_gin ON test_index USING gin (t_hash 
gin_trgm_ops); 
--creation took 7 minutes

SELECT * FROM test_index WHERE t_hash LIKE '%ceea167a5a%'; -- it returned instantly 

SELECT pg_size_pretty(pg_relation_size('idx_text_index_gin')); --size is 613mb

drop index idx_text_index_gin;
drop table test_index;



--task 4

CREATE EXTENSION file_fdw; 
CREATE SERVER test_import FOREIGN DATA WRAPPER file_fdw; 


CREATE FOREIGN TABLE labs.test_foreign_table 
( 
LatD INT, 
LatM INT, 
LatS INT, 
NS  TEXT, 
LonD INT, 
LonM INT, 
LonS INT, 
EW TEXT, 
City TEXT, 
State TEXT 
) 
SERVER test_import  
OPTIONS  
( filename 'C:\Users\matia\Desktop\cities_list.csv',
format 'csv', 
header 'true', 
delimiter ','
); 

SELECT * FROM test_foreign_table; 
SELECT count(*) FROM test_foreign_table; --count is 128



CREATE MATERIALIZED VIEW mview AS
SELECT * FROM labs.test_foreign_table;


-- i deleted one row from the source file so now count should be 127

SELECT count(*) FROM labs.mview;--it still shows 128

--
SELECT count(*) FROM labs.test_foreign_table; -- it shows 127

-- Refresh the materialized view to sync with source data
REFRESH MATERIALIZED VIEW labs.mview;

-- Check count again after refresh
SELECT count(*) FROM labs.mview;-- now it also shows 127 


 