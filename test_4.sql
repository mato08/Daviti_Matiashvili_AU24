CREATE TABLE labs.test_index_plan ( 
num       
float NOT NULL, 
load_date timestamptz NOT NULL 
); 


INSERT INTO labs.test_index_plan(num, load_date) 
SELECT random(), x 
FROM generate_series('2017-01-01 0:00'::timestamptz, 
'2021-12-31 23:59:59'::timestamptz, '10 seconds'::interval) x; 

SET max_parallel_workers_per_gather = 0; 

explain (ANALYZE, BUFFERS)
SELECT * 
FROM labs.test_index_plan 
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 
11:59:59' 
ORDER BY 1; 

--1.2
CREATE INDEX btree_idx ON labs.test_index_plan USING btree (load_date);


explain 
SELECT load_date
FROM labs.test_index_plan 
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 
11:59:59' 
ORDER BY 1; 

drop index labs.btree_idx;


CREATE INDEX idx_test_brin ON labs.test_index_plan USING brin (load_date);

explain (analyze,buffers)
SELECT *
FROM labs.test_index_plan 
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 
11:59:59' 
ORDER BY 1; 



--task  2

--2.1
CREATE TABLE labs.test_inserts ( 
num       float NOT NULL, 
load_date timestamptz NOT NULL 
); 


--2.2
create index btree_idx on labs.test_inserts using btree(load_date);


--2.3
insert into labs.test_inserts(num,load_date)
			SELECT num, load_date 
			FROM labs.test_index_plan;
			

		
--2.4
CREATE table labs.emp ( 
empno        NUMERIC(4)  NOT NULL CONSTRAINT emp_pk PRIMARY KEY, 
ename        VARCHAR(10) UNIQUE, 
job          VARCHAR(9), 
mgr          NUMERIC(4), 
hiredate     DATE 
); 

--it is more efficient way.
INSERT INTO labs.emp (empno, ename, job, mgr, hiredate) 
VALUES 
    (1, 'SMITH', 'CLERK', 13, '1980-12-17'),
    (2, 'ALLEN', 'SALESMAN', 6, '1981-02-20'),
    (3, 'WARD', 'SALESMAN', 6, '1981-02-22'),
    (4, 'JONES', 'MANAGER', 9, '1981-04-02'),
    (5, 'MARTIN', 'SALESMAN', 6, '1981-09-28'),
    (6, 'BLAKE', 'MANAGER', 9, '1981-05-01'),
    (7, 'CLARK', 'MANAGER', 9, '1981-06-09'),
    (8, 'SCOTT', 'ANALYST', 4, '1987-04-19'),
    (9, 'KING', 'PRESIDENT', NULL, '1981-11-17'),
    (10, 'TURNER', 'SALESMAN', 6, '1981-09-08'),
    (11, 'ADAMS', 'CLERK', 8, '1987-05-23'),
    (12, 'JAMES', 'CLERK', 6, '1981-12-03'),
    (13, 'FORD', 'ANALYST', 4, '1981-12-03'),
    (14, 'MILLER', 'CLERK', 7, '1982-01-23');
   
   

--2.2
   
--1
   COPY (
    SELECT num, '"' || load_date || '"' AS load_date
    FROM labs.test_index_plan
) TO 'C:\Users\matia\Desktop\test_index_plan.csv' 
WITH DELIMITER ',' CSV HEADER;


--2
copy(
	select num, '"' || load_date|| '"' as load_date
	from labs.test_index_plan
	where load_date between '2021-09-01 0:00' AND '2021-09-01 11:59:59' 
) to 'C:\Users\matia\Desktop\test_index_plan_short.csv'
with delimiter ',' csv header;


--3
CREATE TABLE labs.test_copy ( 
	num       float NOT NULL, 
	load_date timestamptz NOT NULL 
);

--4
create index btree_index1 on labs.test_copy  using btree(load_date);

copy labs.test_copy(num,load_date)
from 'C:\Users\matia\Desktop\test_index_plan.csv'
WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);



--2.3
INSERT INTO labs.emp (empno, ename, job, mgr, hiredate) 
VALUES
    (1, 'SMITH', 'MANAGER', 13, '2021-12-01'),
    (14, 'KELLY', 'CLERK', 1, '2021-12-01'),
    (15, 'HANNAH', 'CLERK', 1, '2021-12-01'),
    (11, 'ADAMS', 'SALESMAN', 8, '2021-12-01'),
    (4, 'JONES', 'ANALYST', 9, '2021-12-01')
ON CONFLICT (empno) 
DO UPDATE 
SET
    ename = EXCLUDED.ename,
    job = EXCLUDED.job,
    mgr = EXCLUDED.mgr,
    hiredate = EXCLUDED.hiredate;
   
   
select * from labs.emp;




		