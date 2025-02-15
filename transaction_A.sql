
drop table if exists employee;
CREATE table if not exists employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    status VARCHAR(50)
);


--transaction  1
begin;
select txid_current();
insert into public.employee ("name", status)
values ('Alice', 'Not fired');
select *, xmin, xmax
from public.employee e;
commit;

/* here the output of 'select txid_currect()' was 2619 then i inserted row and the final output was 
 * 	id  name    status      xmin  xnax
 *  1   alice   Not fired   2619   0    -> since new value is inserted xmins value is 2619 and we deleted nothing so xmax is 0. 
 * 
 */ 

show transaction isolation level
--shows read commited so that explains why there was no anomaly. because it can handle this kind of anomalies.

--transaction 2
begin;
select *, xmin, xmax
from public.employee e; --output of this is the same as in the first transaction
select *, xmin, xmax
from public.employee e; /*second files delete command was ran before this command,and since read commited is the isolation level here xmax value now shows
						2620 because it can not handle phantom read anomaly*/
commit;


insert into public.employee ("name", status)
values ('Alice', 'Not fired');


-- tranasction 3

begin;
select *, xmin, xmax
from public.employee e;		-- new row is inserted and output is id->2 | name->alice | status->not fired | xmin->2621 |  xmax->0 
select *, xmin, xmax
from public.employee e;   /*since the second transactions update command was first here happened non-repeatable read anomally.
							A non-repeatable read occurs when an object is read twice within a transaction, and between the reads, 
							another transaction modifies that object.I think that is the case here*/
commit;


SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;
show transaction isolation level;

--isolation level is now repeatable read


-- task 5
drop table if exists employee;

CREATE table if not exists employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    status VARCHAR(50)
);

--first transaction
begin;
select txid_current(); --outputs 2636- I did some experiments and thats why it increased so much.
insert into public.employee ("name", status)
values ('Alice', 'Not fired');
select *, cmin,cmax,xmin, xmax
from public.employee e;				-- here new row is inserted cmin,cmax values are 0 and xmin value is 2636,xmax is 0.
commit;



-- second transaction
begin;
select *, cmin,cmax,xmin, xmax -- we have the same output as in first transaction.
from public.employee e;
select *,cmin,cmax, xmin, xmax 
from public.employee e;  /*here it shows with xmax value that this row is deleted since xmax value is 2637 
							but cmin and cmax values stayed unchanged */
commit;


insert into public.employee ("name", status)
values ('Alice', 'Not fired');


-- third transaction
begin;
select *, cmin,cmax,xmin, xmax  --cmin and cmax values remain unchanged and xmin is 2638. new row is inserted with no problem
from public.employee e;
select *,cmin,cmax, xmin, xmax  
from public.employee e;      /*I ran this command after update in the second transaction and since isolation level is not
							repeatable read now status is stil 'not fired' but xmax value changed to 2639. cmin and cmax
							are still 0*/
commit;

--task 6

BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM employee;
INSERT INTO employee (name, status) VALUES ('Bob', 'Active');
COMMIT;  -- This transaction commits successfully

/*When using the Serializable isolation level, PostgreSQL 
prevents serialization anomalies by aborting one of the conflicting transactions*/


--task 7

SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL read committed;
show transaction isolation level;


BEGIN;
SELECT status FROM employee WHERE id = 2;  -- Returns 'Active'
UPDATE employee SET status = 'On Leave' WHERE id = 2;
COMMIT;

/* since read committed avoids lost update anomaly.
downsides: no automatic retry: Unlike some databases that handle lost updates internally, 
PostgreSQL expects the application to handle conflicts by retrying the transaction.*/
