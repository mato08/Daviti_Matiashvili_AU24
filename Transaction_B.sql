begin;
select *, xmin, xmax from public.employee
e;
commit;

/* since there is nothing inserted in this transaction select outputs nothing*/


show transaction isolation level
--shows read commited here too

--transaction 2
begin;
select txid_current(); -- shows 2620 because the last value of xmin is 2619
delete from public.employee
where id = 1;
select *, xmin, xmax  
from public.employee e; /*first transaction was commited before I  ran this command and output of this is nothing but I expected the same output as it was in 
							the first transaction, but maybe it's because the phantom read anomaly happened already and that deletion was commited by the first
							transaction so it is commited here too */
commit;


--transaction 3
begin;
select txid_current(); -- here output is 2622
update public.employee
set status = 'Fired'
where id = 2;  --did the update
select *, xmin, xmax
from public.employee e; /*here now status is updated before we commit this transaction and that happened because anommaly happened.its basically the same case 
						as it was in the second transaction xmin value changed to 2622 and status became 'Fired' */
commit;



SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;
show transaction isolation level;

-- now isolation level is repeatable read


--task 5

-- first transaction
begin;
select *,cmin,cmax, xmin, xmax from public.employee e;-- here nothing is inserted yet so output is empty
commit;



-- second transaction
begin;
select txid_current();--output is 2637
delete from public.employee
where id = 1;
select *, cmin,cmax,xmin, xmax
from public.employee e; --output of this is nothing
commit;



-- third transaction	
begin;
select txid_current();--output is 2639
update public.employee
set status = 'Fired'
where id = 2;
select *, cmin,cmax,xmin, xmax 
from public.employee e;   -- here status is updated and is 'Fired' also xmin value is 2639 and xmax value is . 
						  --cmin and cmax are still 0
commit;


--task 6
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM employee;
INSERT INTO employee (name, status) VALUES ('Alice', 'Active');
COMMIT;


--task 7

SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL read committed;
show transaction isolation level;


BEGIN;
SELECT status FROM employee WHERE id = 2; 
UPDATE employee SET status = 'Terminated' WHERE id = 2;
COMMIT;


/*read committed avoids lost update anomaly.
downsides: no automatic retry: Unlike some databases that handle lost updates internally, 
PostgreSQL expects the application to handle conflicts by retrying the transaction.*/