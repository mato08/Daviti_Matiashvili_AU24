CREATE TABLE test_joins_a ( 
id1 int,  
id2 int 
); 


CREATE TABLE test_joins_b ( 
id1 int,  
id2 int 
); 


INSERT INTO test_joins_a values(generate_series(1,10000),3); 
INSERT INTO test_joins_b values(generate_series(1,10000),3); 
ANALYZE;


explain analyze
SELECT * FROM test_joins_a a, test_joins_b b  
WHERE a.id1 > b.id1;
/* it uses nested loop because in the where condition we have '>' operator and not equality. merge join needs the sorted list 
 * and it's not a sorted list.hash join only works when we have '=' operator and only left is nesteed loop. 
 */



SELECT *  
FROM test_joins_a a 
CROSS JOIN test_joins_b b; 

/*as it was written in the materials a NESTED LOOP is the only way to execute a CROSS join,because it's a carteisan product
and basically nested loop's algorithm works the same way too */


--1.2
explain analyze
SELECT * FROM test_joins_a a,test_joins_b b 
WHERE a.id1 = b.id1; 

/* i replaced '>' with '=' because hash join algorithm works for only '=' operator and also it's good practice to use 
hash join for big tables and that's why postgres now chooses hash join instead of nested join */

EXPLAIN ANALYZE  
SELECT max(id1)  
FROM test_joins_a a  
WHERE EXISTS (  
    SELECT 1 FROM test_joins_b b WHERE a.id1 = b.id1  
);

/*"Semi joins is kind of sub join type to the joining methods such as hash, 
 merge, and nested loop, where the optimizer prefers to use them for EXISTS/IN or NOT EXISTS/NOT IN operators."
 I used this and wrote "where exists" clause. now it uses Hash Semi join */


set enable_hashjoin=off
/* after this it uses merge semi join algorithm,maybe because since we have '='operator in subquery postgres prefers
to use merge join */

set enable_hashjoin=on


--1.3
create index idx_a on test_joins_a(id1);
create index idx_b1 on test_joins_b(id1);

EXPLAIN ANALYZE
SELECT *  
from test_joins_a a
join test_joins_b b
on a.id1=b.id1
order by a.id1,b.id1;

/* I created indexes on both of the table columns and sorted the tables based on these columns. merge join needs sorted lists
 * and also it's preferable to use indexes. now postgres prefers to use merge join.
 * "Merge Join  (cost=0.57..786.57 rows=10000 width=16) (actual time=0.068..32.710 rows=10000 loops=1)"
 */

set enable_mergejoin=off
/* now postgres uses hash join. since we disabled merge join now it has no other choice other than hash join */


--2.1
CREATE TABLE test_joins_c 
( 
id1 int,  
id2 int 
); 

INSERT INTO test_joins_c 
values(generate_series(1,1000000),(random()*10)::int); 

EXPLAIN  
SELECT c.id2  
FROM test_joins_b b 
JOIN test_joins_a a on (b.id1 = a.id1) 
LEFT JOIN test_joins_c c on (c.id1 = b.id1); 

/* in the output it is written like this. at first we use seq scan on table test_joins_a and then hash its values then 
it uses seq scan on test_joins_b and after that it uses hash join on condition b.id1=a.id1. after thet  then it hashes these values
again and then uses seq scan on test_joins_c and after that uses hash right join to join these tables on the condition that is given.*/

set join_collapse_limit = 1 
/* setting join_collapse_limit to 1 prevents reordering, forcing PostgreSQL to execute joins in the 
 * same order they appear in the query. but  plan didn't change it's same  as it was  the same before 
 */

set join_collapse_limit = 8

CREATE TABLE orders AS 
SELECT   id AS order_id, 
(id * 10 * random()*10)::int AS order_cost, 
'order number ' || id AS order_num 
FROM generate_series(1, 1000) AS id; 

CREATE TABLE stores ( 
store_id int, 
store_name text, 
max_order_cost int 
); 

iNSERT INTO stores VALUES 
(1, 'grossery shop', '800'), 
(2, 'bakery', '100'), 
(3, 'manufactured goods', '3000') 
; 


--2.2
SELECT s.store_id, s.store_name, o.order_id, o.order_cost
FROM stores s
LEFT JOIN LATERAL (
    SELECT o.order_id, o.order_cost
    FROM orders o
    WHERE o.order_cost < s.max_order_cost
    ORDER BY o.order_cost DESC
    LIMIT 10
) o ON TRUE
ORDER BY s.store_id, o.order_cost DESC;


select * from labs.emp e 


WITH RECURSIVE mgmt_hierarchy AS (
  SELECT 
    e.empno,
    e.ename as employee_name,
    e.mgr,
    e.ename as manager_name,  
    1 as level
  FROM labs.emp e
  WHERE e.mgr IS NULL

  UNION ALL
  
  SELECT 
    e.empno,
    e.ename as employee_name,
    e.mgr,
    m.ename as manager_name,
    h.level + 1 as level
  FROM labs.emp e
  JOIN mgmt_hierarchy h ON e.mgr = h.empno
  JOIN labs.emp m ON e.mgr = m.empno
)
SELECT 
  empno,
  employee_name,
  mgr,
  manager_name,
  level
FROM mgmt_hierarchy
ORDER BY level, employee_name;



--3.2
CREATE TABLE order_log 
( 
log_id  integer primary key generated always as identity, 
order_id   integer,   
order_cost integer,    
order_num    text, 
action_type  varchar(1) CHECK (action_type IN ('U','D')), 
log_date TIMESTAMPTZ DEFAULT Now()     
);

--3.2
WITH updated_orders AS (
    UPDATE orders
    SET order_cost = order_cost / 2
    WHERE order_cost BETWEEN 100 AND 1000
    RETURNING 
        order_id,
        order_cost,
        order_num,
        'U' as action_type
),
deleted_orders AS (
    DELETE FROM orders
    WHERE order_cost < 50
    RETURNING 
        order_id,
        order_cost,
        order_num,
        'D' as action_type
),
combined_changes AS (
    SELECT * FROM updated_orders
    UNION ALL
    SELECT * FROM deleted_orders
)
INSERT INTO order_log (order_id, order_cost, order_num, action_type)
SELECT 
    order_id,
    order_cost,
    order_num,
    action_type
FROM combined_changes;

select * from order_log