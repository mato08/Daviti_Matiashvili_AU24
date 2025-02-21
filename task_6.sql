--task 1.1


CREATE TABLE SALES_INFO 
( 
id INTEGER, 
category  VARCHAR(1), 
ischeck   BOOLEAN, 
eventdate DATE 
);

--1)
CREATE TABLE sales_info_2021 (
    CHECK (eventdate >= '2021-01-01' AND eventdate < '2022-01-01')
) INHERITS (sales_info);

CREATE TABLE sales_info_2022 (
    CHECK (eventdate >= '2022-01-01' AND eventdate < '2023-01-01')
) INHERITS (sales_info);

CREATE TABLE sales_info_2023 (
    CHECK (eventdate >= '2023-01-01' AND eventdate < '2024-01-01')
) INHERITS (sales_info);

CREATE TABLE sales_info_2024 (
    CHECK (eventdate >= '2024-01-01' AND eventdate < '2025-01-01')
) INHERITS (sales_info);

CREATE TABLE sales_info_default (
    CHECK (eventdate < '2021-01-01' OR eventdate >= '2025-01-01')
) INHERITS (sales_info);

--created default partition in the case if our eventdate will not be in neither partitions



--2)
CREATE OR REPLACE FUNCTION partition_sales_info()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.eventdate >= '2021-01-01' AND NEW.eventdate < '2022-01-01') THEN
        INSERT INTO sales_info_2021 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2022-01-01' AND NEW.eventdate < '2023-01-01') THEN
        INSERT INTO sales_info_2022 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2023-01-01' AND NEW.eventdate < '2024-01-01') THEN
        INSERT INTO sales_info_2023 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2024-01-01' AND NEW.eventdate < '2025-01-01') THEN
        INSERT INTO sales_info_2024 VALUES (NEW.*);
    ELSE
        insert into sales_info_default values(new.*);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--3)
create or replace TRIGGER sales_info_partition_trigger
BEFORE INSERT ON sales_info
FOR EACH ROW EXECUTE FUNCTION partition_sales_info();

--4)
INSERT INTO SALES_INFO(id,category, ischeck, EventDate) 
SELECT id 
,('{"A","B","C","D","E","F","J","H","I","J","K"}'::text[])[(
 (RANDOM())*10)::INTEGER] category 
,((1*(RANDOM())::INTEGER)<1) ischeck 
,(NOW() - '10 day'::INTERVAL * (RANDOM()::int * 100)):: 
DATE EventDate 
FROM generate_series(1,10000000) id;






--5)
CREATE OR REPLACE FUNCTION update_sales_date(p_id INTEGER, p_new_date DATE)
RETURNS VOID AS $$
DECLARE
    v_current_record sales_info%ROWTYPE;
BEGIN
    --get the current record
    SELECT * INTO v_current_record 
    FROM sales_info 
    WHERE id = p_id;
    
    IF NOT FOUND THEN
        RAISE NOTICE 'no record found with ID: %', p_id;
        RETURN;
    END IF;
    
    -- Delete from current partition
    DELETE FROM sales_info WHERE id = p_id;
    
    -- Update the date in record
    v_current_record.eventdate := p_new_date;
    
    -- Insert into correct partition based on new date
    IF p_new_date >= '2024-01-01' AND p_new_date < '2025-01-01' THEN
        INSERT INTO sales_info_2024 VALUES (v_current_record.*);
    ELSIF p_new_date >= '2023-01-01' AND p_new_date < '2024-01-01' THEN
        INSERT INTO sales_info_2023 VALUES (v_current_record.*);
    ELSIF p_new_date >= '2022-01-01' AND p_new_date < '2023-01-01' THEN
        INSERT INTO sales_info_2022 VALUES (v_current_record.*);
    ELSIF p_new_date >= '2021-01-01' AND p_new_date < '2022-01-01' THEN
        INSERT INTO sales_info_2021 VALUES (v_current_record.*);
    ELSE
        INSERT INTO sales_info_default VALUES (v_current_record.*);
    END IF;
    
    RAISE NOTICE 'Successfully updated record with ID: %', p_id;
END;
$$ LANGUAGE plpgsql;


SELECT update_sales_date(4, '2024-06-15');
select * from sales_info_2024 si 

/* at first i was using basic update command but it was impossible for me to update it because it when I was updating some rows
with eventdate the update was happening in the partition table and since I have check constraints on them,it was impossible.
for example id=4 was in partition of 2022 and when i tried update it didn't move to year 2024 partition instead of postgres tried
to update it in this partition and because of the constraint it was giving me an error. so I created function.created the record 
then added our row in that record. then deleted it from our table and based on the eventdate again added this record in the correct
partition and it worked well. I hope this way is correct too*/

--6)
CREATE TABLE SALES_INFO_simple 
( 
id   
INTEGER, 
category  VARCHAR(1), 
ischeck   BOOLEAN, 
eventdate DATE 
); 

INSERT INTO SALES_INFO_simple(id,category, ischeck, EventDate) 
SELECT id 
,('{"A","B","C","D","E","F","J","H","I","J","K"}'::text[])[(
 (RANDOM())*10)::INTEGER] category 
,((1*(RANDOM())::INTEGER)<1) ischeck 
,(NOW() - '10 day'::INTERVAL * (RANDOM()::int * 100)):: 
DATE EventDate 
FROM generate_series(1,10000000) id;


select * from sales_info_simple
select * from sales_info si
/* I would say that the main difference between those two are is that the sales_info_simple is sorted based on the id
 * and sales_info is sorted with event_date. the reason for that may be partitioning. since sales_info is partitioned at first
 * query shows the first partition then second and so on. but in sales_info_simple there is no partition and it's sorted based on id.
 */


select * from sales_info si 
where eventdate between '2022-05-28' and '2022-11-28'

select * from sales_info_simple
where eventdate between '2022-05-28' and '2022-11-28'

/* I would say that the main difference between them is how ther ids are distributed I think maybe that's the only difference*/

select eventdate from sales_info si 
where eventdate='2022-05-28'

select eventdate from sales_info_simple
where eventdate='2022-05-28'

/* i don't know what was the purpose of this task. the outputs are the same and it returns thousands of rows from both of the queries*/


select count(*) from sales_info si 
-- count is 10 000 000 
select count(*) from sales_info_simple
--also 10 000 000 here


select count(*) from sales_info si 
where eventdate between '2022-05-28' and '2022-11-28'

--count is 5 002 131

select count(*) from sales_info_simple
where eventdate between '2022-05-28' and '2022-11-28'
-- count is 4 996 534


--7)
drop table if exists sales_info_2021;

create table  sales_info_3000(
	check (eventdate>='3000-01-01' and eventdate<'3001-01-01')
) inherits(sales_info);




--task 1.2

--1)

create table sales_info_dp(
   	id integer,
   	category varchar(1),
   	ischeck boolean,
   	eventdate date
) partition by range(eventdate);


CREATE TABLE sales_info_dp_before_2021 PARTITION OF sales_info_dp
    FOR VALUES FROM (minvalue) TO ('2021-01-01')
    PARTITION BY LIST (category);

-- List partitions for categories 'A'/'B' and 'C'/'D', plus a default
CREATE TABLE sales_info_dp_before_2021_ab PARTITION OF sales_info_dp_before_2021
    FOR VALUES IN ('A', 'B');
CREATE TABLE sales_info_dp_before_2021_cd PARTITION OF sales_info_dp_before_2021
    FOR VALUES IN ('C', 'D');
CREATE TABLE sales_info_dp_before_2021_default PARTITION OF sales_info_dp_before_2021
    DEFAULT;
   
   
   
   -- Yearly partition for 2021
CREATE TABLE sales_info_dp_2021 PARTITION OF sales_info_dp
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01')
    PARTITION BY LIST (category);

-- List partitions
CREATE TABLE sales_info_dp_2021_ab PARTITION OF sales_info_dp_2021
    FOR VALUES IN ('A', 'B');
CREATE TABLE sales_info_dp_2021_cd PARTITION OF sales_info_dp_2021
    FOR VALUES IN ('C', 'D');
CREATE TABLE sales_info_dp_2021_default PARTITION OF sales_info_dp_2021
    DEFAULT;
   
   

   -- Yearly partition for 2022
CREATE TABLE sales_info_dp_2022 PARTITION OF sales_info_dp
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01')
    PARTITION BY LIST (category);

-- List partitions
CREATE TABLE sales_info_dp_2022_ab PARTITION OF sales_info_dp_2022
    FOR VALUES IN ('A', 'B');
CREATE TABLE sales_info_dp_2022_cd PARTITION OF sales_info_dp_2022
    FOR VALUES IN ('C', 'D');
CREATE TABLE sales_info_dp_2022_default PARTITION OF sales_info_dp_2022
    DEFAULT;
   
   

   -- Yearly partition for 2023
CREATE TABLE sales_info_dp_2023 PARTITION OF sales_info_dp
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
    PARTITION BY LIST (category);

-- List partitions
CREATE TABLE sales_info_dp_2023_ab PARTITION OF sales_info_dp_2023
    FOR VALUES IN ('A', 'B');
CREATE TABLE sales_info_dp_2023_cd PARTITION OF sales_info_dp_2023
    FOR VALUES IN ('C', 'D');
CREATE TABLE sales_info_dp_2023_default PARTITION OF sales_info_dp_2023
    DEFAULT;
   
   
   -- Yearly partition for 2024 (optional)
CREATE TABLE sales_info_dp_after_2023 PARTITION OF sales_info_dp
    FOR VALUES FROM ('2024-01-01') TO (maxvalue)
    PARTITION BY LIST (category);

-- List partitions
CREATE TABLE sales_info_dp_after_2023_ab PARTITION OF sales_info_dp_after_2023
    FOR VALUES IN ('A', 'B');
CREATE TABLE sales_info_dp_after_2023_cd PARTITION OF sales_info_dp_after_2023
    FOR VALUES IN ('C', 'D');
CREATE TABLE sales_info_dp_after_2023_default PARTITION OF sales_info_dp_after_2023
    DEFAULT;
   
   
   
 
   
--3)
INSERT INTO SALES_INFO_DP(id,category, ischeck, EventDate) 
SELECT id 
,('{"A","B","C","D","E","F","J","H","I","J","K"}'::text[])[((
 RANDOM())*10)::INTEGER] category 
,((1*(RANDOM())::INTEGER)<1) ischeck 
,(NOW() - '10 day'::INTERVAL * (RANDOM()::int * 100)):: 
DATE EventDate 
FROM generate_series(1,10000000) id; 


--4)
update sales_info_dp
	set category='B'
	where category='A'
	
/* this kind of update worked here.at this moment I don't fully understand why it worked here and not 
 * in inheritence partitioning but I will try to understand it.maybe it's because check constraints? I don't know*/

	
--5)
	
select * from sales_info_dp
select * from sales_info_simple

/* difference between the output is that SALES_INFO_SIMPLE is sorted based on the id and sales_info_dp is sorted by category
because the outputs of sales_info_dp are first all B then A and so on*/


select * from sales_info_dp
where eventdate between '2022-05-28' and '2023-01-01';

select * from sales_info_simple
where eventdate between '2022-05-28' and '2023-01-01';


--outputs are the same but with that difference that i mentioned above


select eventdate from sales_info_dp
where eventdate='2022-05-28';

select eventdate from sales_info_simple
where eventdate='2022-05-28';

--outputs are the same

select * from sales_info_dp
where category ='B';

select * from sales_info_simple
where category ='B';

/* one difference between outputs is that in sales_info_dp output eventdates are sorted and in sales_info_simple
 * first rows event date is year 2022 next rows date year is 2025 and it goes like that. other things are same*/

select * from sales_info_dp
where category in ('A','B','C');

select * from sales_info_simple
where category in ('A','B','C');

--same result here. eventdate is sorted in sales_info_dp but not ins sales_info_simple

select * from sales_info_dp
where category in ('A','B','C') and eventdate='2022-05-28';

select * from sales_info_simple
where category in ('A','B','C') and eventdate='2022-05-28';


/*the only difference is that  ids start from 29000 in sales_info_dp and from 5000 in sales_info_simple also
categories is sorted in sales_info_dp. first are Bs but in sales_info_simple first is B second C and so on */

select count(*) from sales_info_dp
select count(*) from sales_info_simple

--both 10 000 000

select count(*) from sales_info_dp
where eventdate between '2022-01-01' and '2023-01-01';


select count(*) from sales_info_simple
where eventdate between '2022-01-01' and '2023-01-01';

--5 000 292 in sales_info_dp and  4 996 534 in sales_info_simple




--6)

-- Create a new partition for 'A'
-- Drop the old partition for 'A', 'B'
DROP TABLE sales_info_dp_2021_ab;

CREATE TABLE sales_info_dp_2021_a PARTITION OF sales_info_dp_2021
    FOR VALUES IN ('A');

-- Create a new partition for 'B'
CREATE TABLE sales_info_dp_2021_b PARTITION OF sales_info_dp_2021
    FOR VALUES IN ('B');

  
drop table sales_info_dp_2021_a;
drop table sales_info_dp_2021_b;



--task 2

SET max_parallel_workers_per_gather = 4;

EXPLAIN SELECT * FROM sales_info;
EXPLAIN SELECT * FROM sales_info_dp;
EXPLAIN SELECT * FROM sales_info_simple;

/* sales info output- since we have no index on the tables it uses the sequence scan on all the created tables above and thats it.
 * it does the same for sales_info_dp but there we have much more partitions since i created partitions for already partitioned tables
 * and in sales_info_simple query only made one sequence scan since it's the only table without any partitions.
 */

EXPLAIN SELECT * FROM sales_info ORDER BY eventdate;
EXPLAIN SELECT * FROM sales_info_dp ORDER BY eventdate;
EXPLAIN SELECT * FROM sales_info_simple ORDER BY eventdate;

/* in sales_info output it uses parallel sequence scan for all the partitioned tables then uses parallel append and then sorts
 * and key of the sort is eventdate since we have "order by eventdate" and finnaly uses gather merge.
 * same logic goes for sales_info_dp too
 * in sales_info_simple there is again only one parallel seq scan and everything else is the same
 */

EXPLAIN SELECT COUNT(*) FROM sales_info;
EXPLAIN SELECT COUNT(*) FROM sales_info_dp;
EXPLAIN SELECT COUNT(*) FROM sales_info_simple;

/* in sales_info output we have parallel sequence scan then parallel append and after that we have partial aggregate then we have
 * 'Gather' and finally finalize aggregate
 * same goes for sales_info_dp 
 * same logic is for sales_info_simple too
 */

EXPLAIN SELECT * FROM sales_info WHERE eventdate BETWEEN '2022-01-01' AND '2022-12-31';
EXPLAIN SELECT * FROM sales_info_dp WHERE eventdate BETWEEN '2022-01-01' AND '2022-12-31';
EXPLAIN SELECT * FROM sales_info_simple WHERE eventdate BETWEEN '2022-01-01' AND '2022-12-31';

/* in sales_info at first we use seq scan on sales_info_2022 since its the correct partition and then we go to the parent table 
 * as I understood and again do the seq scan there and after that we append and output result
 * basically same logic goes to sales_info_dp
 * same logic for sales_info_simple too
 */


EXPLAIN SELECT category, COUNT(*) FROM sales_info GROUP BY category;
EXPLAIN SELECT category, COUNT(*) FROM sales_info_dp GROUP BY category;
EXPLAIN SELECT category, COUNT(*) FROM sales_info_simple GROUP BY category;


/* in sales_info we use parallel seq scans on partitioned tables then use parallel append after that we use group key which is category
 * then partial hashaggregate than it is sorted by category and then merged,after that than we use group by and finalize groupaggregate
 * same logic goes for sales_info_dp and sales_info_simple
 */


EXPLAIN 
SELECT COUNT(*) 
FROM sales_info si
JOIN sales_info_dp sidp ON si.id = sidp.id
WHERE si.eventdate = '2022-05-28';

/* first table is sales_info and we parallel sequence scan on its partitioned tables than join algorithm is hash join and we use 
 * parallel hash for that. after that we use parallel scan for all the partitioned tables of sales_info_dp and after that use 
 * parallel append finally after that we join them on the hash condition using parallel hash join then we gather information and
 * give the output. count is 5 002 131 
 */


--3
CREATE INDEX idx_sales_info_eventdate ON sales_info_dp(eventdate);
CREATE INDEX idx_sales_info_dp_eventdate ON sales_info_dp(eventdate);

/*this query still gives the same answer maybe because since the table is too large it prefers to use sequence scan algorithm
other than other algorithms same goes for other queries too.*/
EXPLAIN SELECT * FROM sales_info WHERE eventdate BETWEEN '2022-01-01' AND '2022-12-31';
EXPLAIN SELECT * FROM sales_info_dp WHERE eventdate BETWEEN '2022-01-01' AND '2022-12-31';
EXPLAIN SELECT * FROM sales_info_simple WHERE eventdate BETWEEN '2022-01-01' AND '2022-12-31';



