
--task 3.1
create or replace function sh.get_total_sales_by_category(start_date date, end_date date)
returns table (
    product_category varchar,
    total_sales_amount numeric
)
language plpgsql
as $$
begin
    return query
    select 
        p.prod_category,
        sum(s.amount_sold) 
    from sh.sales s
    join sh.products p on s.prod_id = p.prod_id
    where s.time_id between start_date and end_date
    group by p.prod_category
    order by total_sales_amount desc;
end;
$$;


--task 3.2
create or replace function sh.get_avg_sales_quantity_by_region(product_id int)
returns table (
    region varchar,
    average_quantity numeric
) 
language plpgsql
as $$
begin
    return query
    select c2.country_region,avg(s.quantity_sold)    
    from sh.sales s    
    join sh.customers c on s.cust_id = c.cust_id
    join sh.countries c2 on c.country_id = c2.country_id
    where s.prod_id = product_id
    group by c2.country_region;
    
end;
$$;


--task 3.3
select concat(c.cust_first_name, ' ',c.cust_last_name) as full_name,
		sum(s.amount_sold) as total_sales
		from sh.sales s 
		join sh.customers c on s.cust_id =c.cust_id 
		group by c.cust_id 
		order by sum(s.amount_sold) desc 
		fetch first 5 rows with ties;
		
	



