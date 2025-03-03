--inserting into orders table


create table if not exists bl_3nf.ce_orders(
	order_id bigint not null,
	order_src_id varchar not null,
	order_type varchar not null,
	delivery_method  varchar not null,
	address_id bigint not null,
	order_dt varchar not null,
	ship_dt varchar not null,
	ship_mode varchar not null,
	product_id bigint not null,
	store_id bigint not null,
	customer_id bigint not null,
	employee_id bigint not null,
	payment_method varchar not null,
	source_system varchar not null,
	source_entity varchar not null,
	insert_dt timestamp not null,
	price decimal,
	quantity decimal,
    discount DECIMAL,
    profit decimal,
    event_date date,
	constraint ce_orders_pk primary key(order_id),
	constraint ce_orders_fk1 foreign key(store_id) references bl_3nf.ce_stores(store_id),
	constraint ce_orders_fk2 foreign key(customer_id) references bl_3nf.ce_customers(customer_id),
	constraint ce_orders_fk3 foreign key(employee_id) references bl_3nf.ce_employees_scd(employee_id),
	constraint ce_orders_fk4 foreign key(product_id) references bl_3nf.ce_products(product_id),
	constraint ce_orders_fk5 foreign key(address_id) references bl_3nf.ce_addresses(address_id)
);



CREATE OR REPLACE PROCEDURE bl_cl.load_ce_orders()
LANGUAGE plpgsql AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    -- Insert default row if it doesn't exist
    INSERT INTO bl_3nf.ce_orders (
        order_id, order_src_id, order_type, delivery_method, address_id, order_dt, ship_dt, ship_mode, product_id,
        store_id, customer_id, employee_id, payment_method, source_system, source_entity, insert_dt, price, quantity,
        discount, profit, event_date
    )
    SELECT 
        -1,
        'n,a',
        'n,a',
        'n,a',
        -1,
        '1900-01-01',
        '1900-01-01',
        'n,a',
        -1,
        -1,
        -1,
        -1,
        'n,a',
        'MANUAL',
        'MANUAL',
        '1900-01-01'::timestamp,
        -1,
        -1,
        -1,
        -1,
        '1900-01-01'::date
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_orders o
        WHERE o.order_id = -1
    );

    -- Process and insert data from source tables
    WITH source_data9 AS (
        SELECT 
            order_id,
            order_type,
            delivery_method,
            order_date,
            ship_date,
            ship_mode,
            store_id,
            customer_id,
            employee_id,
            payment_method,
            address_id,
            sales,
            quantity,
            discount,
            profit,
            product_id,
            -- Construct event_date as a valid date string (YYYY-MM-DD)
            TO_DATE(year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(day::text, 2, '0'), 'YYYY-MM-DD') AS event_date,
            'sa_online_sales' AS source_system,
            'src_online_sales' AS source_entity
        FROM sa_online_sales_schema.src_online_sales sos
        WHERE order_id IS NOT NULL AND
		TO_DATE(year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(day::text, 2, '0'), 'YYYY-MM-DD')>'2023-01-01'
        UNION
        SELECT 
            order_id,
            order_type,
            NULL,
            order_date,
            ship_date,
            ship_mode,
            store_id,
            customer_id,
            employee_id,
            payment_method,
            address_id,
            sales,
            quantity,
            discount,
            profit,
            product_id,
            -- Construct event_date as a valid date string (YYYY-MM-DD)
            TO_DATE(year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(day::text, 2, '0'), 'YYYY-MM-DD') AS event_date,
            'sa_offline_sales' AS source_system,
            'src_offline_sales' AS source_entity
        FROM sa_offline_sales_schema.src_offline_sales sos 
        WHERE order_id IS NOT NULL AND 
		TO_DATE(year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(day::text, 2, '0'), 'YYYY-MM-DD')>'2023-01-01'
    ), order_data AS (
        SELECT 
            src9.order_id,
            src9.order_type,
            src9.delivery_method,
            src9.order_date,
            src9.ship_date,
            src9.ship_mode,
            src9.store_id,
            src9.customer_id,
            src9.employee_id,
            src9.payment_method,
            src9.source_system,
            src9.source_entity,  
            src9.address_id,
            src9.sales,
            src9.quantity,
            src9.discount,
            src9.profit,
            src9.product_id,
            src9.event_date::date,
            ROW_NUMBER() OVER (PARTITION BY src9.order_id ORDER BY src9.source_system DESC) AS row_num
        FROM source_data9 src9
    )
    INSERT INTO bl_3nf.ce_orders (
        order_id, order_src_id, order_type, delivery_method, address_id, order_dt, ship_dt, ship_mode,
        product_id, store_id, customer_id, employee_id, payment_method, source_system, source_entity, insert_dt,
        price, quantity, discount, profit, event_date
    )
    SELECT 
        nextval('BL_3NF.orders_id_seq'),
        COALESCE(order_data.order_id, 'n,a'), 
        COALESCE(order_data.order_type, 'n,a'), 
        COALESCE(order_data.delivery_method, 'n,a'),
        COALESCE(ca.address_id, -1),
        COALESCE(order_data.order_date, '1900-01-01'), 
        COALESCE(order_data.ship_date, '1900-01-01'),
        COALESCE(order_data.ship_mode, 'n,a'), 
        COALESCE(cp.product_id, -1),
        COALESCE(cs.store_id, -1), 
        COALESCE(cc.customer_id, -1), 
        COALESCE(ec.employee_id, -1),
        COALESCE(order_data.payment_method, 'n,a'), 
        COALESCE(order_data.source_system, 'n,a'), 
        COALESCE(order_data.source_entity, 'n,a'), 
        NOW(),
        COALESCE(order_data.sales::numeric, -1),
        COALESCE(order_data.quantity::numeric, -1),
        COALESCE(order_data.discount::numeric, -1),
        COALESCE(order_data.profit::numeric, -1),
        COALESCE(order_data.event_date::date, '1900-01-01'::date)
    FROM order_data
    LEFT JOIN bl_3nf.ce_employees_scd ec ON order_data.employee_id = ec.employee_src_id
    LEFT JOIN bl_3nf.ce_stores cs ON order_data.store_id = cs.store_src_id
    LEFT JOIN bl_3nf.ce_customers cc ON order_data.customer_id = cc.customer_src_id
    LEFT JOIN bl_3nf.ce_addresses ca ON order_data.address_id = ca.address_src_id
    LEFT JOIN bl_3nf.ce_products cp ON order_data.product_id = cp.product_src_id
    WHERE row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_orders co
        WHERE order_data.order_id = co.order_src_id
        AND order_data.source_system = co.source_system
        AND order_data.source_entity = co.source_entity
    );

    -- Get the number of affected rows
    GET DIAGNOSTICS affected_rows := ROW_COUNT;

    -- Log the procedure execution
    CALL bl_cl.log_procedure(
        'load_ce_orders',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );


EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_orders',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$;

call bl_cl.load_ce_orders();
select * from bl_3nf.ce_orders co;
select * from bl_cl.logs l ;





create sequence if not exists bl_dm.fct_orders_sequence start 1

create table if not exists bl_dm.fct_orders_dd(
			orders_surr_id bigint not null,
			orders_src_id varchar not null,
			customer_surr_id bigint not null,
			employee_surr_id bigint not null,
			store_surr_id bigint not null,
			address_surr_id bigint not null,
			product_surr_id bigint not null,
			date_id date  not null,
			order_type varchar not null,
			order_dt varchar not null,
			ship_dt varchar not null,
			payment_method varchar not null,
			delivery_method varchar not null,
			ship_mode varchar not null,
			insert_dt timestamp not null,
			source_system varchar not null,
			source_entity varchar not null,
			price decimal not null,
			quantity int not null,
			discount decimal not null,
			profit_amt decimal not null,
			price_without_discount decimal not null,
			event_date date not null,
			constraint orders_pk primary key(orders_surr_id,event_date),
			constraint customer_fk foreign key(customer_surr_id) references bl_dm.dim_customers(customer_surr_id),
			constraint employee_fk foreign key(employee_surr_id) references bl_dm.dim_employees_scd(employee_surr_id),
			constraint store_fk  foreign key(store_surr_id) references bl_dm.dim_stores(store_surr_id),
			constraint address_fk foreign key(address_surr_id) references bl_dm.dim_addresses(address_surr_id),
			constraint products_fk foreign key(product_surr_id) references bl_dm.dim_products(product_surr_id),
			constraint date_fk foreign key(date_id) references bl_dm.dim_dates(date_id)
)partition by range(event_date);


CREATE TABLE bl_dm.fct_orders_historical1 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_historical1
FOR VALUES FROM (MINVALUE) TO ('2024-10-01');

select * from bl_dm.fct_orders_historical1


CREATE TABLE bl_dm.fct_orders_2024_10 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2024_10 
FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

select * from bl_dm.fct_orders_2024_10 

CREATE TABLE bl_dm.fct_orders_2024_11 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2024_11
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

select * from bl_dm.fct_orders_2024_11

CREATE TABLE bl_dm.fct_orders_2024_12 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2024_12
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');


select * from bl_dm.fct_orders_2024_12
CREATE TABLE bl_dm.fct_orders_2025 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2025
FOR VALUES FROM ('2025-01-01') TO ('2025-12-31');

select * from bl_dm.fct_orders_dd fod 





create or replace procedure  bl_cl.fct_orders_dd()
language plpgsql as $$
declare 
	rows_affected integer :=0;
	max_date date;
begin
	insert into bl_dm.fct_orders_dd (orders_surr_id,orders_src_id,customer_surr_id,employee_surr_id,store_surr_id,address_surr_id,
								 product_surr_id,date_id, order_type,order_dt,ship_dt,payment_method,delivery_method,ship_mode,insert_dt,
							      source_system,source_entity,price,quantity,discount,profit_amt,price_without_discount,event_date)
							      
			select 
					-1,
					'n,a',
					-1,
					-1,
					-1,
					-1,
					-1,
					'1-1-1900'::date,
					'n,a',
					'1-1-1900',
					'1-1-1900',
					'n,a',
					'n,a',
					'n,a',
					now(),
					'MANUAL',
					'MANUAL',
					-1,
					-1,
					-1,
					-1,
					-1,
					'1900-1-1'::date
			where not exists(
			select 1 
			from bl_dm.fct_orders_dd o
			where  o.orders_surr_id=-1);



create temp table if not exists order_data as(
			select 
				order_id,
				order_type,
				delivery_method,
				address_id,
				order_dt,
				ship_dt,
				ship_mode,
				product_id,
				store_id,
				customer_id,
				employee_id,
				payment_method,
				source_system,
				source_entity,
				insert_dt,
				price,
				quantity,
				discount,
				profit,
				event_date
			from bl_3nf.ce_orders
);

select max(event_date) from order_data
into max_date ;--2024-12-31

if max_date<'2025-01-01' then		
	insert into bl_dm.fct_orders_dd (orders_surr_id,orders_src_id,customer_surr_id,employee_surr_id,store_surr_id,address_surr_id,
								 product_surr_id,date_id, order_type,order_dt,ship_dt,payment_method,delivery_method,ship_mode,insert_dt,
							      source_system,source_entity,price,quantity,discount,profit_amt,price_without_discount,event_date)
SELECT 
    nextval('bl_dm.fct_orders_sequence'),
	coalesce(order_data.order_id::varchar,'n,a'),
	coalesce(dc.customer_surr_id,-1),
    coalesce(des.employee_surr_id,-1),
	coalesce(ds.store_surr_id, -1),
	coalesce(da.address_surr_id,-1),
	coalesce(dp.product_surr_id,-1),
	coalesce(dd.date_id,'1-1-1900'::date),
    order_data.order_type,
    order_data.order_dt,
    order_data.ship_dt,
	order_data.payment_method,
	order_data.delivery_method,
	order_data.ship_mode,
	now(),
	order_data.source_system,
	order_data.source_entity,
	order_data.price,
	order_data.quantity,
	order_data.discount,
	order_data.profit,
	order_data.profit+order_data.price,
	order_data.event_date
FROM order_data
LEFT JOIN bl_3nf.ce_orders co ON order_data.order_id= co.order_id
left join bl_dm.dim_dates dd on order_data.event_date=dd.date_id
left join bl_dm.dim_customers dc  on order_data.customer_id::varchar=dc.customer_src_id
left join bl_dm.dim_addresses da on order_data.address_id::varchar=da.address_src_id
left join bl_dm.dim_stores ds on order_data.store_id::varchar=ds.store_src_id
left join bl_dm.dim_employees_scd des on order_data.employee_id::varchar=des.employee_src_id
left join bl_dm.dim_products dp on order_data.product_id::varchar=dp.product_src_id
WHERE dd.date_id BETWEEN (CURRENT_DATE - INTERVAL '6 months') AND CURRENT_DATE
AND NOT EXISTS (
    SELECT 1
    FROM bl_dm.fct_orders_dd df
    WHERE order_data.order_id::varchar = df.orders_src_id::varchar
    AND order_data.source_system = df.source_system
    AND order_data.source_entity = df.source_entity
);

else 
			alter table bl_dm.fct_orders_dd detach partition bl_dm.fct_orders_2025;


		insert into  bl_dm.fct_orders_2023_q2(orders_surr_id,orders_src_id,customer_surr_id,employee_surr_id,store_surr_id,address_surr_id,
								 product_surr_id,date_id, order_type,order_dt,ship_dt,payment_method,delivery_method,ship_mode,insert_dt,
							      source_system,source_entity,price,quantity,discount,profit_amt,price_without_discount,event_date)
SELECT 
    nextval('bl_dm.fct_orders_sequence'),
	coalesce(order_data.order_id::varchar,'n,a'),
	coalesce(dc.customer_surr_id,-1),
    coalesce(des.employee_surr_id,-1),
	coalesce(ds.store_surr_id, -1),
	coalesce(da.address_surr_id,-1),
	coalesce(dp.product_surr_id,-1),
	coalesce(dd.date_id,'1-1-1900'::date),
    order_data.order_type,
    order_data.order_dt,
    order_data.ship_dt,
	order_data.payment_method,
	order_data.delivery_method,
	order_data.ship_mode,
	now(),
	order_data.source_system,
	order_data.source_entity,
	order_data.price,
	order_data.quantity,
	order_data.discount,
	order_data.profit,
	order_data.profit+order_data.price,
	order_data.event_date
FROM order_data
LEFT JOIN bl_3nf.ce_orders co ON order_data.order_id= co.order_id
left join bl_dm.dim_dates dd on order_data.event_date=dd.date_id
left join bl_dm.dim_customers dc  on order_data.customer_id::varchar=dc.customer_src_id
left join bl_dm.dim_addresses da on order_data.address_id::varchar=da.address_src_id
left join bl_dm.dim_stores ds on order_data.store_id::varchar=ds.store_src_id
left join bl_dm.dim_employees_scd des on order_data.employee_id::varchar=des.employee_src_id
left join bl_dm.dim_products dp on order_data.product_id::varchar=dp.product_src_id
WHERE dd.date_id BETWEEN (CURRENT_DATE - INTERVAL '6 months') AND CURRENT_DATE
AND NOT EXISTS (
    SELECT 1
    FROM bl_dm.fct_orders_dd df
    WHERE order_data.order_id::varchar = df.orders_src_id::varchar
    AND order_data.source_system = df.source_system
    AND order_data.source_entity = df.source_entity
);




ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2025
FOR VALUES FROM ('2025-01-01') TO ('2025-12-31');



			get diagnostics rows_affected:=row_count;
		
		CALL bl_cl.log_procedure(
        'load_bl_dm.fct_orders',
         rows_affected,
        'Procedure completed successfully',
        'INFO'
    );
end if;

	drop table if exists order_data;

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_fct_orders_dd',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;

end;
$$;




call bl_cl.fct_orders_dd();
select count(*) from bl_dm.fct_orders_dd fod 
select * from  
select * from bl_dm.dim_dates dd 
select * from bl_cl.logs 








