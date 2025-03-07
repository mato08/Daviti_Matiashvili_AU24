create sequence if not exists  bl_dm.dim_address_sequence start 1;
create sequence if not exists  bl_dm.dim_customers_sequence start 1;
create sequence if not exists  bl_dm.dim_employees_scd_sequence start 1;
create sequence if not exists  bl_dm.dim_products_sequence start 1;
create sequence if not exists  bl_dm.dim_stores_sequence start 1;




CREATE OR REPLACE FUNCTION bl_cl.fn_get_dim_store_data()
RETURNS TABLE (
    store_src_id 	varchar,
    store_name 	  varchar,
    source_system VARCHAR,
    source_entity VARCHAR
) 
security definer
AS $$
BEGIN
    RETURN QUERY
    WITH source_data AS (
        SELECT 
			cs.store_id as store_src_id,
			cs.store_name,
			cs.source_system,
			cs.source_entity
	from bl_3nf.ce_stores  cs
),deduplicated_data AS (
        SELECT 
			sd.store_src_id,
			sd.store_name,
            sd.source_system,
            sd.source_entity,
            ROW_NUMBER() OVER (
                PARTITION BY sd.store_src_id
                ORDER BY sd.source_system
            ) AS row_num
        FROM source_data sd
    )
    SELECT 
		dd.store_src_id::varchar,
		dd.store_name,
        dd.source_system,
        dd.source_entity
    FROM deduplicated_data dd
    WHERE dd.row_num = 1;
END;
$$ LANGUAGE plpgsql;

select * from bl_cl.fn_get_dim_store_data()







CREATE OR REPLACE PROCEDURE bl_cl.load_dim_stores()
LANGUAGE plpgsql AS $$
DECLARE
    rec RECORD;
    affected_rows INT;
    store_cursor CURSOR FOR
        SELECT 
            fs.store_src_id,
            fs.store_name,
            fs.source_system,
            fs.source_entity
        FROM bl_cl.fn_get_dim_store_data() fs;
BEGIN
    -- Insert default row
    INSERT INTO bl_dm.dim_stores (
        store_surr_id, store_src_id, store_name, 
        insert_dt, update_dt, source_system, source_entity
    )
    VALUES (
        -1, 'n.a', 'n.a', 
        '1900-01-01'::TIMESTAMP, '1900-01-01'::TIMESTAMP, 'MANUAL', 'MANUAL'
    )
    ON CONFLICT (store_surr_id) DO NOTHING;

    -- Loop through store data
    FOR rec IN store_cursor LOOP
        BEGIN
            -- Check if store exists
            IF EXISTS (
                SELECT 1 FROM bl_dm.dim_stores dd
                WHERE dd.store_src_id = rec.store_src_id  
                AND dd.source_system = rec.source_system
                AND dd.source_entity = rec.source_entity
            ) THEN
                -- Update store record if name has changed
                UPDATE bl_dm.dim_stores dd
                SET store_name = rec.store_name,
                    update_dt = NOW()
                WHERE dd.store_src_id = rec.store_src_id
                AND dd.source_system = rec.source_system
                AND dd.source_entity = rec.source_entity
                AND dd.store_name <> rec.store_name;
			 END IF;
                -- Insert new store record
                INSERT INTO bl_dm.dim_stores (
                    store_surr_id, store_src_id, store_name,
                    insert_dt, update_dt, source_system, source_entity
                )
                select 
                    nextval('bl_dm.dim_stores_sequence'),
                    rec.store_src_id,
                    rec.store_name,
                    NOW(),
                    NOW(),
                    rec.source_system,
                    rec.source_entity
				where not exists(
				SELECT 1 FROM bl_dm.dim_stores dd
                WHERE dd.store_src_id = rec.store_src_id  
                AND dd.source_system = rec.source_system
                AND dd.source_entity = rec.source_entity);

        EXCEPTION WHEN OTHERS THEN
            CALL bl_cl.log_procedure(
                'load_dim_stores', 
                NULL, 
                'Error inserting store '  || ': ' || SQLERRM, 
                'ERROR'
            );
        END;
    END LOOP;

    -- Get number of affected rows
    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    -- Log success
    CALL bl_cl.log_procedure(
        'load_dim_stores', 
        affected_rows, 
        'Procedure completed successfully.',  
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.log_procedure(
        'load_dim_stores', 
        NULL, 
        'Fatal error: ' || SQLERRM, 
        'ERROR'
    );
    RAISE;
END;
$$;







--now inserting into dim_address table
CREATE OR REPLACE PROCEDURE bl_cl.load_dim_addresses()
LANGUAGE plpgsql AS $$
DECLARE 
    affected_rows INT := 0;
BEGIN
    -- Insert default record if it doesn't exist
    INSERT INTO bl_dm.dim_addresses(
        address_surr_id, address_src_id, city_id, city_name, country_id, country_name,
        state_id, state_name, postal_code, insert_dt, update_dt,
        source_system, source_entity
    )
    SELECT 
        -1, 'n.a', -1, 'n.a', -1, 'n.a', -1, 'n.a', 'n.a',
        '1900-01-01'::TIMESTAMP, '1900-01-01'::TIMESTAMP, 'MANUAL', 'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.dim_addresses da WHERE da.address_surr_id = -1
    );

    -- Step 1: Create Temporary Table for source_data
    CREATE TEMP TABLE if not exists temp_source_data AS 
    SELECT
        ca.address_id AS address_src_id,
        cc.city_id,
        cc.city_name,
        cc.postal_code,
        cc1.country_id,
        cc1.country_name,
        cs.state_id,
        cs.state_name,
        ca.source_system,
        ca.source_entity
    FROM bl_3nf.ce_address ca
    JOIN bl_3nf.ce_cities cc ON ca.city_id = cc.city_id
    JOIN bl_3nf.ce_countries cc1 ON ca.country_id = cc1.country_id
    JOIN bl_3nf.ce_states cs ON ca.state_id = cs.state_id;

    -- Step 2: Create Temporary Table for deduplicated_data
    CREATE TEMP TABLE if not exists temp_deduplicated_data AS 
    SELECT
        address_src_id::varchar,
        city_id,
        city_name,
        postal_code,
        country_id,
        country_name,
        state_id,
        state_name,
        source_system,
        source_entity,
        ROW_NUMBER() OVER(PARTITION BY address_src_id ORDER BY source_system) AS row_num
    FROM temp_source_data;

    -- Step 3: Update existing records
    UPDATE bl_dm.dim_addresses da
    SET 
        city_name = dd.city_name,
        postal_code = dd.postal_code,
        country_name = dd.country_name,
        state_name = dd.state_name,
        update_dt = NOW()
    FROM temp_deduplicated_data dd
    WHERE da.address_src_id = dd.address_src_id
    AND da.source_system = dd.source_system
    AND da.source_entity = dd.source_entity
    AND dd.row_num = 1
    AND (
        da.city_name <> dd.city_name
        OR da.postal_code <> dd.postal_code
        OR da.country_name <> dd.country_name
        OR da.state_name <> dd.state_name
    );

    -- Step 4: Insert new records
    INSERT INTO bl_dm.dim_addresses(
        address_surr_id, address_src_id, city_id, city_name, country_id, country_name,
        state_id, state_name, postal_code, insert_dt, update_dt,
        source_system, source_entity
    )
    SELECT 
        nextval('bl_dm.dim_address_sequence'),
        dd.address_src_id,
        dd.city_id,
        dd.city_name,
        dd.country_id,
        dd.country_name,
        dd.state_id,
        dd.state_name,
        dd.postal_code,
        NOW(),
        NOW(),
        dd.source_system,
        dd.source_entity
    FROM temp_deduplicated_data dd
    WHERE dd.row_num = 1
    AND NOT EXISTS (
        SELECT 1 FROM bl_dm.dim_addresses da
        WHERE da.address_src_id = dd.address_src_id
        AND da.source_system = dd.source_system
        AND da.source_entity = dd.source_entity
    );

    -- Get affected row count
    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    -- Log the procedure execution
    CALL bl_cl.log_procedure(
        'load_dim_addresses',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

    -- Drop temporary tables at the end
    DROP TABLE IF EXISTS temp_source_data;
    DROP TABLE IF EXISTS temp_deduplicated_data;

EXCEPTION 
    WHEN OTHERS THEN
        -- Ensure temp tables are dropped even on error
        DROP TABLE IF EXISTS temp_source_data;
        DROP TABLE IF EXISTS temp_deduplicated_data;

        -- Log error and re-raise it
        CALL bl_cl.log_procedure(
            'load_dim_addresses',
            NULL,
            'Fatal error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;




create or replace procedure bl_cl.load_dim_dates()
language plpgsql as $$
DECLARE 
    start_date DATE := '2023-01-01';
    end_date DATE := '2027-12-31';
    date_cursor DATE := start_date;
	affected_rows int :=0;
BEGIN 

		insert into bl_dm.dim_date (date_id,year_no,quarter_no,month_no,month_name,week_no,day_no,day_name,is_weekend,insert_dt,update_dt)
					select 
							'1900-1-1'::date,
							-1,
							-1,
							-1,
							'n,a',
							-1,
							-1,
							'n,a',
							'Y',
							'1900-1-1'::timestamp,
							'1900-1-1'::timestamp
					where not exists(
					select 1 
					from bl_dm.dim_date d
					where d.date_id='1900-1-1'::date);



    WHILE date_cursor <= end_date LOOP 
        INSERT INTO bl_dm.DIM_DATE (DATE_ID, YEAR_NO, QUARTER_NO, MONTH_NO, MONTH_NAME, WEEK_NO, DAY_NO, DAY_NAME, IS_WEEKEND, INSERT_DT, UPDATE_DT)
        VALUES (
            date_cursor, 
            EXTRACT(YEAR FROM date_cursor), 
            EXTRACT(QUARTER FROM date_cursor), 
            EXTRACT(MONTH FROM date_cursor), 
            TO_CHAR(date_cursor, 'Month'),  
            EXTRACT(WEEK FROM date_cursor), 
            EXTRACT(DAY FROM date_cursor), 
            TRIM(TO_CHAR(date_cursor, 'Day')), 
            CASE WHEN EXTRACT(ISODOW FROM date_cursor) IN (6, 7) THEN TRUE ELSE FALSE END,
            now(),  
            now()  
        )
        ON CONFLICT (DATE_ID) DO UPDATE 
        SET UPDATE_DT = now();  

        date_cursor := date_cursor + INTERVAL '1 day'; 
		
	END LOOP;
    CALL bl_cl.log_procedure(
        'load_dim_dates',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

	GET DIAGNOSTICS affected_rows = ROW_COUNT;
    
END $$;








CREATE OR REPLACE PROCEDURE bl_cl.dim_customers()
LANGUAGE plpgsql AS $$
DECLARE
    affected_rows INT := 0;
    rec bl_dm.dim_customers%ROWTYPE; -- Composite type for customer records
BEGIN
    -- Ensure the default record exists
    EXECUTE
    'INSERT INTO bl_dm.dim_customers (customer_surr_id, customer_src_id,customer_name,customer_surname,segment, insert_dt, update_dt, source_system, source_entity)
    SELECT -1, ''n,a'', ''n,a'', ''1900-01-01''::TIMESTAMP without time zone, ''n,a'', now(), now(), ''MANUAL'', ''MANUAL''
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.dim_customers WHERE customer_surr_id = -1
    )';

    -- Create first temp table
    EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS customers_data AS (
        SELECT
            customer_id as customer_src_id,
			customer_name,
			customer_surname,
            segment,
            source_system,
            source_system AS source_entity
        FROM bl_3nf.ce_customers
    )';

    -- Create second temp table
    EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS customers_data1 AS (
        SELECT
            cd.customer_src_id::varchar,
			cd.customer_name,
			cd.customer_surname,
            cd.segment,
            cd.source_system,
            cd.source_entity,
            ROW_NUMBER() OVER (PARTITION BY cd.customer_src_id ORDER BY cd.source_system) AS row_num
        FROM customers_data cd
    )';

EXECUTE 'UPDATE bl_dm.dim_customers cc
    SET segment = cd.segment,
        update_dt = now()
    FROM customers_data1 cd
    WHERE cc.customer_src_id = cd.customer_src_id
    AND cc.source_entity = cd.source_entity
    AND cc.source_system = cd.source_system
    AND cc.segment <> cd.segment';


    -- Create third temp table
    EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS customers_data2 AS (
        SELECT
			nextval(''bl_dm.dim_customers_sequence''),
            customer_src_id AS customer_src_id,
			customer_name,
			customer_surname,
            segment AS segment,
            now() AS insert_dt,
            now() AS update_dt,
            source_system ,
            source_entity
        FROM customers_data1
        WHERE row_num = 1
        AND NOT EXISTS (
            SELECT 1 FROM bl_dm.dim_customers cc
            WHERE customers_data1.customer_src_id = cc.customer_src_id
            AND customers_data1.source_system = cc.source_system
            AND customers_data1.source_entity = cc.source_entity
        )
    )';

    -- Update existing records
    
    -- Insert new records from customers_data2
    FOR rec IN EXECUTE 'SELECT * FROM customers_data2' LOOP
        INSERT INTO bl_dm.dim_customers VALUES (rec.*);
        affected_rows := affected_rows + 1;
    END LOOP;

    -- Drop temporary tables
    EXECUTE 'DROP TABLE IF EXISTS customers_data, customers_data1, customers_data2';

    -- Log success
    CALL bl_cl.log_procedure(
        'load_dim_customers',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Drop temporary tables even in case of error
        EXECUTE 'DROP TABLE IF EXISTS customers_data, customers_data1, customers_data2';
        
        -- Log error
        CALL bl_cl.log_procedure(
            'load_dim_customers',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$;


 




create or replace procedure bl_cl.dim_products()
language plpgsql as $$
declare 
	affected_rows int :=0;
begin
		insert into bl_dm.dim_products (product_surr_id,product_src_id,category,sub_category,product_name,insert_dt,update_dt,source_system,source_entity)
			select -1,
				   'n,a',
				   'n,a',
				   'n,a',
				   'n,a',
				   '1-1-1900'::timestamp,
				   '1-1-1900'::timestamp,
				   'MANUAL',
				   'MANUAL'
			where not exists(
			select 1 from 
			bl_dm.dim_products p
			where p.product_surr_id=-1);
		
	
create temp table if not exists product_data as(
	select 
			product_id as product_src_id,
			category,
			sub_category,
			product_name,
			source_system,
			source_entity
		from bl_3nf.ce_products
);


create  temp table if not exists product_data1 as(
	select 
			pd.product_src_id::varchar,
			pd.category,
			pd.sub_category,
			pd.product_name,
			pd.source_system,
			pd.source_entity,
		row_number() over(partition by pd.product_src_id order by pd.source_system ) as row_num
	 from product_data pd
);

UPDATE bl_dm.dim_products cp
    SET
        product_name = pd1.product_name,
        category = pd1.category,
        sub_category = pd1.sub_category,
        update_dt = NOW()
    FROM product_data1 pd1
    WHERE cp.product_src_id = pd1.product_src_id
    AND cp.source_entity = pd1.source_entity
    AND cp.source_system = pd1.source_system
    AND (cp.product_name <> pd1.product_name
    OR cp.category <> pd1.category
    OR cp.sub_category <> pd1.sub_category);


insert into bl_dm.dim_products(product_surr_id,product_src_id,category,sub_category,product_name,
									insert_dt,update_dt,source_system,source_entity)
			  select 
			  		nextval('bl_dm.dim_products_sequence'),
			  		 product_src_id,
			  		 category,
			  		 sub_category,
			  		 product_name,
			  		 now(),
					 now(),
					 source_system,
					 source_entity
			 from product_data1
			 where row_num=1
			 and not exists(
		    SELECT 1 FROM bl_dm.dim_products cp
		   where product_data1.product_src_id=cp.product_src_id
			 and  product_data1.category=cp.category
			 and product_data1.sub_category=cp.sub_category
			 and product_data1.product_name=cp.product_name
			 and product_data1.source_system=cp.source_system
			 and product_data1.source_entity=cp.source_entity
);
			
			get diagnostics affected_rows :=row_count;
		
		CALL bl_cl.log_procedure(
        'load_dm_products',
         affected_rows,
        'Procedure completed successfully',
        'INFO'
    );


	drop table if exists product_data;
	drop table if exists product_data1;


EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_dm_products',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
end;
$$;




CREATE OR REPLACE PROCEDURE bl_cl.dim_employees()
LANGUAGE plpgsql AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    INSERT INTO bl_dm.dim_employees_scd(
        employee_surr_id, employee_src_id, employee_name, employee_surname,
        start_dt, end_dt, is_active,role, insert_dt, update_dt, source_entity, source_system
    )
    SELECT -1, 'n,a', 'n,a', 'n,a',
           '1900-01-01'::timestamp, '9999-12-31'::timestamp, 'Y', 'n,a',
           '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.dim_employees_scd e WHERE e.employee_surr_id = -1
    );


create temp table if not exists employees_data as(
		select 
			employee_id::varchar as employee_src_id,
			employee_name,
			employee_surname,
			role,
			start_date,
			end_date,
			is_active,
			source_entity,
			source_system
		from bl_3nf.ce_employees_scd
);
    	

    -- Step 2: Identify Existing Active Records That Need to Be Updated
    UPDATE bl_dm.dim_employees_scd ces
    SET 
        is_active = 'N',
        end_dt = NOW(),
        update_dt = NOW()
    FROM employees_data ed
    WHERE ces.employee_src_id = ed.employee_src_id
        AND ces.source_system = ed.source_system
        AND ces.source_entity = ed.source_entity
        AND ces.role <> ed.role
        AND ces.is_active = 'Y'; 

  
    INSERT INTO bl_dm.dim_employees_scd (
        employee_surr_id, employee_src_id, employee_name, employee_surname, start_dt, end_dt, 
        is_active, role, insert_dt, update_dt, source_entity, source_system
    )
    SELECT 
        nextval('bl_dm.dim_employees_scd_sequence'),
        ed.employee_src_id, 
		ed.employee_name, 
		ed.employee_surname,
        NOW(), 
		'9999-12-31'::timestamp, 
		'Y', 
        ed.role, 
		NOW(), 
		NOW(), 
        ed.source_entity, 
		ed.source_system
    FROM employees_data ed
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.dim_employees_scd e
        WHERE ed.employee_src_id = e.employee_src_id
          AND ed.source_system = e.source_system
          AND ed.source_entity = e.source_entity
          AND e.is_active = 'Y'
    );

    -- Get the number of affected rows
    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    -- Drop Temporary Tables
    DROP TABLE IF EXISTS employees_data;


    -- Log procedure execution
    CALL bl_cl.log_procedure(
        'load_dim_employees_scd',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_dim_employees_scd',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$;










create or replace procedure  bl_cl.fct_orders_dd()
language plpgsql as $$
declare 
	rows_affected integer :=0;
	max_date date;
begin
	insert into bl_dm.fct_orders_dd (order_surr_id,order_src_id,customer_surr_id,employee_surr_id,store_surr_id,address_surr_id,
								 product_surr_id,date_id,order_date,ship_date,ship_mode,insert_dt,
							      source_system,source_entity,price,quantity,discount,profit,price_without_discount)
							      
			select 
					-1,
					'n,a',
					-1,
					-1,
					-1,
					-1,
					-1,
					'1-1-1900'::date,
					'1-1-1900'::date,
					'1-1-1900'::date,
					'n,a',
					now(),
					'MANUAL',
					'MANUAL',
					-1,
					-1,
					-1,
					-1,
					-1
			where not exists(
			select 1 
			from bl_dm.fct_orders_dd o
			where  o.order_surr_id=-1);



create temp table if not exists order_data as(
			select 
				order_id,
				address_id,
				order_date,
				ship_date,
				ship_mode,
				product_id,
				store_id,
				customer_id,
				employee_id,
				source_system,
				source_entity,
				insert_dt,
				price,
				quantity,
				discount,
				profit,
				price_without_discount
			from bl_3nf.ce_orders
);

select max(order_date) from order_data
into max_date ;--2025-03-03

if max_date<'2025-03-04' then		
	insert into bl_dm.fct_orders_dd (order_surr_id,order_src_id,customer_surr_id,employee_surr_id,store_surr_id,address_surr_id,
								 product_surr_id,date_id,order_date,ship_date,ship_mode,insert_dt,
							      source_system,source_entity,price,quantity,discount,profit,price_without_discount)
SELECT 
    nextval('bl_dm.fct_orders_sequence'),
	coalesce(order_data.order_id::varchar,'n,a'),
	coalesce(dc.customer_surr_id,-1),
    coalesce(des.employee_surr_id,-1),
	coalesce(ds.store_surr_id, -1),
	coalesce(da.address_surr_id,-1),
	coalesce(dp.product_surr_id,-1),
	coalesce(dd.date_id,'1-1-1900'::date),
    order_data.order_date,
    order_data.ship_date,
	order_data.ship_mode,
	now(),
	order_data.source_system,
	order_data.source_entity,
	order_data.price,
	order_data.quantity,
	order_data.discount,
	order_data.profit,
	order_data.price_without_discount
FROM order_data
LEFT JOIN bl_dm.fct_orders_dd fod ON order_data.order_id::varchar= fod.order_src_id
left join bl_dm.dim_date dd on order_data.order_date=dd.date_id
left join bl_dm.dim_customers dc  on order_data.customer_id::varchar=dc.customer_src_id
left join bl_dm.dim_addresses da on order_data.address_id::varchar=da.address_src_id
left join bl_dm.dim_stores ds on order_data.store_id::varchar=ds.store_src_id
left join bl_dm.dim_employees_scd des on order_data.employee_id::varchar=des.employee_src_id
left join bl_dm.dim_products dp on order_data.product_id::varchar=dp.product_src_id
WHERE dd.date_id BETWEEN ('2025-03-03'::date - INTERVAL '6 months') AND '2025-03-03'::date
AND NOT EXISTS (
    SELECT 1
    FROM bl_dm.fct_orders_dd df
    WHERE order_data.order_id::varchar = df.order_src_id::varchar
    AND order_data.source_system = df.source_system
    AND order_data.source_entity = df.source_entity
);

else 
			alter table bl_dm.fct_orders_dd detach partition  bl_dm.fct_orders_2025_future;


		insert into  bl_dm.fct_orders_2025_future(order_surr_id,order_src_id,customer_surr_id,employee_surr_id,store_surr_id,address_surr_id,
								 product_surr_id,date_id,order_date,ship_date,ship_mode,insert_dt,
							      source_system,source_entity,price,quantity,discount,profit,price_without_discount)
SELECT 
    nextval('bl_dm.fct_orders_sequence'),
	coalesce(order_data.order_id::varchar,'n,a'),
	coalesce(dc.customer_surr_id,-1),
    coalesce(des.employee_surr_id,-1),
	coalesce(ds.store_surr_id, -1),
	coalesce(da.address_surr_id,-1),
	coalesce(dp.product_surr_id,-1),
	coalesce(dd.date_id,'1-1-1900'::date),
    order_data.order_date,
    order_data.ship_date,
	order_data.ship_mode,
	now(),
	order_data.source_system,
	order_data.source_entity,
	order_data.price,
	order_data.quantity,
	order_data.discount,
	order_data.profit,
	order_data.price_without_discount
FROM order_data
LEFT JOIN bl_dm.fct_orders_dd fod ON order_data.order_id::varchar= fod.order_src_id
left join bl_dm.dim_dates dd on order_data.order_date=dd.date_id
left join bl_dm.dim_customers dc  on order_data.customer_id::varchar=dc.customer_src_id
left join bl_dm.dim_addresses da on order_data.address_id::varchar=da.address_src_id
left join bl_dm.dim_stores ds on order_data.store_id::varchar=ds.store_src_id
left join bl_dm.dim_employees_scd des on order_data.employee_id::varchar=des.employee_src_id
left join bl_dm.dim_products dp on order_data.product_id::varchar=dp.product_src_id
WHERE dd.date_id BETWEEN ('2025-03-03'::date - INTERVAL '6 months') AND '2025-03-03'::date
AND NOT EXISTS (
    SELECT 1
    FROM bl_dm.fct_orders_dd df
    WHERE order_data.order_id::varchar = df.order_src_id::varchar
    AND order_data.source_system = df.source_system
    AND order_data.source_entity = df.source_entity
);



ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2025_future
FOR VALUES FROM ('2025-03-04') TO (maxvalue);



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









