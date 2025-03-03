create schema if not exists bl_cl;

CREATE ROLE bl_cl_role1;

GRANT  create on SCHEMA bl_cl TO bl_cl_role1;
grant usage on schema bl_cl,bl_dm,bl_3nf to bl_cl_role1;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_cl,bl_dm,bl_3nf TO bl_cl_role1;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA bl_cl,bl_dm,bl_3nf,sa_offline_sales_schema,sa_online_sales_schema TO bl_cl_role1;
GRANT all privileges ON ALL PROCEDURES IN SCHEMA bl_cl,bl_dm,bl_3nf TO bl_cl_role1;
GRANT all privileges ON SCHEMA bl_cl,bl_dm, bl_3nf TO bl_cl_role1;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bl_cl, bl_dm, bl_3nf TO bl_cl_role1;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bl_cl,bl_dm, bl_3nf TO bl_cl_role1;
GRANT all privileges ON ALL FUNCTIONS IN SCHEMA bl_cl, bl_3nf, sa_offline_sales_schema, sa_online_sales_schema TO bl_cl_role1;

	

set role bl_cl_role1;
reset role;

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
                WHERE dd.store_src_id = rec.store_src_id  -- FIXED: Incorrect reference
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
                VALUES (
                    nextval('bl_dm.dim_stores_sequence'),
                    rec.store_src_id,
                    rec.store_name,
                    NOW(),
                    NOW(),
                    rec.source_system,
                    rec.source_entity
                );

        EXCEPTION WHEN OTHERS THEN
            CALL bl_cl.log_procedure(
                'load_dim_stores', 
                NULL, 
                'Error inserting store ' || rec.store_surr_id || ': ' || SQLERRM, 
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
        'Procedure completed successfully.',  -- FIXED: Missing comma
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


call bl_cl.load_dim_stores();
select * from bl_dm.dim_stores ds;
select * from bl_cl.logs l;


--checking if it works correctly





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
    CREATE TEMP TABLE temp_source_data AS 
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
    FROM bl_3nf.ce_addresses ca
    JOIN bl_3nf.ce_cities cc ON ca.city_id = cc.city_id
    JOIN bl_3nf.ce_countries cc1 ON ca.country_id = cc1.country_id
    JOIN bl_3nf.ce_states cs ON ca.state_id = cs.state_id;

    -- Step 2: Create Temporary Table for deduplicated_data
    CREATE TEMP TABLE temp_deduplicated_data AS 
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
 




call bl_cl.load_dim_addresses();
select * from bl_dm.dim_addresses da;
select * from bl_cl.logs l;

--after changing data
call bl_cl.load_dim_addresses();
select * from bl_dm.dim_addresses da;
select * from bl_cl.logs l;


create or replace procedure bl_cl.load_dim_dates()
language plpgsql as $$
DECLARE 
    start_date DATE := '2023-01-01';
    end_date DATE := '2027-12-31';
    date_cursor DATE := start_date;
	affected_rows int :=0;
BEGIN 

		insert into bl_dm.dim_dates (date_surr_id,year_no,quarter_no,month_no,month_name,week_no,day_no,day_name,isweekend,insert_dt,update_dt)
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
					from bl_dm.dim_dates d
					where d.date_surr_id='1900-1-1'::date);



    WHILE date_cursor <= end_date LOOP 
        INSERT INTO bl_dm.DIM_DATES (DATE_SURR_ID, YEAR_NO, QUARTER_NO, MONTH_NO, MONTH_NAME, WEEK_NO, DAY_NO, DAY_NAME, ISWEEKEND, INSERT_DT, UPDATE_DT)
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
        ON CONFLICT (DATE_SURR_ID) DO UPDATE 
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


call bl_cl.load_dim_dates();
select * from bl_dm.dim_dates;
select * from bl_cl.logs l ;






CREATE OR REPLACE PROCEDURE bl_cl.dim_customers()
LANGUAGE plpgsql AS $$
DECLARE
    affected_rows INT := 0;
    rec bl_dm.dim_customers%ROWTYPE; -- Composite type for customer records
BEGIN
    -- Ensure the default record exists
    EXECUTE
    'INSERT INTO bl_dm.dim_customers (customer_surr_id, customer_src_id, segment, date_of_birth, gender, insert_dt, update_dt, source_system, source_entity)
    SELECT -1, ''n,a'', ''n,a'', ''1900-01-01''::TIMESTAMP without time zone, ''n,a'', now(), now(), ''MANUAL'', ''MANUAL''
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.dim_customers WHERE customer_surr_id = -1
    )';

    -- Create first temp table
    EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS customers_data AS (
        SELECT
            customer_id as customer_src_id,
            segment,
            date_of_birth,
            gender,
            source_system,
            source_system AS source_entity
        FROM bl_3nf.ce_customers
    )';

    -- Create second temp table
    EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS customers_data1 AS (
        SELECT
            cd.customer_src_id::varchar,
            cd.segment,
            cd.date_of_birth,
            cd.gender,
            cd.source_system,
            cd.source_entity,
            ROW_NUMBER() OVER (PARTITION BY cd.customer_src_id ORDER BY cd.source_system) AS row_num
        FROM customers_data cd
    )';

    -- Create third temp table
    EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS customers_data2 AS (
        SELECT
			nextval(''bl_dm.dim_customers_sequence''),
            customer_src_id AS customer_src_id,
            segment AS segment,
            COALESCE(date_of_birth::TIMESTAMP WITHOUT TIME ZONE, ''1900-01-01''::TIMESTAMP without time zone) AS date_of_birth,
            gender,
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
    EXECUTE 'UPDATE bl_dm.dim_customers cc
    SET segment = cd2.segment,
        update_dt = now()
    FROM customers_data2 cd2
    WHERE cc.customer_src_id = cd2.customer_src_id
    AND cc.source_entity = cd2.source_entity
    AND cc.source_system = cd2.source_system
    AND cc.segment <> cd2.segment';

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


call bl_cl.dim_customers();
select * from bl_dm.dim_customers dc ;
select * from bl_cl.logs l;




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



call bl_cl.dim_products();
select * from bl_dm.dim_products dp 
select * from bl_cl.logs l;



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
        AND (ces.employee_surname <> ed.employee_surname OR ces.role <> ed.role)
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
        'load_ce_employees_scd',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_employees_scd',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$;


call bl_cl.dim_employees();
select * from bl_dm.dim_employees_scd des 
select * from bl_cl.logs l






