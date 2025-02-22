create schema if not exists bl_cl;


CREATE OR REPLACE PROCEDURE load_ext_online_sales_data()  
LANGUAGE plpgsql  
AS $$
BEGIN
    -- Check if the foreign table exists before attempting to drop
    IF EXISTS (SELECT 1 FROM information_schema.foreign_tables 
               WHERE foreign_table_schema = 'sa_online_sales_schema' 
               AND foreign_table_name = 'ext_online_sales') THEN
        DROP FOREIGN TABLE sa_online_sales_schema.ext_online_sales;
    END IF;

    -- Recreate the external table
    CREATE FOREIGN TABLE sa_online_sales_schema.ext_online_sales (
        Row_ID VARCHAR,
        Order_ID VARCHAR,
        Order_Date VARCHAR,  
        Ship_Date VARCHAR,   
        Ship_Mode VARCHAR,
        Customer_ID VARCHAR,
        Customer_Name VARCHAR,
        Segment VARCHAR,
        Country_Region VARCHAR,
        City VARCHAR,
        State_Province VARCHAR,
        Postal_Code VARCHAR,
        Region VARCHAR,
        Product_ID VARCHAR,
        Category VARCHAR,
        Sub_Category VARCHAR,
        Product_Name VARCHAR,
        Sales VARCHAR,    
        Quantity VARCHAR,  
        Discount VARCHAR,  
        Profit VARCHAR,   
        Employee_ID VARCHAR,
        Order_Type VARCHAR,
        Delivery_Method VARCHAR,
        Payment_Method VARCHAR,
        Year VARCHAR,     
        Quarter VARCHAR,  
        Month VARCHAR,    
        Month_Name VARCHAR,
        Week VARCHAR,     
        Day VARCHAR,      
        Day_Name VARCHAR,
        Is_Weekend VARCHAR,
        Employee_Name VARCHAR,
        Employee_Surname VARCHAR,
        store_id VARCHAR,
        store_name VARCHAR,
        address_id VARCHAR
    )
    SERVER online_sales_external_server
    OPTIONS (
        FILENAME 'C:\\Users\\matia\\Desktop\\online_sales.csv',  
        FORMAT 'csv',
        HEADER 'true'
    );

    -- Insert data while handling type conversions and avoiding duplicates
    INSERT INTO sa_online_sales_schema.src_online_sales (
        Row_ID, Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, 
        Segment, Country_Region, City, State_Province, Postal_Code, Region, Product_ID,
        Category, Sub_Category, Product_Name, Sales, Quantity, Discount, Profit, 
        Employee_ID, Order_Type, Delivery_Method, Payment_Method, Year, Quarter, Month, 
        Month_Name, Week, Day, Day_Name, Is_Weekend, Employee_Name, Employee_Surname, 
        store_id, store_name, address_id
    )
    SELECT 
        Row_ID, Order_ID, 
        Order_Date,  
        Ship_Date, 
        Ship_Mode, Customer_ID, Customer_Name, 
        Segment, Country_Region, City, State_Province, Postal_Code, Region, Product_ID, 
        Category, Sub_Category, Product_Name, 
        Sales,  
        Quantity,  
        Discount,  
        Profit,  
        Employee_ID, Order_Type, Delivery_Method, Payment_Method, 
        Year, Quarter, Month, 
        Month_Name, Week, Day, Day_Name, Is_Weekend, 
        Employee_Name, Employee_Surname, store_id, store_name, address_id
    FROM sa_online_sales_schema.ext_online_sales eos
    WHERE NOT EXISTS (
        SELECT 1 FROM sa_online_sales_schema.src_online_sales sos WHERE sos.Row_ID = eos.Row_ID
    );
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error instead of just raising it
        RAISE NOTICE 'Error in load_ext_online_sales_data: %', SQLERRM;
        ROLLBACK;
END;
$$;






CREATE OR REPLACE PROCEDURE load_ext_offline_sales_data()  
LANGUAGE plpgsql  
AS $$  
BEGIN  
    -- Check if the foreign table exists before attempting to drop  
    IF EXISTS (SELECT 1 FROM information_schema.foreign_tables  
               WHERE foreign_table_schema = 'sa_offline_sales_schema'  
               AND foreign_table_name = 'ext_offline_sales') THEN  
        DROP FOREIGN TABLE sa_offline_sales_schema.ext_offline_sales;  
    END IF;  

    -- Recreate the external table  
    CREATE FOREIGN TABLE sa_offline_sales_schema.ext_offline_sales (  
        row_id VARCHAR,  
        order_id VARCHAR,  
        order_date VARCHAR,  
        ship_date VARCHAR,  
        ship_mode VARCHAR,  
        customer_id VARCHAR,  
        customer_name VARCHAR,  
        segment VARCHAR,  
        country_region VARCHAR,  
        city VARCHAR,  
        state_province VARCHAR,  
        postal_code VARCHAR,  
        region VARCHAR,  
        product_id VARCHAR,  
        category VARCHAR,  
        sub_category VARCHAR,  
        product_name VARCHAR,  
        sales VARCHAR,  
        quantity VARCHAR,  
        discount VARCHAR,  
        profit VARCHAR,  
        employee_id VARCHAR,  
        order_type VARCHAR,  
        cashier_representative VARCHAR,  
        payment_method VARCHAR,  
        "year" VARCHAR,  
        quarter VARCHAR,  
        "month" VARCHAR,  
        month_name VARCHAR,  
        week VARCHAR,  
        "day" VARCHAR,  
        day_name VARCHAR,  
        is_weekend VARCHAR,  
        employee_name VARCHAR,  
        employee_surname VARCHAR,  
        store_id VARCHAR,  
        address_id VARCHAR  
    )  
    SERVER offline_sales_external_server  
    OPTIONS (  
        FILENAME 'C:\\Users\\matia\\Desktop\\offline_sales.csv',  -- Adjusted for offline file  
        FORMAT 'csv',  
        HEADER 'true'  
    );  

    -- Insert data into src_offline_sales while handling type conversions and avoiding duplicates  
    INSERT INTO sa_offline_sales_schema.src_offline_sales (  
        row_id, order_id, order_date, ship_date, ship_mode, customer_id, customer_name,  
        segment, country_region, city, state_province, postal_code, region, product_id,  
        category, sub_category, product_name, sales, quantity, discount, profit,  
        employee_id, order_type, cashier_representative, payment_method,  
        "year", quarter, "month", month_name, week, "day", day_name,  
        is_weekend, employee_name, employee_surname, store_id, address_id  
    )  
    SELECT  
        row_id, order_id,  
        order_date,  
        ship_date,  
        ship_mode, customer_id, customer_name,  
        segment, country_region, city, state_province, postal_code, region, product_id,  
        category, sub_category, product_name,  
        sales,  
        quantity,  
        discount,  
        profit,  
        employee_id, order_type, cashier_representative, payment_method,  
        "year", quarter, "month",  
        month_name, week::INT, "day", day_name, is_weekend,  
        employee_name, employee_surname, store_id, address_id  
    FROM sa_offline_sales_schema.ext_offline_sales eos  
    WHERE NOT EXISTS (  
        SELECT 1 FROM sa_offline_sales_schema.src_offline_sales sos WHERE sos.row_id = eos.row_id  
    );  

    -- Commit only if no errors occur  
    COMMIT;  

EXCEPTION  
    WHEN OTHERS THEN  
        -- Log the error instead of just raising it  
        RAISE NOTICE 'Error in load_ext_offline_sales_data: %', SQLERRM;  
        ROLLBACK;  
END;  
$$;  


--creating the log table
CREATE TABLE bl_cl.logs (
    log_id          SERIAL PRIMARY KEY,
    log_timestamp   TIMESTAMP DEFAULT NOW(),
    procedure_name  VARCHAR(255),
    rows_affected   INTEGER,
    log_message     TEXT,
    log_level       VARCHAR(10) CHECK (log_level IN ('INFO', 'WARNING', 'ERROR'))
);

--creating log_procedure to insert information of loggings from other tables in logs table
CREATE OR REPLACE PROCEDURE bl_cl.log_procedure(
    _procedure_name VARCHAR,
    _rows_affected INTEGER DEFAULT NULL,
    _log_message TEXT DEFAULT NULL,
    _log_level VARCHAR DEFAULT 'INFO'
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO bl_cl.logs (
        procedure_name,
        rows_affected,
        log_message,
        log_level
    ) VALUES (
        _procedure_name,
        _rows_affected,
        _log_message,
        _log_level
    );
END;
$$;





CREATE OR REPLACE procedure bl_cl.load_ce_countries()
	LANGUAGE plpgsql AS $$
	declare affected_rows integer;
	BEGIN
		
      
	    -- Insert default row if not exists
	    INSERT INTO bl_3nf.ce_countries 
	    (country_id, country_name, country_src_id, insert_dt, update_dt, source_system, source_entity)
	    SELECT -1, 'n,a', 'n,a', '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL'
	    WHERE NOT EXISTS (
	        SELECT 1 FROM bl_3nf.ce_countries WHERE country_id = -1
	    );
	
	    -- Main insert operation
	   with source_data as(
		select 
			distinct(country_region), 
			'sa_offline_sales' as source_system,
			'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
		where country_region is not null
		
		union
		
		select 
			distinct(country_region),
			'sa_online_sales' as source_system,
			'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
			where country_region is not null
	),
	final_data as(
		select 
			src.country_region,
			src.source_system,
			src.source_entity,
		 ROW_NUMBER() OVER (PARTITION BY src.country_region ORDER BY src.source_system) AS row_num
		from source_data src
	)
	insert into bl_3nf.ce_countries (country_id,country_name,country_src_id,insert_dt,update_dt,source_system,source_entity)
				select nextval('bl_3nf.countries_id_seq'),
				   coalesce(final_data.country_region,'n,a') as country_name,
				   coalesce(final_data.country_region,'n,a') as country_src_id,
				   now(),
				   now(),
				   coalesce(final_data.source_system,'MANUAL'),
				   coalesce(final_data.source_entity,'MANUAL')
			from final_data
			where row_num=1
		    and  not exists(
			select 1 
			from  bl_3nf.ce_countries cc
			where lower(cc.country_src_id)=lower(final_data.country_region)
			and  lower(cc.source_system)= lower(final_data.source_system)
			and lower(cc.source_entity)=lower(final_data.source_entity));


			 GET DIAGNOSTICS affected_rows = ROW_COUNT;
			 
			 CALL bl_cl.log_procedure(
        'load_ce_countries',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );
	END;
	$$;

call bl_cl.load_ce_countries();
select count(*) from bl_3nf.ce_countries cc 
select * from bl_cl.logs



--inserting into states

CREATE OR REPLACE PROCEDURE bl_cl.load_ce_states()
LANGUAGE plpgsql AS $$
declare affected_rows integer;
BEGIN
    -- Insert default row if missing
    INSERT INTO bl_3nf.ce_states 
        (state_id, state_src_id, state_name, insert_dt, update_dt, source_system, source_entity, country_id)
    SELECT 
        -1, 
        'n.a', 
        'n.a', 
        '1900-01-01'::TIMESTAMP, 
        '1900-01-01'::TIMESTAMP, 
        'MANUAL', 
        'MANUAL',
        -1
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_states WHERE state_id = -1
    );


with source_data1 as(
	select 
		  state_province,
		  country_region,
		  'sa_online_sales' as source_system,
		  'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
    where state_province is not null
	
	union
	
	select 
		  state_province,
		  country_region,
		  'sa_offline_sales' as source_system,
		  'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos   
	where state_province is not null
),
state_data as(
		select 
			  src1.state_province,
			  src1.country_region,
			  src1.source_system,
			  src1.source_entity,
	    row_number() OVER (PARTITION BY src1.state_province ORDER BY src1.source_system) AS row_num
    FROM source_data1 src1
)
insert into bl_3nf.ce_states(state_id,state_src_id,state_name,insert_dt,update_dt,source_system,source_entity,country_id)
		select
				nextval('BL_3NF.states_id_seq'),
				coalesce(state_data.state_province,'n,a'),
				coalesce(state_data.state_province,'n,a'),
				coalesce(now(),'1900-1-1'::timestamp),
				coalesce(now(),'1900-1-1'::timestamp),
				coalesce(state_data.source_system,'MANUAL'),
				coalesce(state_data.source_entity,'MANUAL'),
				coalesce(cc.country_id,-1)
				from state_data
				left join bl_3nf.ce_countries cc on state_data.country_region=cc.country_src_id
				where state_data.row_num=1
				and not exists(
				select 1
				from bl_3nf.ce_states cs
				where cs.state_src_id=state_data.state_province
				and cs.source_entity=state_data.source_entity
				and cs.source_system=state_data.source_system);

		 GET DIAGNOSTICS affected_rows = ROW_COUNT;
			 
			 CALL bl_cl.log_procedure(
        'load_ce_states',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

	end;
$$;



call bl_cl.load_ce_states();
select count(*) from bl_3nf.ce_states cs 
select * from bl_cl.logs












CREATE OR REPLACE PROCEDURE bl_cl.load_ce_cities()
LANGUAGE plpgsql AS $$
declare affected_rows integer;
BEGIN
    -- Insert default row if missing
    INSERT INTO bl_3nf.ce_cities 
        (city_id, city_name, city_src_id, insert_dt, update_dt, state_id, source_system, source_entity, postal_code)
    SELECT 
        -1, 
        'n.a', 
        'n.a', 
        '1900-01-01'::TIMESTAMP, 
        '1900-01-01'::TIMESTAMP, 
        -1, 
        'MANUAL', 
        'MANUAL',
        'n.a'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_cities WHERE city_id = -1
    );

    with source_data2 as(
		select 
			  city,
			  state_province,
			  'sa_online_sales' as source_system,
			  'src_online_sales' as source_entity,
			  postal_code 
		from sa_online_sales_schema.src_online_sales sos 
		where city is not null
		union
		
		select 
				city,
				state_province,
				'sa_offline_sales' as source_system,
				'src_offline_sales' as source_entity,
				postal_code
		from sa_offline_sales_schema.src_offline_sales sos 
 		where city is not null
),city_data as(
		select 
			 src2.city,
			 src2.state_province,
			 src2.source_system,
			 src2.source_entity,
			 src2.postal_code,
		row_number() over(partition by src2.city order by src2.source_system) as row_num
		from source_data2 src2
)
insert into bl_3nf.ce_cities(city_id,city_name,city_src_id,insert_dt,update_dt,
								state_id,source_system,source_entity,postal_code)
				
		    select nextval('BL_3NF.cities_id_seq'),
		    	   coalesce(city_data.city,'n,a'),
		    	   coalesce(city_data.city,'n,a'),
		    	   coalesce(now(),'1900-1-1'::timestamp),
		    	   coalesce(now(),'1900-1-1'::timestamp),
		    	   coalesce(cs.state_id,-1),
		    	   coalesce(city_data.source_system,'MANUAL'),
		    	   coalesce(city_data.source_entity,'MANUAL'),
		    	   coalesce(city_data.postal_code,'n,a')
		    from city_data 
		    left join bl_3nf.ce_states cs on cs.state_src_id=city_data.state_province
		    where row_num=1
		    and not exists(
		    select 1 
		    from bl_3nf.ce_cities cc
		    where city_data.city=cc.city_src_id
		    and city_data.source_system=cc.source_system
		    and city_data.source_entity=cc.source_entity);


			GET DIAGNOSTICS affected_rows = ROW_COUNT;


			 CALL bl_cl.log_procedure(
        'load_ce_cities',
         affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_cities',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;

END;
$$;


call bl_cl.load_ce_cities();
select count(*) from bl_3nf.ce_cities cc
select * from bl_cl.logs




--insert into addresses
create or replace procedure bl_cl.load_ce_addresses()
language plpgsql as $$
declare affected_rows integer;
begin 
	insert into bl_3nf.ce_addresses(address_id,city_id,country_id,state_id,insert_dt,update_dt,address_src_id,
								source_system,source_entity)
			select -1,
				   -1,
				   -1,
				   -1,
				   '1-1-1900'::timestamp,
				   '1-1-1900'::timestamp,
				   'n,a',
				   'MANUAL',
				   'MANUAL'
			where not exists(
			select 1 from
			bl_3nf.ce_addresses ca
			where ca.address_id=-1);
	
		with source_data4 as(
		select 
				city,
				country_region,
				state_province,
				address_id,
				'sa_online_sales' as source_system,
				'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
		where address_id is not null 

		union 
		select 
			  city,
			  country_region,
			  state_province,
			  address_id,
			  'sa_offline_sales' as source_system,
			  'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
		where address_id is not null
),addresses_data as(
		select 
			 src4.city,
			 src4.country_region,
			 src4.state_province,
			 src4.address_id,
			 src4.source_system,
			 src4.source_entity,
		row_number() over(partition by src4.city,src4.country_region,src4.state_province order by src4.source_system) as row_num
		from source_data4 src4
)insert into bl_3nf.ce_addresses(address_id,city_id,country_id,state_id,insert_dt,
								  update_dt,address_src_id,source_system,source_entity)
			 select nextval('BL_3NF.address_id_seq'),
			 		coalesce(cc.city_id,-1),
			 		coalesce(co.country_id,-1),
			 		coalesce(st.state_id,-1),
			 		now(),
			 		now(),
			 		coalesce(addresses_data.address_id,'n,a') as address_src_id,
			 		coalesce(addresses_data.source_system,'MANUAL'),
			 		coalesce(addresses_data.source_entity,'MANUAL')
			 from addresses_data
			 left join bl_3nf.ce_cities cc on  addresses_data.city=cc.city_src_id 
			 left join bl_3nf.ce_countries co on addresses_data.country_region=co.country_src_id 
			 left join bl_3nf.ce_states st on addresses_data.state_province=st.state_src_id 
			 where row_num=1
			 and not exists(
			 select 1
			 from bl_3nf.ce_addresses ca
			 where addresses_data.address_id=ca.address_src_id
			 and addresses_data.source_system=ca.source_system
			 and addresses_data.source_entity=ca.source_entity
			 );
			
			GET DIAGNOSTICS affected_rows = ROW_COUNT;
		
		 CALL bl_cl.log_procedure(
        'load_ce_addresses',
         affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_addresses',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;

end;
$$;


call bl_cl.load_ce_addresses();
select count(*) from bl_3nf.ce_addresses ca;
select * from bl_cl.logs;


--inserting into table product
create or replace procedure bl_cl.load_ce_product()
language plpgsql as $$
declare affected_rows integer;
begin
	insert into bl_3nf.ce_products (product_id,product_src_id,category,sub_category,product_name,insert_dt,update_dt,source_system,source_entity)
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
			bl_3nf.ce_products p
			where p.product_id=-1);
		
		with source_data3 as(
	select 
			product_id,
			category,
			sub_category,
			product_name,
			'sa_online_sales' as source_system,
			'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
	where product_id is not null and category is not null and sub_category is not null and product_name is not null
	
	union
			
	select 
			product_id,
			category,
			sub_category,
			product_name,
			'sa_offline_sales' as source_system,
			'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos
	where product_id is not null and category is not null and sub_category is not null and product_name is not null
	
), product_data as(
	select 
			src3.product_id,
			src3.category,
			src3.sub_category,
			src3.product_name,
			src3.source_system,
			src3.source_entity,
		row_number() over(partition by src3.product_id order by src3.source_system ) as row_num
	 from source_data3 src3
) insert into bl_3nf.ce_products(product_id,product_src_id,category,sub_category,product_name,
									insert_dt,update_dt,source_system,source_entity)
			  select nextval('BL_3NF.products_id_seq'),
			  		 coalesce(product_data.product_id,'n,a'),
			  		 coalesce(product_data.category,'n,a'),
			  		 coalesce(product_data.sub_category,'n,a'),
			  		 coalesce(product_data.product_name,'n,a'),
			  		 now(),
					 now(),
					 coalesce(product_data.source_system,'MANUAL'),
					 coalesce(product_data.source_entity,'MANUAL')
			 from product_data
			 where row_num=1
			 and not exists(
			 select 1 from
			 bl_3nf.ce_products cp
			 where product_data.product_id=cp.product_src_id
			 and  product_data.category=cp.category
			 and product_data.sub_category=cp.sub_category
			 and product_data.product_name=cp.product_name
			 and product_data.source_system=cp.source_system
			 and product_data.source_entity=cp.source_entity);
			
			get diagnostics affected_rows :=row_count;
		
		CALL bl_cl.log_procedure(
        'load_ce_products',
         affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_products',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
			
end;
$$;

call bl_cl.load_ce_product();
select count(*) from bl_3nf.ce_products cp 
select * from bl_cl.logs





--inserting into table stores


--function to load data from the sources
CREATE OR REPLACE FUNCTION bl_cl.fn_get_store_data()
RETURNS TABLE (
    store_id      VARCHAR,
    store_name 	  varchar,
    address_id    VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    WITH source_data AS (
        SELECT 
            sos.store_id AS store_id,
			sos.store_id as store_name,
            sos.address_id AS address_id,
            'sa_online_sales'::varchar AS source_system,
            'src_online_sales'::varchar AS source_entity
        FROM sa_online_sales_schema.src_online_sales sos
        WHERE sos.store_id IS NOT NULL
        
        UNION ALL
        
        SELECT 
            sos.store_id AS store_id,
			sos.store_id as store_name,
            sos.address_id AS address_id,
            'sa_offline_sales'::varchar AS source_system,
            'src_offline_sales'::varchar AS source_entity
        FROM sa_offline_sales_schema.src_offline_sales sos
        WHERE sos.store_id IS NOT NULL
    ),
    deduplicated_data AS (
        SELECT 
            sd.store_id,  
			sd.store_name,
            sd.address_id,
            sd.source_system,
            sd.source_entity,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(sd.store_id)  
                ORDER BY sd.source_system
            ) AS row_num
        FROM source_data sd
    )
    SELECT 
        dd.store_id, 
		dd.store_name,
        dd.address_id,
        dd.source_system,
        dd.source_entity
    FROM deduplicated_data dd
    WHERE dd.row_num = 1;
END;
$$ LANGUAGE plpgsql;

select * from bl_cl.fn_get_store_data()



CREATE OR REPLACE PROCEDURE bl_cl.load_ce_stores()
LANGUAGE plpgsql AS $$
DECLARE
    rec RECORD;
    _affected_rows INT := 0;
BEGIN
    -- Insert default row if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_id = -1) THEN
        INSERT INTO bl_3nf.ce_stores(
            store_id, store_src_id, store_name, address_id, 
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            -1, 
            'n.a', 
            'n.a', 
            -1, 
            '1900-01-01'::TIMESTAMP, 
            '1900-01-01'::TIMESTAMP, 
            'MANUAL', 
            'MANUAL'
        );
    END IF;

    -- Process stores using FOR LOOP
    FOR rec IN 
        SELECT 
            fs.store_id,
			fs.store_name,
            COALESCE(ca.address_id, -1) AS address_id,
            fs.source_system,
            fs.source_entity
        FROM bl_cl.fn_get_store_data() fs
        LEFT JOIN bl_3nf.ce_addresses ca 
            ON fs.address_id = ca.address_src_id
    LOOP
        BEGIN
            -- Check if the store already exists
            IF NOT EXISTS (
                SELECT 1 
                FROM bl_3nf.ce_stores cs
                WHERE cs.store_src_id = rec.store_id
                AND cs.source_system = rec.source_system
                AND cs.source_entity = rec.source_entity
            ) THEN
                -- Insert new store record
                INSERT INTO bl_3nf.ce_stores(
                    store_id, store_src_id, store_name, address_id,
                    insert_dt, update_dt, source_system, source_entity
                )
                VALUES (
                    nextval('bl_3nf.stores_id_seq'),
                    rec.store_id,
                    rec.store_name,
                    rec.address_id,
                    NOW(),
                    NOW(),
                    rec.source_system,
                    rec.source_entity
                );

                -- Log successful insertion
                GET DIAGNOSTICS _affected_rows = ROW_COUNT;
                IF _affected_rows > 0 THEN
                    CALL bl_cl.log_procedure(
                        'load_ce_stores', 
                        _affected_rows, 
                        'Inserted store: ' || rec.store_id, 
                        'INFO'
                    );
                END IF;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            -- Log errors
            CALL bl_cl.log_procedure(
                'load_ce_stores', 
                NULL, 
                'Error inserting store ' || rec.store_id || ': ' || SQLERRM, 
                'ERROR'
            );
        END;
    END LOOP;

    -- Log procedure completion
    CALL bl_cl.log_procedure('load_ce_stores', NULL, 'Procedure completed', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.log_procedure('load_ce_stores', NULL, 'Fatal error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

call bl_cl.load_ce_stores()
select count(*) from bl_3nf.ce_stores cs 
select * from bl_cl.logs l 



--inserting into employees

create or replace procedure bl_cl.load_ce_employees_scd()
language plpgsql as $$
declare affected_rows integer;
begin
	insert into bl_3nf.ce_employees_scd(employee_id,employee_src_id,employee_name,employee_surname,store_id,
									start_date,end_date,is_active,insert_dt,update_dt,source_entity ,source_system )
			select -1,
				   'n,a',
				   'n,a',
				   'n,a',
				   -1,
				   '1900-1-1'::timestamp,
				   '9999-12-31'::timestamp,
				   'Y',
				   '1-1-1900'::timestamp,
				   '1-1-1900'::timestamp,
				   'MANUAL',
				   'MANUAL'
			where not exists(
			select 1
			from bl_3nf.ce_employees_scd e
			where e.employee_id=-1);
		
	
	
	with source_data8 as(
	select 
			employee_id,
			employee_name,
			employee_surname,
			store_id,
			'sa_online_sales' as source_system,
			'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
	where employee_id is not null
	union
	
	select 
			employee_id,
			employee_name,
			employee_surname,
			store_id,
			'sa_offline_sales' as source_system,
			'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos 
	where employee_id  is not null
), employee_data as(
		select 
				src8.employee_id,
				src8.employee_name,
				src8.employee_surname,
				src8.store_id,
				src8.source_system,
				src8.source_entity,
				row_number() over (partition by src8.employee_id order by src8.source_system) as row_num 
		from source_data8 src8
)insert into bl_3nf.ce_employees_scd(employee_id,employee_src_id,employee_name,employee_surname,store_id,start_date,
									end_date,is_active,insert_dt,update_dt,source_entity,source_system
									)
			select 
					nextval('BL_3NF.employees_id_seq'),
					coalesce(employee_data.employee_id,'n,a'),
					coalesce(employee_data.employee_name,'n,a'),
					coalesce(employee_data.employee_surname,'n,a'),
					coalesce(cs.store_id,-1),
					now(),
					now(),
					'Y',
					coalesce(now(),'1900-1-1'::timestamp),
					coalesce(now(),'1900-1-1'::timestamp),
					coalesce(employee_data.source_entity,'MANUAL'),
					coalesce(employee_data.source_system,'MANUAL')
			 from employee_data
			 left join bl_3nf.ce_stores cs on employee_data.store_id=cs.store_src_id
			 where row_num=1
			 and not exists(
			 select 1
			 from bl_3nf.ce_employees_scd ces
			 where employee_data.employee_id=ces.employee_src_id
			 and  employee_data.source_system=ces.source_system
			 and employee_data.source_entity=ces.source_entity);
			
			get diagnostics affected_rows :=row_count;
		
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
		
end;
$$;

call bl_cl.load_ce_employees_scd();
select * from bl_3nf.ce_employees_scd ces 
select * from bl_cl.logs l 



--inserting into customers table

create or replace procedure bl_cl.load_ce_customers()
language plpgsql as $$
declare affected_rows integer;
begin
	insert into bl_3nf.ce_customers (customer_id,segment,customer_src_id,address_id,insert_dt,update_dt,source_system,source_entity)
			select -1,
				   'n,a',
				   'n,a',
				   -1,
				   '1-1-1900'::timestamp,
				   '1-1-1900'::timestamp,
				   'MANUAL',
				   'MANUAL'
			where not exists(
			select 1 
			from bl_3nf.ce_customers c
			where c.customer_id=-1);
		
	with source_data7 as(
		select 
				customer_id,
				segment,
				address_id,
				'sa_online_sales' as source_system,
				'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
		where customer_id is not null 
	    
		union
		
		select 
				customer_id,
				segment,
				address_id,
				'sa_offline_sales' as source_system,
				'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
		where customer_id is not null
) ,customers_data as(
		select 
				src7.customer_id,
				src7.segment,
				src7.address_id,
				src7.source_system,
				src7.source_entity,
				row_number() over(partition by src7.customer_id order by src7.source_system) as row_num
		from source_data7 as src7		   
)insert into bl_3nf.ce_customers(customer_id,segment,customer_src_id,address_id,insert_dt,update_dt,source_system,source_entity)
			select 
					nextval('BL_3NF.customers_id_seq'),
					coalesce(customers_data.segment,'n,a'),
					coalesce(customers_data.customer_id,'n,a'),
					coalesce(ca.address_id,-1),
				  coalesce(now(),'1900-1-1'::timestamp),
				  coalesce(now(),'1900-1-1'::timestamp),
				  coalesce(customers_data.source_system,'MANUAL'),
				  coalesce(customers_data.source_entity,'MANUAL')
			from customers_data
			left join bl_3nf.ce_addresses ca on customers_data.address_id=ca.address_src_id
			where row_num=1
			and not exists(
			select 1 
			from bl_3nf.ce_customers cc
			where customers_data.customer_id=cc.customer_src_id
			and  customers_data.source_system=cc.source_system
			and customers_data.source_entity=cc.source_entity);
		
		get diagnostics affected_rows:=row_count;
	
	CALL bl_cl.log_procedure(
        'load_ce_customers',
         affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_customers',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
end;
$$;

call bl_cl.load_ce_customers();
select * from bl_3nf.ce_customers cc 
select * from bl_cl.logs l 



--inserting into orders table

create or replace procedure bl_cl.load_ce_orders()
language plpgsql as $$
declare affected_rows integer;
begin
	insert into bl_3nf.ce_orders (order_id,order_src_id,order_type,delivery_method,order_dt,ship_dt,ship_mode,
							  store_id,customer_id,employee_id,payment_method,source_system,source_entity,insert_dt)
			select 
					-1,
					'n,a',
					'n,a',
					'n,a',
					'1-1-1900'::timestamp,
					'1-1-1900'::timestamp,
					'n,a',
					-1,
					-1,
					-1,
					'n,a',
					'MANUAL',
					'MANUAL',
					'1-1-1900'::timestamp
			where not exists(
			select 1 
			from bl_3nf.ce_orders o
			where  o.order_id=-1);
		
		
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
        'sa_online_sales' AS source_system,
        'src_online_sales' AS source_entity
    FROM sa_online_sales_schema.src_online_sales sos 
	where order_id is not null
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
        'sa_offline_sales' AS source_system,
        'src_offline_sales' AS source_entity
    FROM sa_offline_sales_schema.src_offline_sales sos 
    where order_id is not null
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
        ROW_NUMBER() OVER (PARTITION BY src9.order_id ORDER BY src9.source_system DESC) AS row_num
    FROM source_data9 src9
)INSERT INTO bl_3nf.ce_orders(
    order_id, order_src_id, order_type, delivery_method, order_dt, ship_dt, ship_mode,
    store_id, customer_id, employee_id, payment_method, source_system, source_entity, insert_dt
)
SELECT 
    nextval('BL_3NF.orders_id_seq'),
    COALESCE(order_data.order_id, 'n,a'), 
    COALESCE(order_data.order_type, 'n,a'), 
    COALESCE(order_data.delivery_method, 'n,a'),
    COALESCE(order_data.order_date, '1900-01-01'), 
    COALESCE(order_data.ship_date, '1900-01-01'),
    COALESCE(order_data.ship_mode, 'n,a'), 
    COALESCE(cs.store_id, -1), 
    COALESCE(cc.customer_id, -1), 
    COALESCE(ec.employee_id, -1),
    COALESCE(order_data.payment_method, 'n,a'), 
    COALESCE(order_data.source_system, 'n,a'), 
    COALESCE(order_data.source_entity, 'n,a'), 
    NOW()
FROM order_data
LEFT JOIN bl_3nf.ce_employees_scd ec ON order_data.employee_id = ec.employee_src_id
LEFT JOIN bl_3nf.ce_stores cs ON order_data.store_id = cs.store_src_id
LEFT JOIN bl_3nf.ce_customers cc ON order_data.customer_id = cc.customer_src_id
WHERE row_num = 1
AND NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_orders co
    WHERE order_data.order_id = co.order_src_id
    AND order_data.source_system = co.source_system
    AND order_data.source_entity = co.source_entity
);
		

			get diagnostics affected_rows:=row_count;
		
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
end;
$$;


call bl_cl.load_ce_orders();
select * from bl_3nf.ce_orders co ;
select * from bl_cl.logs l ;




--inserting into order_details

create or replace procedure bl_cl.load_ce_order_details()
language plpgsql as $$
declare affected_rows integer;
begin
	with source_data10 as(
		select 
				order_id,
				product_id,
				quantity,
				discount,
				profit,
				'sa_online_sales' as source_system,
				'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
		 where order_id is not null and product_id is not null
		union
		
		select 
				order_id,
				product_id,
				quantity,
				discount,
				profit,
				'sa_offline_sales' as source_system,
				'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
		where order_id is not null and product_id is not null
),order_dt as(
		select 
		src10.order_id,
		src10.product_id,
		src10.quantity,
		src10.discount,
		src10.profit,
		src10.source_system,
		src10.source_entity,
		row_number() over(partition by src10.order_id order by src10.source_system) as row_num 
		from source_data10 src10
)insert into bl_3nf.ce_order_details(order_id,product_id,quantity,discount,profit,insert_dt,
								update_dt,source_system,source_entity,order_details_src_id)
			select 
					coalesce(co.order_id,-1),
					coalesce(cp.product_id,-1),
					coalesce(order_dt.quantity::decimal,-1.0),
					coalesce(order_dt.discount::decimal,-1.0),
					coalesce(order_dt.profit::decimal,-1.0),
					coalesce(now(), '1900-1-1'::timestamp without time zone),
					coalesce(now(),'1900-1-1'::timestamp without time zone),
					coalesce(order_dt.source_system,'MANUAL'),
					coalesce(order_dt.source_entity,'MANUAL'),
					coalesce(order_dt.order_id|| order_dt.product_id,'n,a')
			from order_dt
			left join bl_3nf.ce_orders co on order_dt.order_id=co.order_src_id
			left join bl_3nf.ce_products cp on order_dt.product_id=cp.product_src_id
			where row_num=1
			and not exists(
			select 1
			from bl_3nf.ce_order_details od
			where order_dt.source_system=od.source_system
			and order_dt.source_entity=od.source_entity
			and order_dt.order_id||order_dt.product_id=od.order_details_src_id);
		
		get diagnostics affected_rows:=row_count;
		
		CALL bl_cl.log_procedure(
        'load_ce_order_details',
         affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ce_order_details',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
end;
$$;

call bl_cl.load_ce_order_details();
select * from bl_3nf.ce_order_details cod 
select * from bl_cl.logs l 




