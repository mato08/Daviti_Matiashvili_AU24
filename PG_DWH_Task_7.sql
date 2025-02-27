create schema if not exists bl_cl;
create sequence if not exists BL_3nf.stores_id_seq start 1;
create sequence if not exists BL_3nf.customers_id_seq start 1;
create sequence if not exists BL_3nf.address_id_seq start 1;
create sequence if not exists BL_3nf.products_id_seq start 1;
create sequence if not exists BL_3nf.orders_id_seq start 1;
create sequence if not exists BL_3nf.employees_id_seq start 1;
create sequence if not exists BL_3nf.states_id_seq start 1;
create sequence if not exists BL_3nf.cities_id_seq start 1;
create sequence if not exists BL_3nf.countries_id_seq start 1;

CREATE ROLE bl_cl_role;

--Grant full control on schemas
GRANT ALL ON SCHEMA bl_cl, bl_3nf, sa_offline_sales_schema, sa_online_sales_schema TO bl_cl_role;

-- Step 3: Grant full control on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bl_cl, bl_3nf, sa_offline_sales_schema, sa_online_sales_schema TO bl_cl_role;

-- Step 4: Grant full control on sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bl_3nf TO bl_cl_role;

-- Step 5: Grant execute permission on procedures/functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_cl, bl_3nf, sa_offline_sales_schema, sa_online_sales_schema TO bl_cl_role;



CREATE OR REPLACE PROCEDURE load_ext_online_sales_data()  
LANGUAGE plpgsql  
AS $$
BEGIN
   drop foreign table if exists sa_online_sales_schema.ext_online_sales;

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
        address_id VARCHAR,
		employee_role varchar,
		gender varchar,
		birth_date varchar
    )
    SERVER online_sales_external_server
    OPTIONS (
        FILENAME 'C:\Users\matia\Desktop\online_sales_data_updated.csv',  
        FORMAT 'csv',
        HEADER 'true'
);

   drop table if  exists sa_online_sales_schema.src_online_sales;

create table if not exists sa_online_sales_schema.src_online_sales(
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
        address_id VARCHAR,
		employee_role varchar,
		gender varchar,
		birth_date varchar
);

    -- Insert data while handling type conversions and avoiding duplicates
    INSERT INTO sa_online_sales_schema.src_online_sales (
        Row_ID, Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, 
        Segment, Country_Region, City, State_Province, Postal_Code, Region, Product_ID,
        Category, Sub_Category, Product_Name, Sales, Quantity, Discount, Profit, 
        Employee_ID, Order_Type, Delivery_Method, Payment_Method, Year, Quarter, Month, 
        Month_Name, Week, Day, Day_Name, Is_Weekend, Employee_Name, Employee_Surname, 
        store_id, store_name, address_id,employee_role,gender,birth_date
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
        Employee_Name, Employee_Surname, store_id, store_name, address_id,employee_role,gender,birth_date
    FROM sa_online_sales_schema.ext_online_sales eos
    WHERE NOT EXISTS (
        SELECT 1 FROM sa_online_sales_schema.src_online_sales sos WHERE sos.Row_ID = eos.Row_ID
    );
   

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error instead of just raising it
        RAISE NOTICE 'Error in load_ext_online_sales_data: %', SQLERRM;
        ROLLBACK;
END;
$$;



call load_ext_online_sales_data();
select * from sa_online_sales_schema.src_online_sales sos 




CREATE OR REPLACE PROCEDURE bl_cl.load_ext_offline_sales_data()  
LANGUAGE plpgsql  
AS $$  
BEGIN  
drop foreign table if exists sa_offline_sales_schema.ext_offline_sales;

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
		store_name varchar,
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
        address_id VARCHAR,
		employee_role varchar,
		gender varchar,
		birth_date varchar
    )  
    SERVER offline_sales_external_server  
    OPTIONS (  
        FILENAME 'C:\Users\matia\Desktop\offline_sales_data_updated.csv',
        FORMAT 'csv',  
        HEADER 'true'  
);  

drop table if exists sa_offline_sales_schema.src_offline_sales;


create table if not exists sa_offline_sales_schema.src_offline_sales(
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
		store_name varchar,
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
        address_id VARCHAR,
		employee_role varchar,
		gender varchar,
		birth_date varchar
);
    -- Insert data into src_offline_sales while handling type conversions and avoiding duplicates  
    INSERT INTO sa_offline_sales_schema.src_offline_sales (  
        row_id, order_id, order_date, ship_date, ship_mode, customer_id, customer_name,  
        segment, country_region, city, state_province, postal_code, region, product_id,  
        category, sub_category, product_name, sales, quantity, discount, profit,  
        employee_id, order_type,store_name, cashier_representative, payment_method,  
        "year", quarter, "month", month_name, week, "day", day_name,  
        is_weekend, employee_name, employee_surname, store_id, address_id,employee_role,gender,birth_date 
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
        employee_id, order_type,store_name, cashier_representative, payment_method,  
        "year", quarter, "month",  
        month_name, week, "day", day_name, is_weekend,  
        employee_name, employee_surname, store_id, address_id,employee_role,gender,birth_date
    FROM sa_offline_sales_schema.ext_offline_sales eos  
    WHERE NOT EXISTS (  
        SELECT 1 FROM sa_offline_sales_schema.src_offline_sales sos WHERE sos.row_id = eos.row_id  
    );  


EXCEPTION  
    WHEN OTHERS THEN  
        -- Log the error instead of just raising it  
        RAISE NOTICE 'Error in load_ext_offline_sales_data: %', SQLERRM;  
        ROLLBACK;  
END;  
$$;  

call bl_cl.load_ext_offline_sales_data();
select * from sa_offline_sales_schema.src_offline_sales sos 



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




--inserting into ce_countries
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_countries()
LANGUAGE plpgsql AS $$
DECLARE 
    affected_rows INTEGER;
BEGIN
    -- Insert default row if not exists
    INSERT INTO bl_3nf.ce_countries 
    (country_id, country_name, country_src_id, insert_dt, update_dt, source_system, source_entity)
    SELECT -1, 'n.a', 'n.a', '1900-01-01'::TIMESTAMP, '1900-01-01'::TIMESTAMP, 'MANUAL', 'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_countries WHERE country_id = -1
    );

    -- Create temporary table with source data
    CREATE TEMP TABLE IF NOT EXISTS source_data AS
    SELECT 
        DISTINCT country_region, 
        'sa_offline_sales' AS source_system,
        'src_offline_sales' AS source_entity
    FROM sa_offline_sales_schema.src_offline_sales 
    WHERE country_region IS NOT NULL
    
    UNION
    
    SELECT 
        DISTINCT country_region,
        'sa_online_sales' AS source_system,
        'src_online_sales' AS source_entity
    FROM sa_online_sales_schema.src_online_sales 
    WHERE country_region IS NOT NULL;

    -- Update existing records with changed names
    UPDATE bl_3nf.ce_countries cc
    SET 
        country_name = src.country_region,
        update_dt = NOW()
    FROM source_data src
    WHERE cc.country_src_id = src.country_region
      AND cc.source_system = src.source_system
      AND cc.source_entity = src.source_entity
      AND cc.country_name <> src.country_region;


    WITH final_data AS (
        SELECT 
            country_region,
            source_system,
            source_entity,
            ROW_NUMBER() OVER (
                PARTITION BY country_region 
                ORDER BY source_system
            ) AS row_num
        FROM source_data
    )
    INSERT INTO bl_3nf.ce_countries 
        (country_id, country_name, country_src_id, insert_dt, update_dt, source_system, source_entity)
    SELECT 
        NEXTVAL('bl_3nf.countries_id_seq'),
        COALESCE(country_region, 'n.a'),
        COALESCE(country_region, 'n.a'),
        NOW(),
        NOW(),
        COALESCE(source_system, 'MANUAL'),
        COALESCE(source_entity, 'MANUAL')
    FROM final_data
    WHERE row_num = 1
      AND NOT EXISTS (
          SELECT 1 
          FROM bl_3nf.ce_countries cc
          WHERE LOWER(cc.country_src_id) = LOWER(final_data.country_region)
            AND LOWER(cc.source_system) = LOWER(final_data.source_system)
            AND LOWER(cc.source_entity) = LOWER(final_data.source_entity)
      );

    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    -- Log procedure call
    CALL bl_cl.log_procedure(
        'load_ce_countries',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );
    
    DROP TABLE IF EXISTS source_data;
END;
$$;


call bl_cl.load_ce_countries();
select * from bl_3nf.ce_countries cc;
select * from bl_cl.logs;

/* i changed country_name to tbilisi in the source i was just checking if it was working correctly and it works. it updates
 * but since my country_src_id and country_name have same country_src id is also updated. I will update my data.
 */







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


create temp table if not exists source_data(
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
)


 UPDATE bl_3nf.ce_states cs
    SET 
        state_name = src.state_province,
        update_dt = NOW()
    FROM source_data src
    WHERE cs.state_src_id = src.state_province
      AND cs.source_system = src.source_system
      AND cs.source_entity = src.source_entity
      AND cs.state_name <> src.state_province;


with state_data as(
		select 
			  src1.state_province,
			  src1.country_region,
			  src1.source_system,
			  src1.source_entity,
	    row_number() OVER (PARTITION BY src1.state_province ORDER BY src1.source_system) AS row_num
    FROM source_data src
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

--changing some row in the offline dataset.
call bl_cl.load_ext_offline_sales_data();
call bl_cl.load_ce_states();
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

create temp table if not exists source_data1(
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
)

 UPDATE bl_3nf.ce_cities cc
    SET 
        cc.city_name= src1.city,
        update_dt = NOW()
    FROM source_data1 src1
    WHERE cc.city_src_id = src1.city
      AND cc.source_system = src1.source_system
      AND cc.source_entity = src1.source_entity
      AND cc.city_name <> src1.city;

with city_data as(
		select 
			 src1.city,
			 src1.state_province,
			 src1.source_system,
			 src1.source_entity,
			 src1.postal_code,
		row_number() over(partition by src1.city order by src1.source_system) as row_num
		from source_data1 src1
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

--now change something in dataset and check again
call bl_cl.load_ext_offline_sales_data();
call bl_cl.load_ce_cities();
select * from bl_cl.logs;




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
	

create temp table if not exists source_data4 as(
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
);









with addresses_data as(
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
		
create temp table if not exists source_data3(
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
)


--updating products

update bl_3nf.ce_products cp
set  cp.product_name=src3.product_name,
cp.category=src3.category,
cp.sub_category=src3.sub_category,
update_dt=now()
where cp.product_src_id=src3.product_id 
and cp.source_entity=src3.source_entity 
and cp.source_system=src3.source_system
and (cp.product_name<>src3.product_name 
or cp.category<>src3.category,
or cp.sub_category<>src3.sub_category);
		
					  


with  product_data as(
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


--now after some changes
call bl_cl.load_ext_offline_sales_data();
call bl_cl.load_ce_product();
select * from bl_cl.logs l 





--inserting into table stores
ALTER TABLE bl_3nf.ce_stores 
ADD CONSTRAINT unique_store_src 
UNIQUE (store_src_id, source_system, source_entity);

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
            IF EXISTS (
                SELECT 1 FROM bl_3nf.ce_stores cs
                WHERE cs.store_src_id = rec.store_id
                AND cs.source_system = rec.source_system
                AND cs.source_entity = rec.source_entity
            ) THEN
                -- Update store record if necessary
                UPDATE bl_3nf.ce_stores cs
                SET store_name = rec.store_name,
                    address_id = rec.address_id,
                    update_dt = NOW()
                WHERE cs.store_src_id = rec.store_id
                AND cs.source_system = rec.source_system
                AND cs.source_entity = rec.source_entity
                AND (cs.store_name <> rec.store_name
);
                

                GET DIAGNOSTICS _affected_rows = ROW_COUNT;
                IF _affected_rows > 0 THEN
                    CALL bl_cl.log_procedure(
                        'load_ce_stores_scd1', 
                        _affected_rows, 
                        'Updated store: ' || rec.store_id, 
                        'INFO'
                    );
                END IF;
            ELSE
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
                        'load_ce_stores_scd1', 
                        _affected_rows, 
                        'Inserted store: ' || rec.store_id, 
                        'INFO'
                    );
                END IF;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            CALL bl_cl.log_procedure(
                'load_ce_stores_scd1', 
                NULL, 
                'Error processing store ' || rec.store_id || ': ' || SQLERRM, 
                'ERROR'
            );
        END;
    END LOOP;

    -- Log procedure completion

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.log_procedure('load_ce_stores_scd1', NULL, 'Fatal error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;


call bl_cl.load_ce_stores()
select count(*) from bl_3nf.ce_stores cs 
select * from bl_cl.logs l 



--inserting into employees

CREATE OR REPLACE PROCEDURE bl_cl.load_ce_employees_scd()
LANGUAGE plpgsql AS $$
DECLARE 
    affected_rows INTEGER;
BEGIN
    INSERT INTO bl_3nf.ce_employees_scd(
        employee_id, employee_src_id, employee_name, employee_surname,role, store_id,
        start_date, end_date, is_active, insert_dt, update_dt, source_entity, source_system
    )
    SELECT -1, 'n,a', 'n,a', 'n,a','n,a', -1, 
           '1900-01-01'::timestamp, '9999-12-31'::timestamp, 'Y', 
           '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_employees_scd e WHERE e.employee_id = -1
    );


    CREATE TEMP TABLE IF NOT EXISTS source_data8 AS (
        SELECT 
            employee_id, employee_name, employee_surname, employee_role, store_id,
            'sa_online_sales' AS source_system, 'src_online_sales' AS source_entity
        FROM sa_online_sales_schema.src_online_sales 
        WHERE employee_id IS NOT NULL
        UNION
        SELECT 
            employee_id, employee_name, employee_surname, employee_role, store_id,
            'sa_offline_sales' AS source_system, 'src_offline_sales' AS source_entity
        FROM sa_offline_sales_schema.src_offline_sales 
        WHERE employee_id IS NOT NULL
    );


    UPDATE bl_3nf.ce_employees_scd ces
    SET 
        is_active = 'N',
        end_date = NOW(),
        update_dt = NOW()
    FROM source_data8 src8
    WHERE ces.employee_src_id = src8.employee_id
        AND ces.source_system = src8.source_system
        AND ces.source_entity = src8.source_entity
        AND (ces.employee_surname <> src8.employee_surname OR ces.role <> src8.employee_role)
        AND ces.is_active = 'Y';  

    WITH employee_data AS (
        SELECT 
            src8.employee_id, src8.employee_name, src8.employee_surname, src8.employee_role, src8.store_id, 
            src8.source_system, src8.source_entity,
            ROW_NUMBER() OVER (PARTITION BY src8.employee_id ORDER BY src8.source_system) AS row_num
        FROM source_data8 src8
    )
    INSERT INTO bl_3nf.ce_employees_scd(
        employee_id, employee_src_id, employee_name, employee_surname, role, store_id, 
        start_date, end_date, is_active, insert_dt, update_dt, source_entity, source_system
    )
    SELECT 
        NEXTVAL('BL_3NF.employees_id_seq'), 
        COALESCE(employee_data.employee_id, 'n,a'),
        COALESCE(employee_data.employee_name, 'n,a'),
        COALESCE(employee_data.employee_surname, 'n,a'),
        COALESCE(employee_data.employee_role, 'n,a'),  
        COALESCE(cs.store_id, -1),
        NOW(),
        '9999-12-31'::timestamp, 
        'Y',
        NOW(),
        NOW(),
        COALESCE(employee_data.source_entity, 'MANUAL'),
        COALESCE(employee_data.source_system, 'MANUAL')
    FROM employee_data
    LEFT JOIN bl_3nf.ce_stores cs ON employee_data.store_id = cs.store_src_id
    WHERE row_num = 1
    AND NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_employees_scd ces
        WHERE employee_data.employee_id = ces.employee_src_id
        AND employee_data.source_system = ces.source_system
        AND employee_data.source_entity = ces.source_entity
        AND ces.is_active = 'Y' 
    );


    GET DIAGNOSTICS affected_rows = ROW_COUNT;
	


    CALL bl_cl.log_procedure(
        'load_ce_employees_scd',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );
	
	drop table if exists source_data8;

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.log_procedure(
            'load_ce_employees_scd',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$;


call bl_cl.load_ce_employees_scd();
select * from bl_3nf.ce_employees_scd ces 
select * from bl_cl.logs l 

--after changing dataset
call bl_cl.load_ext_offline_sales_data();
call bl_cl.load_ce_employees_scd();





--inserting into customers table
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_customers()
LANGUAGE plpgsql AS $$
DECLARE 
    affected_rows INTEGER;
BEGIN
    -- Insert default customer if it does not exist
    INSERT INTO bl_3nf.ce_customers (customer_id, segment,date_of_birth,gender, customer_src_id, address_id, insert_dt, update_dt, source_system, source_entity)
    SELECT -1, 'n,a','1900-01-01'::date,'n,a', 'n,a', -1, '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_customers c WHERE c.customer_id = -1
    );

    -- Use a temporary table inside the procedure correctly
    CREATE TEMP TABLE  if not exists  source_data7 AS 
    (
        SELECT 
            customer_id,
            segment,
			birth_date,
			gender,
            address_id,
            'sa_online_sales' AS source_system,
            'src_online_sales' AS source_entity
        FROM sa_online_sales_schema.src_online_sales
        WHERE customer_id IS NOT NULL

        UNION

        SELECT 
            customer_id,
            segment,
			birth_date,
			gender,
            address_id,
            'sa_offline_sales' AS source_system,
            'src_offline_sales' AS source_entity
        FROM sa_offline_sales_schema.src_offline_sales
        WHERE customer_id IS NOT NULL
    );


    -- Update customer segments
    UPDATE bl_3nf.ce_customers cc
    SET segment = src7.segment,
		update_dt=now()
    FROM source_data7 src7
    WHERE cc.customer_src_id = src7.customer_id
      AND cc.source_entity = src7.source_entity
      AND cc.source_system = src7.source_system
      AND cc.segment <> src7.segment;

    -- Insert new customers
    WITH customers_data AS (
        SELECT 
            src7.customer_id,
            src7.segment,
			src7.birth_date,
			src7.gender,
            src7.address_id,
            src7.source_system,
            src7.source_entity,
            ROW_NUMBER() OVER (PARTITION BY src7.customer_id ORDER BY src7.source_system) AS row_num
        FROM source_data7 src7
    )
    INSERT INTO bl_3nf.ce_customers (customer_id, segment,date_of_birth,gender, customer_src_id, address_id, insert_dt, update_dt, source_system, source_entity)
    SELECT 
        nextval('BL_3NF.customers_id_seq'),
        COALESCE(customers_data.segment, 'n,a'),
		coalesce(customers_data.birth_date::date,'1900-01-01'::date),
		coalesce(customers_data.gender,'n,a'),
        COALESCE(customers_data.customer_id, 'n,a'),
        COALESCE(ca.address_id, -1),
        COALESCE(NOW(), '1900-01-01'::timestamp),
        COALESCE(NOW(), '1900-01-01'::timestamp),
        COALESCE(customers_data.source_system, 'MANUAL'),
        COALESCE(customers_data.source_entity, 'MANUAL')
    FROM customers_data
    LEFT JOIN bl_3nf.ce_addresses ca ON customers_data.address_id = ca.address_src_id
    WHERE row_num = 1
    AND NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_customers cc
        WHERE customers_data.customer_id = cc.customer_src_id
          AND customers_data.source_system = cc.source_system
          AND customers_data.source_entity = cc.source_entity
    );

    -- Get affected rows
    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    -- Log success
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
END;
$$;



call bl_cl.load_ce_customers();
select * from bl_3nf.ce_customers cc 
select * from bl_cl.logs l 

--now after changing the src data
call bl_cl.load_ext_offline_sales_data();
call bl_cl.load_ce_customers();
select * from bl_cl.logs l 






