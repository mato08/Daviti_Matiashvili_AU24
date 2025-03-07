

--creating log table 
--drop table if exists bl_cl.logs
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





CREATE OR REPLACE PROCEDURE bl_cl.main_load_ext_online_sales_data()  
LANGUAGE plpgsql  
AS $$
declare affected_rows integer := 0;
BEGIN
   drop foreign table if exists sa_online_sales_schema.ext_online_sales;

    -- Recreate the external table
    CREATE FOREIGN TABLE sa_online_sales_schema.ext_online_sales (
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
        Product_ID VARCHAR,
        Category VARCHAR,
        Sub_Category VARCHAR,
        Product_Name VARCHAR,
        Sales VARCHAR,    
        Quantity VARCHAR,  
        Discount VARCHAR,  
        Profit VARCHAR,   
        Employee_ID VARCHAR,
		store_name varchar,
        Employee_Name VARCHAR,
        Employee_Surname VARCHAR,
        store_id VARCHAR,
		employee_role varchar,
		customer_surname varchar,
		country_id varchar,
		state_id  varchar,
		city_id varchar,
		address_id varchar
    )
    SERVER online_sales_external_server
    OPTIONS (
        FILENAME 'C:\Users\matia\Desktop\online_trimmed_dataset.csv',  
        FORMAT 'csv',
        HEADER 'true'
);

   drop table if  exists sa_online_sales_schema.src_online_sales;

create table if not exists sa_online_sales_schema.src_online_sales(
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
        Product_ID VARCHAR,
        Category VARCHAR,
        Sub_Category VARCHAR,
        Product_Name VARCHAR,
        Sales VARCHAR,    
        Quantity VARCHAR,  
        Discount VARCHAR,  
        Profit VARCHAR,   
        Employee_ID VARCHAR,
		store_name varchar,
        Employee_Name VARCHAR,
        Employee_Surname VARCHAR,
        store_id VARCHAR,
		employee_role varchar,
		customer_surname varchar,
		country_id varchar,
		state_id  varchar,
		city_id varchar,
		address_id varchar
);

    -- Insert data while handling type conversions and avoiding duplicates
    INSERT INTO sa_online_sales_schema.src_online_sales (
      	Order_ID,
        Order_Date,  
        Ship_Date,   
        Ship_Mode,
        Customer_ID,
        Customer_Name,
        Segment,
        Country_Region,
        City,
        State_Province,
        Postal_Code,
        Product_ID,
        Category,
        Sub_Category,
        Product_Name,
        Sales,    
        Quantity,  
        Discount,  
        Profit,   
        Employee_Id,
		store_name,
        Employee_Name,
        Employee_Surname,
        store_id,
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id,
		address_id
    )
    SELECT 
        Order_ID,
        Order_Date,  
        Ship_Date,   
        Ship_Mode,
        Customer_ID,
        Customer_Name,
        Segment,
        Country_Region,
        City,
        State_Province,
        Postal_Code,
        Product_ID,
        Category,
        Sub_Category,
        Product_Name,
        Sales,    
        Quantity,  
        Discount,  
        Profit,   
        Employee_Id,
		store_name,
        Employee_Name,
        Employee_Surname,
        store_id,
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id,
		address_id
    FROM sa_online_sales_schema.ext_online_sales eos
    WHERE NOT EXISTS (
        SELECT 1 FROM sa_online_sales_schema.src_online_sales sos WHERE sos.order_ID = eos.order_ID
    );
   
get diagnostics affected_rows := row_count;

	
		 CALL bl_cl.log_procedure(
        'load_ext_offline and load_src_offline',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ext_online and load_src_online',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;

END;
$$;



select * from bl_cl.logs l 




CREATE OR REPLACE PROCEDURE bl_cl.main_load_ext_offline_sales_data()  
LANGUAGE plpgsql  
AS $$  
declare affected_rows integer := 0;
BEGIN  
drop foreign table if exists sa_offline_sales_schema.ext_offline_sales;

    -- Recreate the external table  
    CREATE FOREIGN TABLE sa_offline_sales_schema.ext_offline_sales (  	
        order_id VARCHAR,  
        order_date VARCHAR,  
        customer_id VARCHAR,  
        customer_name VARCHAR,  
        segment VARCHAR,  
        country_region VARCHAR,  
        city VARCHAR,  
        state_province VARCHAR,  
        postal_code VARCHAR,    
        product_id VARCHAR,  
        category VARCHAR,  
        sub_category VARCHAR,  
        product_name VARCHAR,  
        sales VARCHAR,  
        quantity VARCHAR,  
        discount VARCHAR,  
        profit VARCHAR,  
        employee_id VARCHAR,   
		store_name varchar,  
        employee_name VARCHAR, 
        employee_surname VARCHAR,  
		store_id VARCHAR, 
		employee_role varchar,
		customer_surname varchar,
		country_id varchar,
		state_id varchar,
		city_id varchar, 
        address_id VARCHAR
    )  
    SERVER offline_sales_external_server  
    OPTIONS (  
        FILENAME  'C:\Users\matia\Desktop\offline_sales_data.csv',
        FORMAT 'csv',  
        HEADER 'true'  
);  

drop table if exists sa_offline_sales_schema.src_offline_sales;


create table if not exists sa_offline_sales_schema.src_offline_sales(
        order_id VARCHAR,  
        order_date VARCHAR,  
        customer_id VARCHAR,  
        customer_name VARCHAR,  
        segment VARCHAR,  
        country_region VARCHAR,  
        city VARCHAR,  
        state_province VARCHAR,  
        postal_code VARCHAR,  
        product_id VARCHAR,  
        category VARCHAR,  
        sub_category VARCHAR,  
        product_name VARCHAR,  
        sales VARCHAR,  
        quantity VARCHAR,  
        discount VARCHAR,  
        profit VARCHAR,  
        employee_id VARCHAR,   
		store_name varchar, 
        employee_name VARCHAR, 
        employee_surname VARCHAR,  
		store_id VARCHAR, 
		employee_role varchar,
		customer_surname varchar,
		country_id varchar,
		state_id varchar,
		city_id varchar, 
        address_id VARCHAR
);
    -- Insert data into src_offline_sales while handling type conversions and avoiding duplicates  
    INSERT INTO sa_offline_sales_schema.src_offline_sales (      
		order_id,  
        order_date,   
        customer_id,  
        customer_name,  
        segment,  
        country_region,  
        city,  
        state_province,  
        postal_code,    
        product_id,  
        category,  
        sub_category,  
        product_name,  
        sales,  
        quantity,  
        discount,  
        profit,  
        employee_id,   
		store_name,
        employee_name, 
        employee_surname,  
		store_id, 
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id , 
        address_id 
      
    )  
    SELECT  
        order_id,  
        order_date,   
        customer_id,  
        customer_name,  
        segment,  
        country_region,  
        city,  
        state_province,  
        postal_code,    
        product_id,  
        category,  
        sub_category,  
        product_name,  
        sales,  
        quantity,  
        discount,  
        profit,  
        employee_id,   
		store_name,
        employee_name, 
        employee_surname,  
		store_id, 
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id , 
        address_id 
    FROM sa_offline_sales_schema.ext_offline_sales eos  
    WHERE NOT EXISTS (  
        SELECT 1 FROM sa_offline_sales_schema.src_offline_sales sos WHERE sos.order_id = eos.order_id  
    );  

		get diagnostics affected_rows := row_count;

	
		 CALL bl_cl.log_procedure(
        'load_ext_offline and load_src_offline',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ext_offline and load_src_offline',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;  
$$; 





CREATE OR REPLACE PROCEDURE bl_cl.incremental_load_ext_online_sales_data()
LANGUAGE plpgsql  
AS $$
declare affected_rows integer := 0;
BEGIN
   drop foreign table if exists sa_online_sales_schema.ext_online_sales2;

    -- Recreate the external table
    CREATE FOREIGN TABLE sa_online_sales_schema.ext_online_sales2 (
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
        Product_ID VARCHAR,
        Category VARCHAR,
        Sub_Category VARCHAR,
        Product_Name VARCHAR,
        Sales VARCHAR,    
        Quantity VARCHAR,  
        Discount VARCHAR,  
        Profit VARCHAR,   
        Employee_ID VARCHAR,
		store_name varchar,
        Employee_Name VARCHAR,
        Employee_Surname VARCHAR,
        store_id VARCHAR,
		employee_role varchar,
		customer_surname varchar,
		country_id varchar,
		state_id  varchar,
		city_id varchar,
		address_id varchar
    )
    SERVER online_sales_external_server
    OPTIONS (
        FILENAME 'C:\Users\matia\Desktop\online_sales_dataset_10000.csv',
        FORMAT 'csv',
        HEADER 'true'
);

  
    -- Insert data while handling type conversions and avoiding duplicates
    INSERT INTO sa_online_sales_schema.src_online_sales (
      	Order_ID,
        Order_Date,  
        Ship_Date,   
        Ship_Mode,
        Customer_ID,
        Customer_Name,
        Segment,
        Country_Region,
        City,
        State_Province,
        Postal_Code,
        Product_ID,
        Category,
        Sub_Category,
        Product_Name,
        Sales,    
        Quantity,  
        Discount,  
        Profit,   
        Employee_Id,
		store_name,
        Employee_Name,
        Employee_Surname,
        store_id,
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id,
		address_id
    )
    SELECT 
        Order_ID,
        Order_Date,  
        Ship_Date,   
        Ship_Mode,
        Customer_ID,
        Customer_Name,
        Segment,
        Country_Region,
        City,
        State_Province,
        Postal_Code,
        Product_ID,
        Category,
        Sub_Category,
        Product_Name,
        Sales,    
        Quantity,  
        Discount,  
        Profit,   
        Employee_Id,
		store_name,
        Employee_Name,
        Employee_Surname,
        store_id,
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id,
		address_id
    FROM sa_online_sales_schema.ext_online_sales2 eos
    WHERE NOT EXISTS (
        SELECT 1 FROM sa_online_sales_schema.src_online_sales sos WHERE sos.order_ID = eos.order_ID
    );
   
get diagnostics affected_rows := row_count;

	
		 CALL bl_cl.log_procedure(
        'load_ext_offline and load_src_offline',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ext_online and load_src_online',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;

END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.incremental_load_ext_offline_sales_data()  
LANGUAGE plpgsql  
AS $$  
declare affected_rows integer := 0;
BEGIN  
drop foreign table if exists sa_offline_sales_schema.ext_offline_sales2;

    -- Recreate the external table  
    CREATE FOREIGN TABLE sa_offline_sales_schema.ext_offline_sales2 (  	
        order_id VARCHAR,  
        order_date VARCHAR,  
        customer_id VARCHAR,  
        customer_name VARCHAR,  
        segment VARCHAR,  
        country_region VARCHAR,  
        city VARCHAR,  
        state_province VARCHAR,  
        postal_code VARCHAR,    
        product_id VARCHAR,  
        category VARCHAR,  
        sub_category VARCHAR,  
        product_name VARCHAR,  
        sales VARCHAR,  
        quantity VARCHAR,  
        discount VARCHAR,  
        profit VARCHAR,  
        employee_id VARCHAR,   
		store_name varchar,  
        employee_name VARCHAR, 
        employee_surname VARCHAR,  
		store_id VARCHAR, 
		employee_role varchar,
		customer_surname varchar,
		country_id varchar,
		state_id varchar,
		city_id varchar, 
        address_id VARCHAR
    )  
    SERVER offline_sales_external_server  
    OPTIONS (  
        FILENAME 'C:\Users\matia\Desktop\offline_sales_dataset_10000.csv',
        FORMAT 'csv',  
        HEADER 'true'  
);  


    -- Insert data into src_offline_sales while handling type conversions and avoiding duplicates  
    INSERT INTO sa_offline_sales_schema.src_offline_sales (      
		order_id,  
        order_date,   
        customer_id,  
        customer_name,  
        segment,  
        country_region,  
        city,  
        state_province,  
        postal_code,    
        product_id,  
        category,  
        sub_category,  
        product_name,  
        sales,  
        quantity,  
        discount,  
        profit,  
        employee_id,   
		store_name,
        employee_name, 
        employee_surname,  
		store_id, 
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id , 
        address_id 
      
    )  
    SELECT  
        order_id,  
        order_date,   
        customer_id,  
        customer_name,  
        segment,  
        country_region,  
        city,  
        state_province,  
        postal_code,    
        product_id,  
        category,  
        sub_category,  
        product_name,  
        sales,  
        quantity,  
        discount,  
        profit,  
        employee_id,   
		store_name,
        employee_name, 
        employee_surname,  
		store_id, 
		employee_role,
		customer_surname,
		country_id,
		state_id,
		city_id , 
        address_id 
    FROM sa_offline_sales_schema.ext_offline_sales2 eos  
    WHERE NOT EXISTS (  
        SELECT 1 FROM sa_offline_sales_schema.src_offline_sales sos WHERE sos.order_id = eos.order_id  
    );  

		get diagnostics affected_rows := row_count;

	
		 CALL bl_cl.log_procedure(
        'load_ext_offline and load_src_offline',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        CALL bl_cl.log_procedure(
            'load_ext_offline and load_src_offline',
            NULL,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE NOTICE 'Error: %', SQLERRM;
END;  
$$; 

