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
				 country_id,
        'sa_offline_sales' AS source_system,
        'src_offline_sales' AS source_entity
    FROM sa_offline_sales_schema.src_offline_sales 
    WHERE country_region IS NOT NULL
    
    UNION
    
    SELECT 
        DISTINCT country_region,
				 country_id,
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
    WHERE cc.country_src_id = src.country_id
      AND cc.source_system = src.source_system
      AND cc.source_entity = src.source_entity
      AND cc.country_name <> src.country_region;


    WITH final_data AS (
        SELECT 
            country_region,
			country_id,
            source_system,
            source_entity,
            ROW_NUMBER() OVER (
                PARTITION BY country_id
                ORDER BY source_system
            ) AS row_num
        FROM source_data
    )
    INSERT INTO bl_3nf.ce_countries 
        (country_id, country_name, country_src_id, insert_dt, update_dt, source_system, source_entity)
    SELECT 
        NEXTVAL('bl_3nf.countries_id_seq'),
        COALESCE(country_region, 'n.a'),
        COALESCE(country_id, 'n.a'),
        NOW(),
        NOW(),
        COALESCE(source_system, 'MANUAL'),
        COALESCE(source_entity, 'MANUAL')
    FROM final_data
    WHERE row_num = 1
      AND NOT EXISTS (
          SELECT 1 
          FROM bl_3nf.ce_countries cc
          WHERE LOWER(cc.country_src_id) = LOWER(final_data.country_id)
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


create temp table if not exists source_data as(
	select 
		  state_province,
		  country_id,
		  state_id,
		  'sa_online_sales' as source_system,
		  'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
    where state_id is not null
	
	union
	
	select 
		  state_province,
		  country_id,
		  state_id,
		  'sa_offline_sales' as source_system,
		  'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos   
	where state_id is not null
);


 UPDATE bl_3nf.ce_states cs
    SET 
        state_name = src.state_province,
        update_dt = NOW()
    FROM source_data src
    WHERE cs.state_src_id = src.state_id
      AND cs.source_system = src.source_system
      AND cs.source_entity = src.source_entity
      AND cs.state_name <> src.state_province;


with state_data as(
		select 
			  src.state_province,
			  src.country_id,
			  src.state_id,
			  src.source_system,
			  src.source_entity,
	    row_number() OVER (PARTITION BY src.state_id ORDER BY src.source_system) AS row_num
    FROM source_data src
)
insert into bl_3nf.ce_states(state_id,state_src_id,state_name,insert_dt,update_dt,source_system,source_entity,country_id)
		select
				nextval('BL_3NF.states_id_seq'),
				coalesce(state_data.state_id,'n,a'),
				coalesce(state_data.state_province,'n,a'),
				coalesce(now(),'1900-1-1'::timestamp),
				coalesce(now(),'1900-1-1'::timestamp),
				coalesce(state_data.source_system,'MANUAL'),
				coalesce(state_data.source_entity,'MANUAL'),
				coalesce(cc.country_id,-1)
				from state_data
				left join bl_3nf.ce_countries cc on state_data.country_id=cc.country_src_id
				where state_data.row_num=1
				and not exists(
				select 1
				from bl_3nf.ce_states cs
				where cs.state_src_id=state_data.state_id
				and cs.source_entity=state_data.source_entity
				and cs.source_system=state_data.source_system);

		 GET DIAGNOSTICS affected_rows = ROW_COUNT;
		 drop table if exists source_data;
			 
			 CALL bl_cl.log_procedure(
        'load_ce_states',
        affected_rows,
        'Procedure completed successfully',
        'INFO'
    );
	end;
$$;









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

create temp table if not exists source_data1 as(
		select 
			  city,
			  state_id,
			  city_id,
			  'sa_online_sales' as source_system,
			  'src_online_sales' as source_entity,
			  postal_code 
		from sa_online_sales_schema.src_online_sales sos 
		where city_id is not null
		union
		
		select 
				city,
				state_id,
				city_id,
				'sa_offline_sales' as source_system,
				'src_offline_sales' as source_entity,
				postal_code
		from sa_offline_sales_schema.src_offline_sales sos 
 		where city_id is not null
);

 UPDATE bl_3nf.ce_cities cc
    SET 
        city_name= src1.city,
        update_dt = NOW()
    FROM source_data1 src1
    WHERE cc.city_src_id = src1.city_id
      AND cc.source_system = src1.source_system
      AND cc.source_entity = src1.source_entity
      AND cc.city_name <> src1.city;

with city_data as(
		select 
			 src1.city,
			 src1.state_id,
			 src1.city_id,
			 src1.source_system,
			 src1.source_entity,
			 src1.postal_code,
		row_number() over(partition by src1.city_id order by src1.source_system) as row_num
		from source_data1 src1
)
insert into bl_3nf.ce_cities(city_id,city_name,city_src_id,insert_dt,update_dt,
								state_id,source_system,source_entity,postal_code)
				
		    select nextval('BL_3NF.cities_id_seq'),
		    	   coalesce(city_data.city,'n,a'),
		    	   coalesce(city_data.city_id,'n,a'),
		    	   coalesce(now(),'1900-1-1'::timestamp),
		    	   coalesce(now(),'1900-1-1'::timestamp),
		    	   coalesce(cs.state_id,-1),
		    	   coalesce(city_data.source_system,'MANUAL'),
		    	   coalesce(city_data.source_entity,'MANUAL'),
		    	   coalesce(city_data.postal_code,'n,a')
		    from city_data 
		    left join bl_3nf.ce_states cs on cs.state_src_id=city_data.state_id
		    where row_num=1
		    and not exists(
		    select 1 
		    from bl_3nf.ce_cities cc
		    where city_data.city_id=cc.city_src_id
		    and city_data.source_system=cc.source_system
		    and city_data.source_entity=cc.source_entity);


			GET DIAGNOSTICS affected_rows = ROW_COUNT;
			drop table if exists source_data1;

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






--insert into addresses
create or replace procedure bl_cl.load_ce_addresses()
language plpgsql as $$
declare affected_rows integer;
begin 
	insert into bl_3nf.ce_address(address_id,city_id,country_id,state_id,insert_dt,update_dt,address_src_id,
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
			bl_3nf.ce_address ca
			where ca.address_id=-1);
	

create temp table if not exists source_data4 as(
		select 
				city_id,
				country_id,
				state_id,
				address_id,
				'sa_online_sales' as source_system,
				'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
		where address_id is not null 

		union 
		select 
			  city_id,
			  country_id,
			  state_id,
			  address_id,
			  'sa_offline_sales' as source_system,
			  'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
		where address_id is not null
);






with addresses_data as(
		select 
			 src4.city_id,
			 src4.country_id,
			 src4.state_id,
			 src4.address_id,
			 src4.source_system,
			 src4.source_entity,
		row_number() over(partition by src4.address_id order by src4.source_system) as row_num
		from source_data4 src4
)insert into bl_3nf.ce_address(address_id,city_id,country_id,state_id,insert_dt,
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
			 left join bl_3nf.ce_cities cc on  addresses_data.city_id=cc.city_src_id 
			 left join bl_3nf.ce_countries co on addresses_data.country_id=co.country_src_id 
			 left join bl_3nf.ce_states st on addresses_data.state_id=st.state_src_id 
			 where row_num=1
			 and not exists(
			 select 1
			 from bl_3nf.ce_address ca
			 where addresses_data.address_id=ca.address_src_id
			 and addresses_data.source_system=ca.source_system
			 and addresses_data.source_entity=ca.source_entity
			 );
			
			GET DIAGNOSTICS affected_rows = ROW_COUNT;
			drop table if exists source_data4;

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


create temp table if not exists source_data3 as (
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
);


--updating products

update bl_3nf.ce_products cp
set  	product_name=src3.product_name,
		category=src3.category,
		sub_category=src3.sub_category,
		update_dt=now()
from source_data3 src3
where cp.product_src_id=src3.product_id 
and cp.source_entity=src3.source_entity 
and cp.source_system=src3.source_system
and (cp.product_name<>src3.product_name 
or cp.category<>src3.category
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
			 and product_data.source_system=cp.source_system
			 and product_data.source_entity=cp.source_entity);
			
			get diagnostics affected_rows :=row_count;
			drop table if exists source_data3;
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
			sos.store_name as store_name,
            sos.address_id AS address_id,
            'sa_online_sales'::varchar AS source_system,
            'src_online_sales'::varchar AS source_entity
        FROM sa_online_sales_schema.src_online_sales sos
        WHERE sos.store_id IS NOT NULL
        
        UNION ALL
        
        SELECT 
            sos.store_id AS store_id,
			sos.store_name as store_name,
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
                PARTITION BY sd.store_id
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
        LEFT JOIN bl_3nf.ce_address ca 
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
                AND cs.store_name <> rec.store_name;

            ELSE
                INSERT INTO bl_3nf.ce_stores(
                    store_id, store_src_id, store_name, address_id,
                    insert_dt, update_dt, source_system, source_entity
                )
                select
                    nextval('bl_3nf.stores_id_seq'),
                    rec.store_id,
                    rec.store_name,
                    rec.address_id,
                    NOW(),
                    NOW(),
                    rec.source_system,
                    rec.source_entity
                where not exists(
				select 1 
				from bl_3nf.ce_stores cs
  				WHERE cs.store_src_id = rec.store_id
                AND cs.source_system = rec.source_system
                AND cs.source_entity = rec.source_entity);

                -- Log successful insertion
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
	
	            GET DIAGNOSTICS _affected_rows = ROW_COUNT;
                IF _affected_rows > 0 THEN
                    CALL bl_cl.log_procedure(
                        'load_ce_stores_scd1', 
                        _affected_rows, 
                        'succesful insertion: ' 
                        'INFO'
                    );
                END IF;

END;
$$;





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
            distinct(employee_id), employee_name, employee_surname, employee_role, store_id,
            'sa_online_sales' AS source_system, 'src_online_sales' AS source_entity
        FROM sa_online_sales_schema.src_online_sales 
        WHERE employee_id IS NOT NULL
        UNION
        SELECT 
            distinct(employee_id), employee_name, employee_surname, employee_role, store_id,
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
        AND ces.role <> src8.employee_role
        AND ces.is_active = 'Y';  

    WITH employee_data AS (
        SELECT 
            src8.employee_id, src8.employee_name, src8.employee_surname, src8.employee_role, src8.store_id, 
            src8.source_system, src8.source_entity,
            ROW_NUMBER() OVER (PARTITION BY src8.employee_id,src8.source_system,src8.source_entity ORDER BY src8.source_system) AS row_num
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
	where row_num=1
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







--inserting into customers table
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_customers()
LANGUAGE plpgsql AS $$
DECLARE 
    affected_rows INTEGER;
BEGIN
    -- Insert default customer if it does not exist
    INSERT INTO bl_3nf.ce_customers (customer_id,customer_name,customer_surname,segment, customer_src_id, address_id, insert_dt, update_dt, source_system, source_entity)
    SELECT -1, 'n,a','n,a','n,a', 'n,a', -1, '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_customers c WHERE c.customer_id = -1
    );

    -- Use a temporary table inside the procedure correctly
    CREATE TEMP TABLE  if not exists  source_data7 AS 
    (
        SELECT 
            customer_id,
            segment,
			customer_name,
			customer_surname,
            address_id,
            'sa_online_sales' AS source_system,
            'src_online_sales' AS source_entity
        FROM sa_online_sales_schema.src_online_sales
        WHERE customer_id IS NOT NULL

        UNION

        SELECT 
            customer_id,
            segment,
			customer_name,
			customer_surname,
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
			src7.customer_name,
			src7.customer_surname,
            src7.address_id,
            src7.source_system,
            src7.source_entity,
            ROW_NUMBER() OVER (PARTITION BY src7.customer_id,src7.source_system,src7.source_entity ORDER BY src7.source_system) AS row_num
        FROM source_data7 src7
    )
    INSERT INTO bl_3nf.ce_customers (customer_id,customer_name,customer_surname, segment, customer_src_id, address_id, insert_dt, update_dt, source_system, source_entity)
    SELECT 
        nextval('BL_3NF.customers_id_seq'),
		coalesce(customers_data.customer_name,'n,a'),
		coalesce(customers_data.customer_surname,'n,a'),
        COALESCE(customers_data.segment, 'n,a'),
        COALESCE(customers_data.customer_id, 'n,a'),
        COALESCE(ca.address_id, -1),
        COALESCE(NOW(), '1900-01-01'::timestamp),
        COALESCE(NOW(), '1900-01-01'::timestamp),
        COALESCE(customers_data.source_system, 'MANUAL'),
        COALESCE(customers_data.source_entity, 'MANUAL')
    FROM customers_data
    LEFT JOIN bl_3nf.ce_address ca ON customers_data.address_id = ca.address_src_id
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
	drop table if exists source_data7;
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










--inserting into bl_3nf ce_orders.

CREATE OR REPLACE PROCEDURE bl_cl.load_ce_orders()
LANGUAGE plpgsql AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    -- Insert default row if it doesn't exist
    INSERT INTO bl_3nf.ce_orders (
        order_id,order_src_id,order_date,ship_date,ship_mode,store_id,customer_id,employee_id, product_id,address_id,
        source_system, source_entity, insert_dt, price, quantity,
        discount, profit, price_without_discount
    )
    SELECT 
        -1,
        'n,a',
        '1900-01-01'::date,
        '1900-01-01'::date,
        'n,a',
        -1,
        -1,
        -1,
        -1,
        -1,
        'MANUAL',
        'MANUAL',
        '1900-01-01'::timestamp,
        -1,
        -1,
        -1,
        -1,
        -1
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_orders o
        WHERE o.order_id = -1
    );

    -- Process and insert data from source tables
    create temp table if not exists source_data9 AS (
        SELECT 
            order_id,
            order_date,
            ship_date,
            ship_mode,
            store_id,
            customer_id,
            employee_id,
            address_id,
            sales,
            quantity,
            discount,
            profit,
            product_id,
            'sa_online_sales' AS source_system,
            'src_online_sales' AS source_entity
        FROM sa_online_sales_schema.src_online_sales sos
        WHERE order_id IS NOT NULL
        UNION
        SELECT 
            order_id,
            order_date,
            null,
            null,
            store_id,
            customer_id,
            employee_id,
            address_id,
            sales,
            quantity,
            discount,
            profit,
            product_id,
            'sa_offline_sales' AS source_system,
            'src_offline_sales' AS source_entity
        FROM sa_offline_sales_schema.src_offline_sales sos 
        WHERE order_id IS NOT NULl
    );


create temp  table if not exists order_data AS (
        SELECT 
            src9.order_id,
            src9.order_date,
            src9.ship_date,
            src9.ship_mode,
            src9.store_id,
            src9.customer_id,
            src9.employee_id,
            src9.source_system,
            src9.source_entity,  
            src9.address_id,
            src9.sales,
            src9.quantity,
            src9.discount,
            src9.profit,
            src9.product_id,
            ROW_NUMBER() OVER (PARTITION BY src9.order_id ORDER BY src9.source_system DESC) AS row_num
        FROM source_data9 src9
    );



    INSERT INTO bl_3nf.ce_orders (
        order_id,order_src_id,order_date,ship_date,ship_mode,store_id,customer_id,employee_id, product_id,address_id,
        source_system, source_entity, insert_dt, price, quantity,
        discount, profit, price_without_discount
    )
    SELECT 
        nextval('BL_3NF.orders_id_seq'),
        COALESCE(order_data.order_id, 'n,a'), 
		COALESCE(order_data.order_date::date, '1900-01-01'::date), 
        COALESCE(order_data.ship_date::date, '1900-01-01'),
        COALESCE(order_data.ship_mode, 'n,a'),
		COALESCE(cs.store_id, -1), 
		COALESCE(cc.customer_id, -1),
		COALESCE(ec.employee_id, -1),
		COALESCE(cp.product_id, -1),
        COALESCE(ca.address_id, -1),
        COALESCE(order_data.source_system, 'n,a'), 
        COALESCE(order_data.source_entity, 'n,a'), 
        NOW(),
        COALESCE(order_data.sales::numeric, -1),
        COALESCE(order_data.quantity::numeric, -1),
        COALESCE(order_data.discount::numeric, -1),
        COALESCE(order_data.profit::numeric, -1),
		coalesce(order_data.profit::numeric+order_data.sales::numeric,-1)
    FROM order_data
	left join bl_3nf.ce_orders co on order_data.order_id=co.order_src_id
    LEFT JOIN bl_3nf.ce_employees_scd ec ON order_data.employee_id = ec.employee_src_id
    LEFT JOIN bl_3nf.ce_stores cs ON order_data.store_id = cs.store_src_id
    LEFT JOIN bl_3nf.ce_customers cc ON order_data.customer_id = cc.customer_src_id
    LEFT JOIN bl_3nf.ce_address ca ON order_data.address_id = ca.address_src_id
    LEFT JOIN bl_3nf.ce_products cp ON order_data.product_id = cp.product_src_id
    WHERE row_num = 1 and order_data.order_date::date>'2023-01-01'::date and co.order_date is null 
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
	drop table if exists source_data9;
	drop table if exists order_data;

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