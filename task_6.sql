create schema if not exists BL_3NF;
---
create sequence if not exists BL_3NF.stores_id_seq start 1;
create sequence if not exists BL_3NF.customers_id_seq start 1;
create sequence if not exists BL_3NF.address_id_seq start 1;
create sequence if not exists BL_3NF.states_id_seq start 1;
create sequence if not exists BL_3NF.cities_id_seq start 1;
create sequence if not exists BL_3NF.countries_id_seq start 1;
create sequence if not exists BL_3NF.products_id_seq start 1;
create sequence if not exists BL_3NF.orders_id_seq start 1;
create sequence if not exists BL_3NF.employees_id_seq start 1;

create table if not exists bl_3nf.ce_countries(
	country_id bigint not null,
	country_name varchar not null,
	country_src_id varchar not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_system varchar not null,
	source_entity varchar not null,
	constraint ce_countries_pk primary key(country_id)
);

 

create table if not exists bl_3nf.ce_states(
	state_id bigint not null,
	state_src_id varchar not null,
	state_name varchar not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_system varchar not null,
	source_entity varchar not null,
	country_id bigint not null,
	constraint ce_states_pk primary key(state_id),
	constraint ce_states_fk foreign key(country_id) references bl_3nf.ce_countries(country_id)
);



create table if not exists bl_3nf.ce_cities(
	city_id bigint not null,
	city_name varchar not null,
	city_src_id varchar not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	state_id bigint not null,
	postal_code varchar not null,
	source_system varchar not null,
	source_entity varchar not null,
	constraint ce_cities_pk primary key(city_id), 
	constraint ce_cities_fk foreign key(state_id) references bl_3nf.ce_states(state_id)
);




create table if not exists bl_3nf.ce_addresses(
	address_id bigint not null,
	city_id bigint not null,
	country_id bigint not null,
	state_id bigint not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	address_src_id varchar not null,
	source_system  varchar not null,
	source_entity varchar not null,
	constraint ce_addresses_pk primary key(address_id),
	constraint ce_addresses_fk foreign key(city_id) references bl_3nf.ce_cities(city_id),
	constraint ce_addreses_fk1 foreign key(state_id) references bl_3nf.ce_states(state_id),
	constraint ce_addreses_fk2 foreign key(country_id) references bl_3nf.ce_countries(country_id)
);




create table if not exists  BL_3NF.CE_Stores(
	store_id bigint not null,
	store_src_id varchar not null,
	store_name varchar not null,
	address_id bigint not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_system varchar not null,
	source_entity varchar not null,
	constraint ce_stores_pk primary key (store_id),
	constraint ce_stores_fk foreign key (address_id) references bl_3nf.ce_addresses(address_id)
);




create table if not exists bl_3nf.ce_customers(
	customer_id bigint not null,
	segment varchar not null,
	customer_src_id varchar not null,
	address_id bigint not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_system varchar not null,
	source_entity varchar not null,
	constraint ce_customers_pk primary key (customer_id),
	constraint ce_customers_fk foreign key (address_id) references bl_3nf.ce_addresses(address_id)
);





create table if not exists  bl_3nf.ce_employees_scd(
	employee_id bigint not null,
	employee_src_id varchar not null,
	employee_name varchar not null,
	employee_surname varchar not null,
	store_id bigint not null,
	start_date timestamp not null,
	end_date timestamp not null,
	is_active boolean not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_entity varchar not null,
	source_system varchar not null,
	constraint ce_employee_scd_pk primary key(employee_id),
	constraint ce_employee_scd_fl foreign key(store_id) references bl_3nf.ce_stores(store_id)
);



create table if not exists bl_3nf.ce_products(
	product_id bigint not null,
	product_src_id varchar not null,
	category varchar not null,
	sub_category varchar not null,
	product_name varchar not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_system varchar not null,
	source_entity varchar not null,
	constraint ce_products_pk primary key(product_id)
);


select * from bl_3nf.ce_products cp 

create table if not exists bl_3nf.ce_orders(
	order_id bigint not null,
	order_src_id varchar not null,
	order_type varchar not null,
	delivery_method  varchar not null,
	order_dt date not null,
	ship_dt date not null,
	ship_mode varchar not null,
	store_id bigint not null,
	customer_id bigint not null,
	employee_id bigint not null,
	payment_method varchar not null,
	source_system varchar not null,
	source_entity varchar not null,
	insert_dt timestamp not null,
	constraint ce_orders_pk primary key(order_id),
	constraint ce_orders_fk1 foreign key(store_id) references bl_3nf.ce_stores(store_id),
	constraint ce_orders_fk2 foreign key(customer_id) references bl_3nf.ce_customers(customer_id),
	constraint ce_orders_fk3 foreign key(employee_id) references bl_3nf.ce_employees_scd(employee_id)
);



CREATE table if not exists bl_3nf.ce_order_details(
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity decimal,
    discount DECIMAL,
    profit decimal,
    insert_dt timestamp without time zone not null ,
    update_dt timestamp without time zone not null,
    source_system VARCHAR,
    source_entity VARCHAR,
    order_details_src_id VARCHAR,
    PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES bl_3nf.ce_orders(order_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES bl_3nf.ce_products(product_id)
);


insert into bl_3nf.ce_countries (country_id,country_name,country_src_id,insert_dt,update_dt,source_system,source_entity)
		select -1,
			   'n,a',
			   'n,a',
			   '1-1-1900'::timestamp,
			   '1-1-1900'::timestamp,
			   'MANUAL',
			   'MANUAL'
		where not exists(
		select 1 
		from bl_3nf.ce_countries c
		where c.country_id=-1);
		commit;

		

insert into bl_3nf.ce_states (state_id,state_src_id,state_name,insert_dt,update_dt,source_system,source_entity,country_id)
				select -1,
					   'n,a',
					   'n,a',
					   '1-1-1900'::timestamp,
					   '1-1-1900'::timestamp,
					   'MANUAL',
					   'MANUAL',
					   (select c.country_id
					   from bl_3nf.ce_countries c
					   where c.country_name='n,a')
				where not exists(
				select 1 
				from bl_3nf.ce_states s
				where s.state_id=-1);
			commit;
						

insert into bl_3nf.ce_cities (city_id,city_name,city_src_id,insert_dt,update_dt,state_id,source_system,source_entity,postal_code)
			select -1,
				   'n,a',
				   'n,a',
				   '1-1-1900'::timestamp,
				   '1-1-1900'::timestamp,
				   (select s.state_id
				   from bl_3nf.ce_states s
				   where s.state_name='n,a'),
				   'MANUAL',
				   'MANUAL',
				   'n,a'
			where not exists(
			select 1 
			from bl_3nf.ce_cities c
			where c.city_id=-1);
		commit;
			

				   
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
			commit;	
		
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
		commit;
	
	
insert into bl_3nf.ce_stores(store_id,store_src_id,store_name,address_id,insert_dt,update_dt,source_system,source_entity)
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
			from bl_3nf.ce_stores s
			where s.store_id=-1);
		commit;
	
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
		commit;
	
	
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
		commit;
	
	
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
		commit;


--inserting rows in country table 
	
with source_data as(
	select 
		distinct(country_region), 
		'sa_offline_sales' as source_system,
		'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos 
	
	union
	
	select 
		distinct(country_region),
		'sa_online_sales' as source_system,
		'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
),
final_data as(
	select 
		src.country_region,
		src.source_system,
		src.source_entity,
	 ROW_NUMBER() OVER (PARTITION BY src.country_region ORDER BY src.source_system) AS row_num
    FROM source_data src
)
insert into bl_3nf.ce_countries (country_id,country_name,country_src_id,insert_dt,update_dt,source_system,source_entity)
			select nextval('BL_3NF.countries_id_seq'),
			   coalesce(final_data.country_region,'n,a') as country_name,
			   coalesce(final_data.country_region,'n,a') as country_src_id,
			   coalesce(now(),'1900-1-1'::timestamp),
			   coalesce(now(),'1900-1-1'::timestamp),
			   coalesce(final_data.source_system,'MANUAL'),
			   coalesce(final_data.source_entity,'MANUAL')
		from final_data
		where final_data.row_num=1
		and not exists(
		select 1 
		from  bl_3nf.ce_countries cc
		where cc.country_src_id=final_data.country_region
		and  cc.source_system= final_data.source_system
		and cc.source_entity=final_data.source_entity);

	
--inserting rows in states table
	
with source_data1 as(
	select 
		  state_province,
		  country_region,
		  'sa_online_sales' as source_system,
		  'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
	
	union
	
	select 
		  state_province,
		  country_region,
		  'sa_offline_sales' as source_system,
		  'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos   
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
						
				
			
--inserting data in ce_cities		
	
with source_data2 as(
		select 
			  city,
			  state_province,
			  'sa_online_sales' as source_system,
			  'src_online_sales' as source_entity,
			  postal_code 
		from sa_online_sales_schema.src_online_sales sos 
		
		union
		
		select 
				city,
				state_province,
				'sa_offline_sales' as source_system,
				'src_offline_sales' as source_entity,
				postal_code
		from sa_offline_sales_schema.src_offline_sales sos 
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
		    

	

	
--inserting in product
	
with source_data3 as(
	select 
			product_id,
			category,
			sub_category,
			product_name,
			'sa_online_sales' as source_system,
			'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
	
	union
			
	select 
			product_id,
			category,
			sub_category,
			product_name,
			'sa_offline_sales' as source_system,
			'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos
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
			  		 coalesce(now(),'1900-1-1'::timestamp),
					 coalesce(now(),'1900-1-1'::timestamp),
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
					 

--inserting into addresses

with source_data4 as(
		select 
				city,
				country_region,
				state_province,
				'sa_online_sales' as source_system,
				'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
		union 
		select 
			  city,
			  country_region,
			  state_province,
			  'sa_offline_sales' as source_system,
			  'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
),addresses_data as(
		select 
			 src4.city,
			 src4.country_region,
			 src4.state_province,
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
			 		coalesce(now(),'1900-1-1'::timestamp),
			 		coalesce(now(),'1900-1-1'::timestamp),
			 		coalesce(addresses_data.city || ''||addresses_data.country_region|| ',' || addresses_data.state_province,'n,a') as address_src_id,
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
			 where addresses_data.city || ''||addresses_data.country_region|| ',' || addresses_data.state_province=ca.address_src_id
			 and addresses_data.source_system=ca.source_system
			 and addresses_data.source_entity=ca.source_entity
			 );
			 		


with source_data6 as (
select
    store_id,
    city,
    country_region,
    state_province,
    'sa_online_sales' as source_system,
    'src_online_sales' as source_entity
from sa_online_sales_schema.src_online_sales sos

union

select
    store_id,
    city,
    country_region,
    state_province,
    'sa_offline_sales' as source_system,
    'src_offline_sales' as source_entity
from sa_offline_sales_schema.src_offline_sales sos
),
store_data as(
select
    src6.store_id,
    src6.city,
    src6.country_region,
    src6.state_province,
    src6.source_system,
    src6.source_entity,
    row_number() over(partition by src6.store_id order by src6.source_system) as row_num
from source_data6 src6
) 
insert into bl_3nf.ce_stores (
    store_id,
    store_src_id,
    store_name,
    address_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
select 
    nextval('BL_3NF.stores_id_seq'),
    coalesce(store_data.store_id,'n,a'),
    coalesce(store_data.store_id,'n,a'),
    coalesce((
        select ca.address_id
        from bl_3nf.ce_addresses ca
        where ca.address_src_id = store_data.city || '' || store_data.country_region || ',' || store_data.state_province
        limit 1
    ),-1),
    coalesce(now(),'1900-1-1'::timestamp),
    coalesce(now(),'1900-1-1'::timestamp),
    coalesce(store_data.source_system,'MANUAL'),
    coalesce(store_data.source_entity,'MANUAL')
from store_data
where row_num=1 
and not exists(
    select 1
    from bl_3nf.ce_stores cs
    where store_data.store_id = cs.store_src_id
    and store_data.source_system = cs.source_system
    and store_data.source_entity = cs.source_entity
);


			  		
			  		 
with source_data7 as(
		select 
				customer_id,
				segment,
				city,
				country_region,
				state_province,
				'sa_online_sales' as source_system,
				'src_online_sales' as source_entity
		from sa_online_sales_schema.src_online_sales sos 
	    
		union
		
		select 
				customer_id,
				segment,
				city,
				country_region,
				state_province,
				'sa_offline_sales' as source_system,
				'src_offline_sales' as source_entity
		from sa_offline_sales_schema.src_offline_sales sos 
) ,customers_data as(
		select 
				src7.customer_id,
				src7.segment,
				src7.city,
				src7.country_region,
				src7.state_province,
				src7.source_system,
				src7.source_entity,
				row_number() over(partition by src7.customer_id order by src7.source_system) as row_num
		from source_data7 as src7		   
)insert into bl_3nf.ce_customers(customer_id,segment,customer_src_id,address_id,insert_dt,update_dt,source_system,source_entity)
			select 
					nextval('BL_3NF.customers_id_seq'),
					coalesce(customers_data.segment,'n,a'),
					coalesce(customers_data.customer_id,'n,a'),
					coalesce((
						        select ca.address_id
						        from bl_3nf.ce_addresses ca
						        where ca.address_src_id = customers_data.city || '' || customers_data.country_region || ',' || customers_data.state_province
						        limit 1
						    ),-1),
				  coalesce(now(),'1900-1-1'::timestamp),
				  coalesce(now(),'1900-1-1'::timestamp),
				  coalesce(customers_data.source_system,'MANUAL'),
				  coalesce(customers_data.source_entity,'MANUAL')
			from customers_data
			where row_num=1
			and not exists(
			select 1 
			from bl_3nf.ce_customers cc
			where customers_data.customer_id=cc.customer_src_id
			and  customers_data.source_system=cc.source_system
			and customers_data.source_entity=cc.source_entity);
		
    				



with source_data8 as(
	select 
			employee_id,
			employee_name,
			employee_surname,
			store_id,
			'sa_online_sales' as source_system,
			'src_online_sales' as source_entity
	from sa_online_sales_schema.src_online_sales sos 
	
	union
	
	select 
			employee_id,
			employee_name,
			employee_surname,
			store_id,
			'sa_offline_sales' as source_system,
			'src_offline_sales' as source_entity
	from sa_offline_sales_schema.src_offline_sales sos 
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
					coalesce(now(),'1900-1-1'::timestamp),
					coalesce(now(),'9999-12-31'::timestamp),
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
    COALESCE(NOW(), '1900-01-01 00:00:00'::TIMESTAMP) 
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
			

			
		
