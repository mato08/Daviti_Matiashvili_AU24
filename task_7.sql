create schema if not exists bl_dm;



create table if not exists bl_dm.dim_stores(
		store_surr_id bigint not null,
		store_src_id varchar not null,
		store_name varchar not null,
		insert_dt date not null,
		update_dt date not null,
		source_system varchar not null,
		source_entity varchar not null,
		constraint dim_stores_pk primary key (store_surr_id)
);


create table if not exists bl_dm.dim_products(
		product_surr_id bigint not null,
		product_src_id varchar not null,
		category varchar not null,
		sub_category varchar not null,
		product_name varchar not null,
		price decimal not null,
		discount decimal not null,
		insert_dt date not null,
		update_dt date not null,
		source_system varchar not null,
		source_entity varchar not null,
		constraint products_pk primary key (product_surr_id)
);


create table if not exists bl_dm.dim_date(
		date_id date not null,
		year_no smallint not null,
		quarter_no smallint not null,
		month_no smallint not null,
		month_name text not null,
		week_no smallint not null,
		day_no smallint not null,
		day_name text not null,
		is_weekend boolean not null,
		insert_dt date not null,
		update_dt date not null,
		constraint date_pk primary key (date_id)
);


create table if not exists bl_dm.dim_addresses(
		address_id bigint not null,
		address_src_id varchar not null,
		city_id bigint not null,
		city_name varchar not null,
		country_id bigint not null,
		country_name varchar not null,
		state_id bigint not null,
		state_name varchar not null,
		postal_code varchar not null,
		insert_dt date not null,
		update_dt date not null,
		source_system varchar not null,
		source_entity varchar not null,
		constraint addresses_pk primary key(address_id)
);



create table if not exists bl_dm.dim_employees_scd(
			employee_surr_id bigint not null,
			employee_src_id varchar not null,
			employee_name varchar not null,
			employee_surname varchar not null,
			start_dt date not null,
			end_dt date not null,
			is_active boolean not null,
			insert_dt date not null,
			update_dt date not null,
			source_system varchar not null,
			source_entity varchar not null,
			constraint employee_surr_id primary key(employee_surr_id)
);



create table if not exists bl_dm.dim_customers(
			customer_surr_id bigint not null,
			customer_src_id varchar not null,
			segment varchar not null,
			date_of_birth date not null,
			gender varchar not null,
			insert_dt date not null,
			update_dt date not null,
			source_system varchar not null,
			source_entity varchar not null,
			constraint customers_pk primary key(customer_surr_id )	
);



create table if not exists bl_dm.dim_orders_dd(
			orders_surr_id bigint not null,
			customer_surr_id bigint not null,
			employee_surr_id bigint not null,
			store_surr_id bigint not null,
			address_id bigint not null,
			product_surr_id bigint not null,
			date_id date not null,
			order_type varchar not null,
			order_dt date not null,
			ship_dt date not null,
			ship_mode varchar not null,
			insert_dt date not null,
			source_system varchar not null,
			source_entity varchar not null,
			total_price decimal not null,
			quantity int not null,
			discounted_amount decimal not null,
			profit_amt decimal not null,
			constraint orders_pk primary key(orders_surr_id),
			constraint customer_fk foreign key(customer_surr_id) references bl_dm.dim_customers(customer_surr_id),
			constraint employee_fk foreign key(employee_surr_id) references bl_dm.dim_employees_scd(employee_surr_id),
			constraint store_fk  foreign key(store_surr_id) references bl_dm.dim_stores(store_surr_id),
			constraint address_fk foreign key(address_id) references bl_dm.dim_addresses(address_id),
			constraint products_fk foreign key(product_surr_id) references bl_dm.dim_products(product_surr_id),
			constraint date_fk foreign key(date_id) references bl_dm.dim_date(date_id)
);


insert into bl_dm.dim_addresses(address_id,address_src_id,city_id,city_name,country_id,country_name,
								state_id,state_name,postal_code,insert_dt,update_dt,source_system,source_entity)
			select  
					-1,
					'n,a',
					-1,
					'n,a',
					-1,
					'n,a',
					-1,
					'n,a',
					'n,a',
					'1900-1-1'::date,
					'1900-1-1'::date,
					'MANUAL',
					'MANUAL'
			where not exists(
			select 1
			from bl_dm.dim_addresses a
			where a.address_id=-1);
		commit;
	
	
	
insert into bl_dm.dim_customers (customer_surr_id,customer_src_id,segment,date_of_birth,gender,insert_dt,update_dt,source_system,source_entity)
				select 
						-1,
						'n,a',
						'n,a',
						'1900-1-1'::date,
						'n,a',
						'1900-1-1'::date,
						'1900-1-1'::date,
						'MANUAL',
						'MANUAL'
				where not exists(
				select 1
				from bl_dm.dim_customers c
				where c.customer_surr_id=-1);
			commit;
					
		
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
							'1900-1-1'::date,
							'1900-1-1'::date
					where not exists(
					select 1 
					from bl_dm.dim_date d
					where d.date_id='1900-1-1'::date);
			commit;
					

insert into bl_dm.dim_employees_scd(employee_surr_id,employee_src_id,employee_name,employee_surname,start_dt,end_dt,is_active,
									insert_dt,update_dt,source_system,source_entity)
					select 
					      -1,
					      'n,a',
					      'n,a',
					      'n,a',
					      '1900-1-1'::date,
					      '9999-12-31'::date,
					      'Y',
					      '1900-1-1'::date,
					      '1900-1-1'::date,
					      'MANUAL',
					      'MANUAl'
					 where not exists(
					 select 1 
					 from bl_dm.dim_employees_scd e
					 where e.employee_surr_id=-1);
				commit;

			
insert into bl_dm.dim_products (product_surr_id,product_src_id,category,sub_category,product_name,price,discount,
								insert_dt,update_dt,source_system,source_entity)
					select 
							-1,
							'n,a',
							'n,a',
							'n,a',
							'n,a',
							-1,
							-1,
							'1900-1-1'::date,
							'1900-1-1'::date,
							'MANUAL',
							'MANUAL'
					where not exists(
					select 1
					from bl_dm.dim_products p
					where p.product_surr_id=-1);
				commit;
		

insert into bl_dm.dim_stores (store_surr_id,store_src_id,store_name,insert_dt,update_dt,source_system,source_entity)

				select 
						-1,
						'n,a',
						'n,a',
						'1900-1-1'::date,
						'1900-1-1'::date,
						'MANUAL',
						'MANUAL'
			  where not exists(
			  select 1 
			  from bl_dm.dim_stores s
			  where s.store_surr_id=-1);
			 commit;
			 
			
			
			

					
