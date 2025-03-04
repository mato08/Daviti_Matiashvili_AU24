create table if not exists bl_dm.dim_stores(
		store_surr_id bigint not null,
		store_src_id varchar not null,
		store_name varchar not null,
		insert_dt timestamp not null,
		update_dt timestamp not null,
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
		insert_dt timestamp not null,
		update_dt timestamp not null,
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
		insert_dt timestamp not null,
		update_dt timestamp not null,
		constraint date_pk primary key (date_id)
);



create table if not exists bl_dm.dim_addresses(
		address_surr_id bigint not null,
		address_src_id varchar not null,
		city_id bigint not null,
		city_name varchar not null,
		country_id bigint not null,
		country_name varchar not null,
		state_id bigint not null,
		state_name varchar not null,
		postal_code varchar not null,
		insert_dt timestamp not null,
		update_dt timestamp not null,
		source_system varchar not null,
		source_entity varchar not null,
		constraint addresses_pk primary key(address_surr_id)
);




create table if not exists bl_dm.dim_employees_scd(
			employee_surr_id bigint not null,
			employee_src_id varchar not null,
			employee_name varchar not null,
			employee_surname varchar not null,
			start_dt date not null,
			end_dt date not null,
			is_active boolean not null,
			role varchar not null,
			insert_dt timestamp not null,
			update_dt timestamp not null,
			source_system varchar not null,
			source_entity varchar not null,
			constraint employee_surr_id primary key(employee_surr_id)
);




create table if not exists bl_dm.dim_customers(
			customer_surr_id bigint not null,
			customer_src_id varchar not null,
			customer_name varchar not null,
			customer_surname varchar not null,
			segment varchar not null,
			insert_dt timestamp not null,
			update_dt timestamp not null,
			source_system varchar not null,
			source_entity varchar not null,
			constraint customers_pk primary key(customer_surr_id )	
);



create table if not exists bl_dm.fct_orders_dd(
			order_surr_id bigint not null,
			order_src_id varchar not null,
			customer_surr_id bigint not null,
			employee_surr_id bigint not null,
			store_surr_id bigint not null,
			address_surr_id bigint not null,
			product_surr_id bigint not null,
			date_id date not null,
			order_date date not null,
			ship_date date not null,
			ship_mode varchar not null,
			insert_dt timestamp not null,
			source_system varchar not null,
			source_entity varchar not null,
			price numeric not null,
			quantity numeric not null,
			discount numeric not null,
			profit numeric not null,
			price_without_discount numeric not null,
			constraint orders_pk primary key(order_surr_id,order_date),
			constraint customer_fk foreign key(customer_surr_id) references bl_dm.dim_customers(customer_surr_id),
			constraint employee_fk foreign key(employee_surr_id) references bl_dm.dim_employees_scd(employee_surr_id),
			constraint store_fk  foreign key(store_surr_id) references bl_dm.dim_stores(store_surr_id),
			constraint address_fk foreign key(address_surr_id) references bl_dm.dim_addresses(address_surr_id),
			constraint products_fk foreign key(product_surr_id) references bl_dm.dim_products(product_surr_id),
			constraint date_fk foreign key(date_id) references bl_dm.dim_date(date_id)
)partition by range(order_date);


--historical partition
CREATE TABLE bl_dm.fct_orders_historical (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_historical
FOR VALUES FROM (MINVALUE) TO ('2024-10-01');

--2024 october partition
CREATE TABLE bl_dm.fct_orders_2024_10 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2024_10 
FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');



--2024 november partition
CREATE TABLE bl_dm.fct_orders_2024_11 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2024_11
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');



--2024 december partition
CREATE TABLE bl_dm.fct_orders_2024_12 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2024_12
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');



--2025 january partition
CREATE TABLE bl_dm.fct_orders_2025_1 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2025_1
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');




--2025 from february to 3 march partition
CREATE TABLE bl_dm.fct_orders_2025_2 (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);

ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2025_2
FOR VALUES FROM ('2025-02-01') TO ('2025-03-04');


--from 3 march to future partition.
CREATE TABLE bl_dm.fct_orders_2025_future (
    LIKE bl_dm.fct_orders_dd INCLUDING ALL
);


ALTER TABLE bl_dm.fct_orders_dd
ATTACH PARTITION bl_dm.fct_orders_2025_future
FOR VALUES FROM ('2025-03-04') TO (maxvalue);