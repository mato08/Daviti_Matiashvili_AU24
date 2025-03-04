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

ALTER TABLE bl_3nf.ce_countries 
ADD CONSTRAINT unique_countries_src1 
UNIQUE (country_src_id, source_system, source_entity);
 

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
ALTER TABLE bl_3nf.ce_states 
ADD CONSTRAINT unique_states_src1 
UNIQUE (state_src_id, source_system, source_entity);


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




create table if not exists bl_3nf.ce_address(
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
	constraint ce_stores_fk foreign key (address_id) references bl_3nf.ce_address(address_id)
);




create table if not exists bl_3nf.ce_customers(
	customer_id bigint not null,
	customer_name varchar not null,
	customer_surname varchar  not null,
	segment varchar not null,
	customer_src_id varchar not null,
	address_id bigint not null,
	insert_dt timestamp not null,
	update_dt timestamp not null,
	source_system varchar not null,
	source_entity varchar not null,
	constraint ce_customers_pk primary key (customer_id),
	constraint ce_customers_fk foreign key (address_id) references bl_3nf.ce_address(address_id)
);






create table if not exists  bl_3nf.ce_employees_scd(
	employee_id bigint not null,
	employee_src_id varchar not null,
	employee_name varchar not null,
	employee_surname varchar not null,
	store_id bigint not null,
	role varchar not null,
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






create table if not exists bl_3nf.ce_orders(
	order_id bigint not null,
	order_src_id varchar not null,
	order_date date not null,
	ship_date date not null,
	ship_mode varchar not null,	
	store_id bigint not null,
	customer_id bigint not null,
	employee_id bigint not null,
	product_id bigint not null,
	address_id bigint not null,
	source_system varchar not null,
	source_entity varchar not null,
	insert_dt timestamp not null,
	price decimal,
	quantity decimal,
    discount DECIMAL,
    profit decimal,
    price_without_discount decimal not null,
	constraint ce_orders_pk primary key(order_id),
	constraint ce_orders_fk1 foreign key(store_id) references bl_3nf.ce_stores(store_id),
	constraint ce_orders_fk2 foreign key(customer_id) references bl_3nf.ce_customers(customer_id),
	constraint ce_orders_fk3 foreign key(employee_id) references bl_3nf.ce_employees_scd(employee_id),
	constraint ce_orders_fk4 foreign key(product_id) references bl_3nf.ce_products(product_id),
	constraint ce_orders_fk5 foreign key(address_id) references bl_3nf.ce_address(address_id)
);

