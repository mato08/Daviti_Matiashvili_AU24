create or replace procedure bl_cl.master_call_for_loading_source_data()
language plpgsql 
as $$
begin
	call bl_cl.main_load_ext_online_sales_data();
	call bl_cl.main_load_ext_offline_sales_data();
end;
$$;

call  bl_cl.master_call_for_loading_source_data();
select * from bl_cl.logs l 


create or replace procedure bl_cl.inserting_rows_in_3nf_tables()
language plpgsql
as $$
begin
	call bl_cl.load_ce_countries();
	call bl_cl.load_ce_states();
	call bl_cl.load_ce_cities();
	call bl_cl.load_ce_addresses();
	call bl_cl.load_ce_product();
	call bl_cl.load_ce_stores();
	call bl_cl.load_ce_employees_scd();
    call bl_cl.load_ce_customers();
   	call bl_cl.load_ce_orders();
end;
$$;

call bl_cl.inserting_rows_in_3nf_tables();
select * from bl_cl.logs l 
 
 select * from bl_3nf.ce_orders co 
group by order_src_id,order_id
create or replace procedure bl_cl.inserting_rows_in_dm_tables()
language plpgsql 
as $$
begin 
		call bl_cl.load_dim_stores();
		call bl_cl.load_dim_addresses();
		call bl_cl.load_dim_dates();
		call bl_cl.dim_customers();
		call bl_cl.dim_products();
		call bl_cl.dim_employees();  
		call bl_cl.fct_orders_dd();
end;
$$;



call bl_cl.inserting_rows_in_dm_tables();
select * from bl_cl.logs l 
select count(*) from bl_dm.fct_orders_dd fod 




create or replace procedure bl_cl.incremental_loading()
language plpgsql
as $$
begin 
	call bl_cl.incremental_load_ext_online_sales_data();
	call bl_cl.incremental_load_ext_offline_sales_data();
end;
$$;



call bl_cl.incremental_loading()
select * from bl_cl.logs l 



select count(*) from bl_dm.fct_orders_2025_2
select count(*) from bl_dm.fct_orders_2025_1



