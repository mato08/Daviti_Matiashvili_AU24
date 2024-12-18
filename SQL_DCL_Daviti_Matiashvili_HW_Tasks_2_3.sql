--task 2

--1)
create user rentaluser with password 'rentalpassword';
grant connect on database dvdrental to rentaluser;


--2)
grant select on table public.customer to rentaluser

SET ROLE rentaluser;
SELECT * FROM public.customer;
RESET ROLE;

--3)
create user rental;
grant rental to rentaluser;

--4)
/* in the documentation it is written that-
 * Allows UPDATE of any column, or specific column(s), of a table, view, etc. 
 * (In practice, any nontrivial UPDATE command will require SELECT privilege as well, 
 * since it must reference table columns to determine which rows to update, 
 * and/or to compute new values for columns.) SELECT ... FOR UPDATE and SELECT ... 
 * FOR SHARE also require this privilege on at least one column, in addition to the SELECT privilege. 
 * For sequences, this privilege allows use of the nextval and setval functions. For large objects,
 *  this privilege allows writing or truncating the object.
 */

grant insert on public.rental to rental;
grant select on public.rental to rental;
grant update on public.rental to rental;

select * from public.inventory i 
select * from public.staff s
select * from public.rental r 
order by r.rental_id desc;


set role rental;
insert into public.rental(rental_id,rental_date,inventory_id,customer_id,return_date,staff_id,last_update)
		select 32310,'2017-02-03',1,2,'2017-02-04',1,current_date
		returning *;
reset role;

set role rental;
UPDATE public.rental
SET return_date = '2017-02-05', staff_id = 2
WHERE rental_id = 32310;
reset role;


/* I tried solving this problem withoud hardcodingIDs but for that i needed to grant select on inventory,staff and customer tables 
to rental but since task description don't allow us to do that I think there is no other option other than what I did.
also about rental_id i wrote rental_id 32310 because at first I left rental_id part in insert statement,because I thought since
it is serial type and has auto increment we didn't need to specify its id in insert statement.but it gave me an error so thats
why I specified new ID for this table. */


--5)
revoke insert on public.rental from rental;

set role rental;
insert into public.rental(rental_id,rental_date,inventory_id,customer_id,return_date,staff_id,last_update)
		select 32311,'2017-02-03',1,2,'2017-02-04',1,current_date
		returning *;

/* it now gives error message- ERROR: permission denied for table rental
Error position: */

	
--6)
do $$
declare
    customer record;
    role_name text;
begin
    for customer in
        select distinct c.first_name, c.last_name
        from public.customer c
        join public.payment p on c.customer_id = p.customer_id
        join public.rental r on c.customer_id = r.customer_id
        where p.amount is not null and r.rental_date is not null
    loop
        role_name := 'client_' || lower(customer.first_name) || '_' || lower(customer.last_name);
        if not exists (select 1 from pg_roles where rolname = role_name) then
            execute 'create role ' || quote_ident(role_name) || ' with nologin;';
        end if;
    end loop;
end $$;



-- task 3
do $$ 
declare
    customer record;
    role_name text;
begin
    for customer in
        select distinct c.first_name, c.last_name
        from public.customer c
        join public.payment p on c.customer_id = p.customer_id
        join public.rental r on c.customer_id = r.customer_id
        where p.amount is not null and r.rental_date is not null
    loop
        role_name := 'client_' || lower(customer.first_name) || '_' || lower(customer.last_name);
        execute format('
            create policy %s_rental_policy on public.rental
            for select
            using (customer_id = (select customer_id from public.customer where lower(first_name) || ''_'' || lower(last_name) = current_user));
        ', role_name);
        execute format('
            create policy %s_payment_policy on public.payment
            for select
            using (customer_id = (select customer_id from public.customer where lower(first_name) || ''_'' || lower(last_name) = current_user));
        ', role_name);
    end loop;
end $$;




	

