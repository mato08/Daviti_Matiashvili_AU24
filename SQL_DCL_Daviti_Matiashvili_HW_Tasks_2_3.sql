
--task 2

--1)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_roles 
        WHERE rolname = 'rentaluser'
    ) THEN
        CREATE USER rentaluser WITH PASSWORD 'rentalpassword';
    ELSE
        RAISE NOTICE 'Role "rentaluser" already exists.';
    END IF;
END $$;


grant connect on database dvdrental to rentaluser;


--2)
grant select on table public.customer to rentaluser

SET ROLE rentaluser;
SELECT * FROM public.customer;
RESET ROLE;

--3)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_roles 
        WHERE rolname = 'rental'
    ) THEN
        CREATE role rental;
    ELSE
        RAISE NOTICE 'Role "rental" already exists.';
    END IF;
END $$;

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
grant select on public.film  to rental;
grant insert on public.rental to rental;
grant select on public.rental to rental;
grant update on public.rental to rental;
grant select on public.inventory  to rental;
grant select on public.customer to rental;
grant select on public.staff to rental;
GRANT USAGE, SELECT ON SEQUENCE public.rental_rental_id_seq TO rental;




set role rental;
INSERT INTO public.rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
select 
    '2024-12-15', 
    (SELECT inventory_id FROM public.inventory i
    join public.film f on i.film_id=f.film_id
    WHERE upper(f.title) = upper('solyaris') and f.release_year=1972
    LIMIT 1),
    (SELECT customer_id FROM public.customer c
     WHERE UPPER(c.email) = UPPER('datomatiashvili91@gmail.com') 
       AND UPPER(c.first_name) = UPPER('daviti') 
       AND UPPER(c.last_name) = UPPER('matiashvili')),
    '2024-12-16', 
    (SELECT staff_id FROM public.staff s
     WHERE UPPER(s.first_name) = UPPER('jon') 
       AND UPPER(s.last_name) = UPPER('stephens')), 
    CURRENT_DATE
    where not exists(
    select 1 
    from public.rental r
    join public.inventory i on r.inventory_id=i.inventory_id
    join public.film f on i.film_id=f.film_id
    join public.customer c on r.customer_id=c.customer_id
    join public.staff s on r.staff_id=s.staff_id
    where UPPER(c.email) = UPPER('datomatiashvili91@gmail.com') 
       AND UPPER(c.first_name) = UPPER('daviti') 
       AND UPPER(c.last_name) = UPPER('matiashvili') 
       and  UPPER(s.first_name) = UPPER('jon') 
       AND UPPER(s.last_name) = UPPER('stephens')
       and upper(f.title) = upper('solyaris') 
       and f.release_year=1972)
      returning *;
reset role;

/* at first i revoked all the privileges from role rental and then granted them again.(to be sure that it works now not only for
 * me but also for you too)
 * to select film title we need to grant select privilege to rental table so I added it also and it added new row
 * */


set role rental;
UPDATE public.rental
SET return_date = '2024-12-17'
WHERE rental_id = (
    SELECT rental_id
    FROM public.rental
    WHERE inventory_id = (SELECT inventory_id FROM public.inventory i
    					  join public.film  f on i.film_id=f.film_id
    					  WHERE upper(f.title) =upper('solyaris') and f.release_year=1972
    					  LIMIT 1)
      AND customer_id = (SELECT customer_id FROM public.customer 
                         WHERE UPPER(email) = UPPER('datomatiashvili91@gmail.com') 
                           AND UPPER(first_name) = UPPER('daviti') 
                           AND UPPER(last_name) = UPPER('matiashvili'))
    LIMIT 1
);
reset role;





--5)
revoke insert on public.rental from rental;

set role rental;
INSERT INTO public.rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
select 
    '2023-12-15', 
    (SELECT inventory_id FROM public.inventory i
    join public.film f on i.film_id=f.film_id
    WHERE upper(f.title) = upper('solyaris') and f.release_year=1972
    LIMIT 1),
    (SELECT customer_id FROM public.customer c
     WHERE UPPER(c.email) = UPPER('datomatiashvili91@gmail.com') 
       AND UPPER(c.first_name) = UPPER('daviti') 
       AND UPPER(c.last_name) = UPPER('matiashvili')),
    '2024-12-16', 
    (SELECT staff_id FROM public.staff s
     WHERE UPPER(s.first_name) = UPPER('jon') 
       AND UPPER(s.last_name) = UPPER('stephens')), 
    CURRENT_DATE
    where not exists(
    select 1 
    from public.rental r
    join public.inventory i on r.inventory_id=i.inventory_id
    join public.film f on i.film_id=f.film_id
    join public.customer c on r.customer_id=c.customer_id
    join public.staff s on r.staff_id=s.staff_id
    where UPPER(c.email) = UPPER('datomatiashvili91@gmail.com') 
       AND UPPER(c.first_name) = UPPER('daviti') 
       AND UPPER(c.last_name) = UPPER('matiashvili') 
       and  UPPER(s.first_name) = UPPER('jon') 
       AND UPPER(s.last_name) = UPPER('stephens')
       and upper(f.title) = upper('solyaris') 
       and f.release_year=1972)
      returning *;
reset role;
/* it now gives error message- ERROR: permission denied for table rental
Error position: */

	
--6)
DO $$
DECLARE
    customer RECORD;
    role_name TEXT;
BEGIN
    -- Loop through each customer who has data in the payment and rental tables
    FOR customer IN
        SELECT DISTINCT c.first_name, c.last_name
        FROM public.customer c
        JOIN public.payment p ON c.customer_id = p.customer_id
        JOIN public.rental r ON c.customer_id = r.customer_id
        WHERE p.amount IS NOT NULL AND r.rental_date IS NOT NULL
    LOOP
        role_name := 'client_' || lower(customer.first_name) || '_' || lower(customer.last_name);
        
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || quote_ident(role_name) || ' WITH NOLOGIN;';
        END IF;
        
        EXECUTE 'GRANT SELECT ON public.rental TO ' || quote_ident(role_name) || ';';
        EXECUTE 'GRANT SELECT ON public.payment TO ' || quote_ident(role_name) || ';';
    END LOOP;
END $$;



-- task 3
ALTER TABLE rental ENABLE ROW LEVEL SECURITY;
ALTER TABLE payment ENABLE ROW LEVEL SECURITY;


-- Create a policy for the rental table

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_policies 
        WHERE tablename = 'rental' AND policyname = 'rental_policy'
    ) THEN
        EXECUTE '
            CREATE POLICY rental_policy 
            ON rental 
            FOR SELECT 
            USING (
                customer_id = (
                    SELECT customer_id
                    FROM public.customer
                    WHERE UPPER(first_name) = UPPER(split_part(current_user, ''_'', 2))
                    AND UPPER(last_name) = UPPER(split_part(current_user, ''_'', 3))
                    LIMIT 1
                )
            )';
    END IF;
END $$;

-- Create a policy for the payment table
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_policies 
        WHERE tablename = 'payment' AND policyname = 'payment_policy'
    ) THEN
        EXECUTE '
            CREATE POLICY payment_policy 
            ON payment 
            FOR SELECT 
            USING (
                customer_id = (
                    SELECT customer_id
                    FROM public.customer
                    WHERE UPPER(first_name) = UPPER(split_part(current_user, ''_'', 2))
                    AND UPPER(last_name) = UPPER(split_part(current_user, ''_'', 3))
                    LIMIT 1
                )
            )';
    END IF;
END $$;



set role client_aaron_selby
select * from public.payment p 
reset role


	

