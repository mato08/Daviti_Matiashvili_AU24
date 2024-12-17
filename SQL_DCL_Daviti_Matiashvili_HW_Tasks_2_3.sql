
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

grant insert on public.rental to rental;
grant select on public.rental to rental;
grant update on public.rental to rental;
grant select on public.inventory  to rental;
grant select on public.customer to rental;
grant select on public.staff to rental;
GRANT USAGE, SELECT ON SEQUENCE public.rental_rental_id_seq TO rental;


set role rental;
INSERT INTO public.rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
VALUES (
    '2024-12-15', 
    (SELECT inventory_id FROM public.inventory WHERE store_id = 1 LIMIT 1),
    (SELECT customer_id FROM public.customer 
     WHERE UPPER(email) = UPPER('datomatiashvili91@gmail.com') 
       AND UPPER(first_name) = UPPER('daviti') 
       AND UPPER(last_name) = UPPER('matiashvili')),
    '2024-12-16', 
    (SELECT staff_id FROM public.staff 
     WHERE UPPER(first_name) = UPPER('mike') 
       AND UPPER(last_name) = UPPER('hillyer')), 
    CURRENT_DATE
);
reset role;

set role rental;
UPDATE public.rental
SET return_date = '2024-12-17'
WHERE rental_id = (
    SELECT rental_id
    FROM public.rental
    WHERE inventory_id = (SELECT inventory_id FROM public.inventory WHERE store_id = 1 LIMIT 1)
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
insert into public.rental(rental_id,rental_date,inventory_id,customer_id,return_date,staff_id,last_update)
		select 32311,'2017-02-03',1,2,'2017-02-04',1,current_date
		returning *;

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


	

