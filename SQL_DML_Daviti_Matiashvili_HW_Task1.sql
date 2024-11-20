
-- first film
 insert into public.film(title,description,release_year,language_id,original_language_id,
				rental_duration,rental_rate,length,replacement_cost,
				rating,last_update,special_features,fulltext)
				
SELECT 'Solyaris', 'A psychologist is sent to a station orbiting a distant planet in order to discover what has caused the crew to go insane.', 
       1972,(select l.language_id from public."language" l 
       			where upper(l.name)=upper('english')), null, 7, 4.99, 167, 16.99, 
       			'PG', current_date, '{Trailers}', 'solyaris'
       			where not exists (
       			select 1 from public.film f 
       			where upper(f.title)=upper('Solyaris'))
       returning * ;
       
       -- second film
  insert into public.film(title,description,release_year,language_id,original_language_id,
				rental_duration,rental_rate,length,replacement_cost,	
				rating,last_update,special_features,fulltext)
       
SELECT 'Dreams', 'A collection of tales based upon eight of director Akira Kurosawas recurring dreams.', 
       1990,(select l.language_id from public."language" l 
       			where upper(l.name)=upper('japanese')), null, 14, 9.99, 119, 
       			14.99, 'PG', current_date, '{Dreams}', 'Dreams'
       			where not exists(
       			select 1 from publi.film f
       			where upper(f.title)=upper('dreams'))
       returning *;
       
   		--third film    
  insert into public.film(title,description,release_year,language_id,original_language_id,
				rental_duration,rental_rate,length,replacement_cost,
				rating,last_update,special_features,fulltext)
				
SELECT 'The double life of veronique', 'Two parallel stories about two identical women; one living in Poland, the other in France. They dont know each other, but their lives are nevertheless profoundly connected.', 
       1991, 
       		(select l.language_id from public.language l
       		where upper(l.name)=upper('french')), null, 21, 19.99, 98, 21.99, 'R', current_date, 
       		'{Behind the scenes}', 'The double life of veronique'
       		where not exists(
       		select 1 from public.film f
       		where upper(f.title)=upper('the double life of veronique'))
       returning *;
       
        

    
       
       
       
     
       
    /*At first I tried different approach
    	insert into film(...) 
    	select ...
    	returning *
    	 
    	 I wrote two other select statements for other two films, and used UNION ALL between the select statements but it didn't work.
    	 I don't know why because in error message it was written that there was casting problems.for instance one was like this-
    	 ERROR: column "original_language_id" is of type smallint but expression is of type text.I casted null to smallint but
    	 then there was another casting problem for mpaa_rating.
    	 So because of that I changed the approach and wrote it like this. I also tried to do it with CTE but
    	 it didn't work also.I think there will be better ways to do this, so if there is I would be grateful to hear
    	 it from you.
     */
       
       insert into public.actor(first_name,last_name,last_update)	  
       
       (select 'Negishi','Toshie',current_date
       where not exists(
       select 1 from public.actor a
       where upper(a.first_name)=upper('negishi') and upper(a.last_name)=upper('toshie')
       )
       union all 
       select 'Harada','Mieko', current_date
       where not exists(
       select 1 from public.actor a
       where upper(a.first_name)=upper('harada') and upper(a.last_name)=upper('mieko'))
       union all 
       select 'Irene','Jacob',current_date
       where not exists(
       select 1 from public.actor a
       where upper(a.first_name)=upper('irene') and upper(a.last_name)=upper('jacob'))
       union all
       select 'Aleksander','Bardini',current_date
        where not exists(
       select 1 from public.actor a	
       where upper(a.first_name)=upper('aleksander') and upper(a.last_name)=upper('Bardini'))
       union all 
       select 'Natalya','Bondarchuk',current_date
        where not exists(
       select 1 from public.actor a
       where upper(a.first_name)=upper('Natalya') and upper(a.last_name)=upper('Bondarchuk'))
       union all 
       select 'Donatas','Banionis',current_date
       where not exists(
       select 1 from public.actor a
       where upper(a.first_name)=upper('donatas') and upper(a.last_name)=upper('banionis'))
       )
       returning *;    
       --actor_id 204,205  film_id-1025
       --actor_id 206,207  film_id-1026
       --actor_id 208,209  film_id-1024
       
      
 insert into public.film_actor (actor_id, film_id, last_update)
select 
    actor_id, film_id, current_date
from (
    select 
        (select a.actor_id 
         from public.actor a
         where upper(a.first_name) = upper('negishi') 
           and upper(a.last_name) = upper('toshie')) as actor_id,
        (select f.film_id 
         from public.film f
         where upper(f.title) = upper('dreams')) as film_id
    where not exists (
        select 1 
        from public.film_actor fa
        join public.actor a on fa.actor_id = a.actor_id
        join public.film f on fa.film_id = f.film_id
        where upper(a.first_name) = upper('negishi') 
          and upper(a.last_name) = upper('toshie')
          and upper(f.title) = upper('dreams')
    )
    union all
    select 
        (select a.actor_id 
         from public.actor a
         where upper(a.first_name) = upper('harada') 
           and upper(a.last_name) = upper('mieko')) as actor_id,
        (select f.film_id 
         from public.film f
         where upper(f.title) = upper('dreams')) as film_id
    where not exists (
        select 1 
        from public.film_actor fa
        join public.actor a on fa.actor_id = a.actor_id
        join public.film f on fa.film_id = f.film_id
        where upper(a.first_name) = upper('harada') 
          and upper(a.last_name) = upper('mieko')
          and upper(f.title) = upper('dreams')
    )
    union all
    select 
        (select a.actor_id 
         from public.actor a
         where upper(a.first_name) = upper('irene') 
           and upper(a.last_name) = upper('jacob')) as actor_id,
        (select f.film_id 
         from public.film f
         where upper(f.title) = upper('the double life of veronique')) as film_id
    where not exists (
        select 1 
        from public.film_actor fa
        join public.actor a on fa.actor_id = a.actor_id
        join public.film f on fa.film_id = f.film_id
        where upper(a.first_name) = upper('irene') 
          and upper(a.last_name) = upper('jacob')
          and upper(f.title) = upper('the double life of veronique')
    )
    union all
    select 
        (select a.actor_id 
         from public.actor a
         where upper(a.first_name) = upper('aleksander') 
           and upper(a.last_name) = upper('bardini')) as actor_id,
        (select f.film_id 
         from public.film f
         where upper(f.title) = upper('the double life of veronique')) as film_id
    where not exists (
        select 1 
        from public.film_actor fa
        join public.actor a on fa.actor_id = a.actor_id
        join public.film f on fa.film_id = f.film_id
        where upper(a.first_name) = upper('aleksander') 
          and upper(a.last_name) = upper('bardini')
          and upper(f.title) = upper('the double life of veronique')
    )
    union all
    select 
        (select a.actor_id 
         from public.actor a
         where upper(a.first_name) = upper('natalya') 
           and upper(a.last_name) = upper('bondarchuk')) as actor_id,
        (select f.film_id 
         from public.film f
         where upper(f.title) = upper('solyaris')) as film_id
    where not exists (
        select 1 
        from public.film_actor fa
        join public.actor a on fa.actor_id = a.actor_id
        join public.film f on fa.film_id = f.film_id
        where upper(a.first_name) = upper('natalya') 
          and upper(a.last_name) = upper('bondarchuk')
          and upper(f.title) = upper('solyaris')
    )
    union all
    select 
        (select a.actor_id 
         from public.actor a
         where upper(a.first_name) = upper('donatas') 
           and upper(a.last_name) = upper('banionis')) as actor_id,
        (select f.film_id 
         from public.film f
         where upper(f.title) = upper('solyaris')) as film_id
    where not exists (
        select 1 
        from public.film_actor fa
        join public.actor a on fa.actor_id = a.actor_id
        join public.film f on fa.film_id = f.film_id
        where upper(a.first_name) = upper('donatas') 
          and upper(a.last_name) = upper('banionis')
          and upper(f.title) = upper('solyaris')
    )
) subquery
where actor_id is not null and film_id is not null
returning *;


-- adding movies to inventory
 
insert into public.inventory (film_id, store_id, last_update)
	select f.film_id,
	(select distinct(s.store_id) from public.store s
  		join public.address a on s.address_id= a.address_id
  		where  upper(a.address)=upper('28 mysql boulevard')
    ),current_date
	from public.store s
	cross join public.film f
	where upper(f.title) = upper('dreams')
  and not exists (
    select 1
    from public.inventory i
    where i.film_id = f.film_id 
      and i.store_id = s.store_id
  )
  returning * ;


 
insert into public.inventory (film_id, store_id, last_update)
select f.film_id, 
		(select distinct(s.store_id) from public.store s
  		join public.address a on s.address_id= a.address_id
  		where  upper(a.address)=upper('28 mysql boulevard')
    ), current_date
from public.store s
cross join public.film f
where upper(f.title) = upper('solyaris')
     and 
     not exists(
    select 1
    from inventory i
    where i.film_id = f.film_id 
      and i.store_id = s.store_id
  )
  returning *;

insert into public.inventory (film_id, store_id, last_update)
select f.film_id,
	(select distinct(s.store_id) from public.store s
  		join public.address a on s.address_id= a.address_id
  		where  upper(a.address)=upper('47 mysakila drive')
    ), current_date
from public.store s
cross join public.film f
where upper(f.title) = upper('the double life of veronique')
  and not exists (
    select 1
    from inventory i
    where i.film_id = f.film_id 
      and i.store_id = s.store_id
  )
       returning *;
       
      /* here I didn't use exact ids and because of that I used two store and film tables. 
  		 I needed cross join because it creates cartesian product of store and film tables and then 
       	 I filter data with where conditions.
       */ 
       
       --altering  any existing customer in the database with at least 43 rental and payment records
       update public.customer 
       set first_name ='Daviti',
       	   last_name ='Matiashvili',
           email = 'datomatiashvili91@gmail.com',
           last_update =current_date
       where customer_id in(
       select c.customer_id 	
       from public.customer c 
       join public.payment p on c.customer_id=p.customer_id 
       join public.rental r on p.rental_id =r.rental_id 
       group by c.customer_id 
       having count(distinct p.payment_id)>=43 and count(distinct r.rental_id)>=43
       limit 1)
       returning *
       
      
       
       /* I guess in this task main point was to write the subquery correctly.so I just explain it.
         I joined 3 tables customer,payment and rental. at first join we get the customers who have
         payment records and at the second join we get their rental records. we need group by c.customer_id
         to get every customer which has more records than 43. than in having clause I wrote
         count(distinct p.payment_id)>=43 and count(distinct r.rental_id)>=43 with this I count 
         every distinct payment and rental record for every customer and then I wrote limit 1 because
         we needed only one customer.then I updated her first_name, last_name and  email.
        */
       
       
       
   --Remove any records related to you (as a customer) from all tables except 'Customer' and 'Inventory'
		
	/*WITH removing_me AS (
    SELECT customer_id 
    FROM customer 
    WHERE upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI'
)
DELETE FROM rental 
WHERE customer_id IN (SELECT customer_id FROM removing_me);

		I tried this  and it gives me an error message- SQL Error [42703]: ERROR: column "removing_me" does not exist
  Position: 51 
  		I don't know how to fix this.so I wrote whole query.
  		*/
       					
        delete from public.payment 
       where payment.customer_id in(select customer_id from public.customer 
       								where upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI')			
       
       delete from public.rental 
       where rental.customer_id in(select customer_id from public.customer 
       								where upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI')
       
      --there was only these two tables with customer_id so I deleted every record.
       		
       				
       	--Rent you favorite movies from the store they are in and pay for them (add corresponding records to the database to represent this activity)							
       					
		insert into public.rental(rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
select 
    rental_data.rental_date,
    rental_data.inventory_id,
    rental_data.customer_id,
    rental_data.return_date,
    rental_data.staff_id,
    current_date
from (
    select 
        '2017-02-19'::timestamptz as rental_date,
        i.inventory_id,
        c.customer_id,
        '2017-02-20'::timestamptz as return_date,
        (select distinct (s.staff_id) 
         from public.staff s
         join public.rental r on s.staff_id = r.staff_id 
         join public.inventory i on r.inventory_id = i.inventory_id 
         join public.film f on i.film_id = f.film_id
         where upper(s.first_name) = upper('Mike') 
         and upper(s.last_name) = upper('hillyer')) as staff_id
    from inventory i
    join film f on i.film_id = f.film_id
    cross join customer c
    where upper(f.title) = 'SOLYARIS'
      and upper(c.first_name) = 'DAVITI'
      and upper(c.last_name) = 'MATIASHVILI'

    union all

    select 
        '2017-01-10 10:23:21'::timestamptz,
        i.inventory_id,
        c.customer_id,
        '2017-01-15'::timestamptz,
        (select distinct (s.staff_id) 
         from public.staff s
         join public.rental r on s.staff_id = r.staff_id 
         join public.inventory i on r.inventory_id = i.inventory_id 
         join public.film f on i.film_id = f.film_id
         where upper(s.first_name) = upper('Mike') 
         and upper(s.last_name) = upper('hillyer')) as staff_id
    from inventory i
    join film f on i.film_id = f.film_id
    cross join customer c
    where upper(f.title) = 'DREAMS'
      and upper(c.first_name) = 'DAVITI'
      and upper(c.last_name) = 'MATIASHVILI'

    union all

    select 
        '2017-03-06'::timestamptz,
        i.inventory_id,
        c.customer_id,
        '2017-03-10'::timestamptz,
        (select distinct (s.staff_id) 
         from public.staff s
         join public.rental r on s.staff_id = r.staff_id 
         join public.inventory i on r.inventory_id = i.inventory_id 
         join public.film f on i.film_id = f.film_id
         where upper(s.first_name) = upper('Mike') 
         and upper(s.last_name) = upper('hillyer')) as staff_id
    from inventory i
    join film f on i.film_id = f.film_id
    cross join customer c
    where upper(f.title) = 'THE DOUBLE LIFE OF VERONIQUE'
      and upper(c.first_name) = 'DAVITI'
      and upper(c.last_name) = 'MATIASHVILI'
) as rental_data
where not exists (
    select 1
    from rental r
    where r.inventory_id = rental_data.inventory_id
      and r.customer_id = rental_data.customer_id
      and r.rental_date = rental_data.rental_date
) returning *;

				
	/* we have to insert the data into rental table before the payment table so here i just added the data and filtered it
	 based on customer first_name last_name and film_title. i used cross join customer c because I needed some information
	 about customer for filtering data.
	 */		
				insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
select 
    c.customer_id,
    (select distinct s.staff_id
     from public.staff s
     join public.rental r on s.staff_id = r.staff_id
     join public.inventory i on r.inventory_id = i.inventory_id
     join public.film f on i.film_id = f.film_id
     where upper(s.first_name) = upper('Mike') 
       and upper(s.last_name) = upper('hillyer')) as staff_id,
    r.rental_id,
    f.rental_rate as amount,
    '2017-02-20 14:45:00'::timestamp as payment_date
from customer c
join rental r on c.customer_id = r.customer_id
join film f on f.film_id = r.inventory_id
where upper(c.first_name) = 'DAVITI' 
  and upper(c.last_name) = 'MATIASHVILI' 
  and upper(f.title) = 'SOLYARIS'
  and not exists (
    select 1
    from payment p
    where p.customer_id = c.customer_id
      and p.rental_id = r.rental_id
      and p.payment_date = '2017-02-20 14:45:00'::timestamp
)

union all

select 
    c.customer_id,
    (select distinct s.staff_id
     from public.staff s
     join public.rental r on s.staff_id = r.staff_id
     join public.inventory i on r.inventory_id = i.inventory_id
     join public.film f on i.film_id = f.film_id
     where upper(s.first_name) = upper('jon') 
       and upper(s.last_name) = upper('stephens')) as staff_id,
    r.rental_id,
    f.rental_rate as amount,
    '2017-01-15 10:30:00'::timestamp as payment_date
from customer c
join rental r on c.customer_id = r.customer_id
join film f on f.film_id = r.inventory_id
where upper(c.first_name) = 'DAVITI' 
  and upper(c.last_name) = 'MATIASHVILI' 
  and upper(f.title) = 'DREAMS'
  and not exists (
    select 1
    from payment p
    where p.customer_id = c.customer_id
      and p.rental_id = r.rental_id
      and p.payment_date = '2017-01-15 10:30:00'::timestamp
)

union all

select 
    c.customer_id,
    (select distinct s.staff_id
     from public.staff s
     join public.rental r on s.staff_id = r.staff_id
     join public.inventory i on r.inventory_id = i.inventory_id
     join public.film f on i.film_id = f.film_id
     where upper(s.first_name) = upper('Mike') 
       and upper(s.last_name) = upper('hillyer')) as staff_id,
    r.rental_id,
    f.rental_rate as amount,
    '2017-03-10 09:15:00'::timestamp as payment_date
from customer c
join rental r on c.customer_id = r.customer_id
join film f on f.film_id = r.inventory_id
where upper(c.first_name) = 'DAVITI' 
  and upper(c.last_name) = 'MATIASHVILI' 
  and upper(f.title) = 'THE DOUBLE LIFE OF VERONIQUE'
  and not exists (
    select 1
    from payment p
    where p.customer_id = c.customer_id
      and p.rental_id = r.rental_id
      and p.payment_date = '2017-03-10 09:15:00'::timestamp
)
returning *;

		/* and here I did everything samely, there was just one problem about timestamp casting and I just casted some random
		 date to timestamp and that's it. I think that's all for the Task 1. 
		 */
				 
				
		
              
       
			

 