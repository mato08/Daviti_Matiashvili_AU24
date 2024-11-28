
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
       			select 1 from public.film f
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
	select (select distinct(f.film_id) from public.film f
	where	upper(f.title)=upper('dreams')),
	(select distinct(s.store_id) from public.store s
  		join public.address a on s.address_id= a.address_id
  		where  upper(a.address)=upper('28 mysql boulevard'))
  		,current_date
  where not exists (
    select 1
    from public.inventory i
    join public.film f  on i.film_id = f.film_id 
    join public.store s on i.store_id = s.store_id
    join public.address a on s.address_id= a.address_id
    where upper(f.title)=upper('dreams') and 
    upper(a.address)=upper('28 mysql boulevard') 
  )
  returning * ;


 
insert into public.inventory (film_id, store_id, last_update)
select (select f.film_id from public.film f
		where upper(f.title)=upper('solyaris')),
		(select distinct(s.store_id) from public.store s
  		join public.address a on s.address_id= a.address_id
  		where  upper(a.address)=upper('28 mysql boulevard')
    ), current_date
     where not exists(
    select 1
    from public.inventory i
    join public.film f on i.film_id=f.film_id 
    join public.store s on i.store_id=s.store_id
    join public.address a on s.address_id=a.address_id
    where upper(f.title)=upper('solyaris')
    and upper(a.address)=upper('28 mysql boulevard'))
  returning *;
 

insert into public.inventory (film_id, store_id, last_update)
select (select f.film_id from public.film f
		where upper(f.title)=upper('the double life of veronique')),
	(select s.store_id from public.store s
  		join public.address a on s.address_id= a.address_id
  		where  upper(a.address)=upper('47 mysakila drive'))
    , current_date
  where not exists (
    select 1
    from inventory i
    join public.film f on i.film_id=f.film_id
    join public.store s on i.store_id=s.store_id
    join public.address a on s.address_id=a.address_id
    where upper(f.title)=upper('the double life of veronique') and 
    upper(a.address)=upper('47 mysakila drive')
  )
 returning *;
       
     
       
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
       

       					
        delete from public.payment 
       where payment.customer_id in(select customer_id from public.customer 
       								where upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI')			
       
       delete from public.rental 
       where rental.customer_id in(select customer_id from public.customer 
       								where upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI')
       
      --there was only these two tables with customer_id so I deleted every record.
       		
       				
       	--Rent you favorite movies from the store they are in and pay for them (add corresponding records to the database to represent this activity)							

insert into public.rental(rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
		select '2017-02-19'::timestamptz,
			   (select i.inventory_id from public.inventory i
			   join public.film f on i.film_id=f.film_id
			   where upper(f.title)=upper('solyaris')),
			   (select c.customer_id from public.customer c
			   where upper(c.first_name)=upper('daviti') and
			   		 upper(c.last_name)=upper('matiashvili')),
			   '2017-02-20'::timestamptz,
			   (select s.staff_id from public.staff s
			   where upper(s.first_name)=upper('mike') and 
			   		 upper(s.last_name)=upper('hillyer')),
			   current_date
			   where not exists(
			   select 1 from public.rental r
			   join public.inventory i on r.inventory_id=i.inventory_id
			   join public.film f on i.film_id= f.film_id
			   join public.customer c on r.customer_id=c.customer_id
			   join public.staff s on r.staff_id=s.staff_id
			   where upper(f.title)=upper('solyaris') and
			   		 upper(c.first_name)=upper('daviti') and
			   		 upper(c.last_name)=upper('matiashvili') and
			   		 upper(s.first_name)=upper('mike') and 
			   		 upper(s.last_name)=upper('hillyer'))
			   	returning *;
			   		
			   
			   
			
 insert into public.rental(rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
		select '2017-01-10 10:23:21'::timestamptz,
			   (select i.inventory_id from public.inventory i
			   join public.film f on i.film_id=f.film_id
			   where upper(f.title)=upper('dreams')),
			   (select c.customer_id from public.customer c
			   where upper(c.first_name)=upper('daviti') and
			   		 upper(c.last_name)=upper('matiashvili')),
			   '2017-01-15'::timestamptz,
			   (select s.staff_id from public.staff s
			   where upper(s.first_name)=upper('mike') and 
			   		 upper(s.last_name)=upper('hillyer')),
			   current_date
			   where not exists(
			   select 1 from public.rental r
			   join public.inventory i on r.inventory_id=i.inventory_id
			   join public.film f on i.film_id= f.film_id
			   join public.customer c on r.customer_id=c.customer_id
			   join public.staff s on r.staff_id=s.staff_id
			   where upper(f.title)=upper('dreams') and
			   		 upper(c.first_name)=upper('daviti') and
			   		 upper(c.last_name)=upper('matiashvili') and
			   		 upper(s.first_name)=upper('mike') and 
			   		 upper(s.last_name)=upper('hillyer'))
			   	returning *;
		
			   
			   
insert into public.rental(rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
		select '2017-01-10 10:23:21'::timestamptz,
			   (select i.inventory_id from public.inventory i
			   join public.film f on i.film_id=f.film_id
			   where upper(f.title)=upper('the double life of veronique')),
			   (select c.customer_id from public.customer c
			   where upper(c.first_name)=upper('daviti') and
			   		 upper(c.last_name)=upper('matiashvili')),
			   '2017-01-15'::timestamptz,
			   (select s.staff_id from public.staff s
			   where upper(s.first_name)=upper('mike') and 
			   		 upper(s.last_name)=upper('hillyer')),
			   current_date
			   where not exists(
			   select 1 from public.rental r
			   join public.inventory i on r.inventory_id=i.inventory_id
			   join public.film f on i.film_id= f.film_id
			   join public.customer c on r.customer_id=c.customer_id
			   join public.staff s on r.staff_id=s.staff_id
			   where upper(f.title)=upper('the double life of veronique') and
			   		 upper(c.first_name)=upper('daviti') and
			   		 upper(c.last_name)=upper('matiashvili') and
			   		 upper(s.first_name)=upper('mike') and 
			   		 upper(s.last_name)=upper('hillyer'))
			   	returning *; 			   



--inserting data into table payment
			   
insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
		select
		(select c.customer_id from public.customer c
		where upper(c.first_name)=upper('daviti') and upper(c.last_name)=upper('matiashvili')),
		(select s.staff_id from public.staff s
		where upper(s.first_name)=upper('mike') and upper(s.last_name)=upper('hillyer')),
		(select distinct(r.rental_id) from public.rental r
		join public.inventory i on r.inventory_id=i.inventory_id
		join public.film f on i.film_id=f.film_id
		where upper(f.title)=upper('solyaris')),
		(select f.rental_rate from public.film f
		where upper(f.title)=upper('solyaris')),
		'2017-02-20 14:45:00'::timestamp
		where not exists(
		select 1 from public.payment p   
		join public.rental r on p.rental_id=r.rental_id
		join public.inventory i on r.inventory_id=i.inventory_id
		join public.film f on i.film_id=f.film_id
		join public.staff s on p.staff_id=s.staff_id
		join public.customer c on p.customer_id=c.customer_id
		where upper(c.first_name)=upper('daviti') and upper(c.last_name)=upper('matiashvili')
		and   upper(s.first_name)=upper('mike') and upper(s.last_name)=upper('hillyer')
		and   upper(f.title)=upper('solyaris'))
returning *;	



insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
		select
		(select c.customer_id from public.customer c
		where upper(c.first_name)=upper('daviti') and upper(c.last_name)=upper('matiashvili')),
		(select s.staff_id from public.staff s
		where upper(s.first_name)=upper('mike') and upper(s.last_name)=upper('hillyer')),
		(select distinct(r.rental_id) from public.rental r
		join public.inventory i on r.inventory_id=i.inventory_id
		join public.film f on i.film_id=f.film_id
		where upper(f.title)=upper('dreams')),
		(select f.rental_rate from public.film f
		where upper(f.title)=upper('dreams')),
		'2017-01-15 10:30:00'::timestamp
		where not exists(
		select 1 from public.payment p   
		join public.rental r on p.rental_id=r.rental_id
		join public.inventory i on r.inventory_id=i.inventory_id
		join public.film f on i.film_id=f.film_id
		join public.staff s on p.staff_id=s.staff_id
		join public.customer c on p.customer_id=c.customer_id
		where upper(c.first_name)=upper('daviti') and upper(c.last_name)=upper('matiashvili')
		and   upper(s.first_name)=upper('mike') and upper(s.last_name)=upper('hillyer')
		and   upper(f.title)=upper('dreams'))
returning *;	


insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
		select
		(select c.customer_id from public.customer c
		where upper(c.first_name)=upper('daviti') and upper(c.last_name)=upper('matiashvili')),
		(select s.staff_id from public.staff s
		where upper(s.first_name)=upper('mike') and upper(s.last_name)=upper('hillyer')),
		(select distinct(r.rental_id) from public.rental r
		join public.inventory i on r.inventory_id=i.inventory_id
		join public.film f on i.film_id=f.film_id
		where upper(f.title)=upper('the double life of veronique')),
		(select f.rental_rate from public.film f
		where upper(f.title)=upper('the double life of veronique')),
		'2017-03-10 09:15:00'::timestamp
		where not exists(
		select 1 from public.payment p   
		join public.rental r on p.rental_id=r.rental_id
		join public.inventory i on r.inventory_id=i.inventory_id
		join public.film f on i.film_id=f.film_id
		join public.staff s on p.staff_id=s.staff_id
		join public.customer c on p.customer_id=c.customer_id
		where upper(c.first_name)=upper('daviti') and upper(c.last_name)=upper('matiashvili')
		and   upper(s.first_name)=upper('mike') and upper(s.last_name)=upper('hillyer')
		and   upper(f.title)=upper('the double life of veronique'))
returning *;


/* I changed the whole logic of how i was inserting data in tables rental and payment. now I tested it and it 
 * doesn't produce duplicates and also I inserted data and there was no problem.so I think this solution should be
 * 100% correct now.
 */
				 
				
		
              
       
			

 