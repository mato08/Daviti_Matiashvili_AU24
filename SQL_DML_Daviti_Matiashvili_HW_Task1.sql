   
      

-- first film
 insert into film(title,description,release_year,language_id,original_language_id,
				rental_duration,rental_rate,length,replacement_cost,
				rating,last_update,special_features,fulltext)
				
SELECT 'Solyaris', 'A psychologist is sent to a station orbiting a distant planet in order to discover what has caused the crew to go insane.', 
       1972, 1, null, 1, 4.99, 167, 16.99, 'PG', current_date, '{Trailers}', 'solyaris'
       returning * 
       
       -- second film
  insert into film(title,description,release_year,language_id,original_language_id,
				rental_duration,rental_rate,length,replacement_cost,
				rating,last_update,special_features,fulltext)
       
SELECT 'Dreams', 'A collection of tales based upon eight of director Akira Kurosawas recurring dreams.', 
       1990, 3, null, 2, 9.99, 119, 14.99, 'PG', current_date, '{Dreams}', 'Dreams'
       returning *
       
   		--third film    
  insert into film(title,description,release_year,language_id,original_language_id,
				rental_duration,rental_rate,length,replacement_cost,
				rating,last_update,special_features,fulltext)
				
SELECT 'The double life of veronique', 'Two parallel stories about two identical women; one living in Poland, the other in France. They dont know each other, but their lives are nevertheless profoundly connected.', 
       1991, 5, null, 3, 19.99, 98, 21.99, 'R', current_date, '{Behind the scenes}', 'The double life of veronique'
       returning *
       
       
      
       
       
       
     
       
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
       
       insert into actor(first_name,last_name,last_update)	  
       
       (select 'Negishi','Toshie',current_date
       union all 
       select 'Harada','Mieko', current_date
       union all 
       select 'Irene','Jacob',current_date
       union all
       select 'Aleksander','Bardini',current_date
       union all 
       select 'Natalya','Bondarchuk',current_date
       union all 'Donatas','Banionis',current_date
       )
       returning *
       
       
       
       --actor_id 204,205  film_id-1025
       --actor_id 206,207  film_id-1026
       --actor_id 208,209  film_id-1024
       
      
  insert into film_actor (actor_id,film_id,last_update)
       
     (select 204, 1025,current_date
       union all
       select 205,1025,current_date
       union all
       select 206,1026,current_date
       union all
       select 207,1026,current_date
       union all
       select 208,1024,current_date
       union all
       select 209,1024,current_date
       )
       returning *
       
       delete from film_actor 
       where actor_id in (204,205,206,207,208,209)
       
   
       /* here I again used union all and this time it worked. I don't know what is the problem with the first one.
         I thought about that using the exact numbers in select clause is not good practice. but then I wrote actor_id film_id
         joined the actor and film tables and wrote it like that but then query became massive so because of that I left it like this.
        */
       
       -- adding movies to inventory
       
       insert into inventory(film_id,store_id,last_update)
       
       (select f.film_id,s.store_id, current_date
       		from store s
       		cross join film f 
       		where s.store_id=1 and upper(f.title)='DREAMS'
       union all
       select f.film_id ,s.store_id ,current_date
       		from store s
       		cross join film f 
       		where s.store_id=2 and upper(f.title)='SOLYARIS'
       union all
       select f.film_id, s.store_id,current_date
       		  from store s 
       		  cross join film f 
       where s.store_id=1 and upper(f.title)='THE DOUBLE LIFE OF VERONIQUE'
       		)
       returning *
       
      /* here I didn't use exact ids and because of that I used two store and film tables. 
  		 I needed cross join because it creates cartesian product of store and film tables and then 
       	 I filter data with where conditions.
       */
       select * from customer c 
       
       --altering  any existing customer in the database with at least 43 rental and payment records
       update customer 
       set first_name ='Daviti',
       	   last_name ='Matiashvili',
           email = 'LINDA.WILLIAMS@sakilacustomer.org',
           last_update =current_date
       where customer_id in(
       select c.customer_id 	
       from customer c 
       join payment p on c.customer_id=p.customer_id 
       join rental r on p.rental_id =r.rental_id 
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
       					
        delete from payment 
       where payment.customer_id in(select customer_id from customer 
       								where upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI')			
       
       delete from rental 
       where rental.customer_id in(select customer_id from customer 
       								where upper(first_name) = 'DAVITI' AND upper(last_name) = 'MATIASHVILI')
       
      --there was only these two tables with customer_id so I deleted every record.
       		
       				 select * from rental r 
       				
       	--Rent you favorite movies from the store they are in and pay for them (add corresponding records to the database to represent this activity)							
       					
		insert into rental(rental_date,inventory_id,customer_id,return_date,staff_id,last_update)
		
			(select current_date,i.inventory_id,c.customer_id,null::timestamp,1,current_date
				from inventory i 
				join film f on i.film_id =f.film_id 
				cross join customer c
				where upper(f.title) ='SOLYARIS' and upper(c.first_name)='DAVITI' and upper(c.last_name)='MATIASHVILI' 
				
				union all
				
			select current_date,i.inventory_id,c.customer_id,null::timestamp,1,current_date
				from inventory i 
				join film f on i.film_id =f.film_id 
				cross join customer c
				where upper(f.title) ='DREAMS' and upper(c.first_name)='DAVITI' and upper(c.last_name)='MATIASHVILI' 
				
				union all
				
				select current_date,i.inventory_id,c.customer_id,null::timestamp,1,current_date
				from inventory i 
				join film f on i.film_id =f.film_id 
				cross join customer c
				where upper(f.title) ='THE DOUBLE LIFE OF VERONIQUE' and upper(c.first_name)='DAVITI' and upper(c.last_name)='MATIASHVILI' )
				returning *
				
	/* we have to insert the data into rental table before the payment table so here i just added the data and filtered it
	 based on customer first_name last_name and film_title. i used cross join customer c because I needed some information
	 about customer for filtering data.
	 */
				
				
				insert into payment(customer_id,staff_id,rental_id,amount,payment_date)
				
				(select c.customer_id,1,rental_id,f.rental_rate,'2017-02-20 14:45:00'::timestamp
				from customer c 
				join rental r ON c.customer_id =r.customer_id 
				cross join film f 
				where upper(c.first_name)='DAVITI' and upper(c.last_name)='MATIASHVILI' and upper(f.title)='SOLYARIS'
				
				union all
				
				select c.customer_id,1,rental_id,f.rental_rate,'2017-01-15 10:30:00'::timestamp
				from customer c 
				join rental r ON c.customer_id =r.customer_id 
				cross join film f 
				where upper(c.first_name)='DAVITI' and upper(c.last_name)='MATIASHVILI' and upper(f.title)='DREAMS'
				
				union all
				
				select c.customer_id,1,rental_id,f.rental_rate,'2017-05-10 09:15:00'::timestamp 
				from customer c 
				join rental r ON c.customer_id =r.customer_id 
				cross join film f 
				where upper(c.first_name)='DAVITI' and upper(c.last_name)='MATIASHVILI' 
				and upper(f.title)='THE DOUBLE LIFE OF VERONIQUE')
				returning *
				
		/* and here I did everything samely, there was just one problem about timestamp casting and I just casted some random
		 date to timestamp and that's it. I think that's all for the Task 1. 
		 */
				 
				
		
              
       
			

 