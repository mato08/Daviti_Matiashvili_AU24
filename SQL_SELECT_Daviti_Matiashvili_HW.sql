 -- task 1 
select f.title 
from public.film f
join public.film_category fc on f.film_id = fc.film_id 
join public.category c on fc.category_id = c.category_id 
where upper(c."name") = 'ANIMATION' 
  and f.rental_rate > 1
  and f.release_year between 2017 and 2019
order by f.title;
--task 2

select 
    concat(a.address, ' ', a.address2) as addresses, 
    sum(p.amount) as revenue
from public.address a 
join public.store s on a.address_id = s.address_id
join public.inventory i on s.store_id = i.store_id 
join public.rental r on i.inventory_id = r.inventory_id 
join public.payment p on r.rental_id = p.rental_id 
where (extract(year from p.payment_date) = 2017 
  and extract(month from p.payment_date) >= 3) or extract(year from p.payment_date)>=2018
group by s.store_id, addresses;

/* it is not good to use string comparators on date so I used extract function to get the year and the month */


--task 3

select 
    a.first_name, 
    a.last_name, 
    count(f.film_id) as number_of_movies
from public.actor a
join public.film_actor fa on a.actor_id = fa.actor_id
join public.film f on fa.film_id = f.film_id
where f.release_year >= 2015
group by a.actor_id
order by number_of_movies desc
limit 5;


--task 4

select 
    f.release_year,
    count(case when upper(c.name) = 'DRAMA' then 1 end) as number_of_drama_movies,
    count(case when upper(c.name) = 'TRAVEL' then 1 end) as number_of_travel_movies,
    count(case when upper(c.name) = 'DOCUMENTARY' then 1 end) as number_of_documentary_movies
from public.film f
join public.film_category fc on f.film_id = fc.film_id
join public.category c on fc.category_id = c.category_id
where upper(c.name) in ('DRAMA', 'TRAVEL', 'DOCUMENTARY')
group by f.release_year 
order by f.release_year desc;
 
/* coalesce was not necessary because count() already treats null values as a 0 so I just deleted it*/
	
--task 5

select 
    concat(c.first_name, ' ', c.last_name) as customer,
    sum(p.amount) as amount_of_money,
    (string_agg(distinct(f.title), ', ')) as list_of_horrors
from public.customer c 
join public.payment p on c.customer_id = p.customer_id 
join public.rental r on p.rental_id = r.rental_id 
join public.inventory i on r.inventory_id = i.inventory_id 
join public.film f on i.film_id = f.film_id 
join public.film_category fc on f.film_id = fc.film_id 
join public.category c2 on fc.category_id = c2.category_id 
where upper(c2."name") = 'HORROR' 
group by c.customer_id;

/* I didn't display the list of horrors so I with help of the string_agg function now I display all the horror movies 
 * in one column
 * now I also added distinct(f.title) to remove duplicates
 */
 
 --part 2 task 1
 
 select  
    concat(s.first_name, ' ', s.last_name) as staff_name,
    sum(p.amount) as total_revenue,
    s.store_id as last_store
from public.staff s
join public.payment p on s.staff_id = p.staff_id
join public.store s2 on s.store_id = s2.store_id 
where extract(year from p.payment_date) = 2017
group by s.staff_id, s.store_id 
order by sum(p.amount) desc 
limit 3;
  
  /* I don't understand the logic of this task fully so this solution can be incorrect again but I will try to explain how
   * why i wrote this. I'm just summing the amount in payment table and just joining three tables. but the question for me 
   * was how to know which store was the last. if employee changed the store then in staff table store_id will be changed too
   * so because of that I decided to group them by the s.store_id and also display it.*/
  



--part 2. task 2
   

select 
    f.title,
    f.rating, 
    count(f.film_id) as number_of_rentals,
    case
        when f.rating = 'G' then 'all ages'
        when f.rating = 'PG' then 'recommended age 8+'
        when f.rating = 'PG-13' then 'recommended age 13+'
        when f.rating = 'R' then 'recommended age 17+'
        when f.rating = 'NC-17' then 'recommended age 18+'
    end as recommended_age
from public.film f
join public.inventory i on f.film_id = i.film_id 
join public.rental r on i.inventory_id = r.inventory_id 
group by f.film_id 
order by number_of_rentals desc 
limit 5;

/* here I just added cases for all the ratings. searched the excpected ages for mpa_rating in google and add the
 * recommended age column.
 */

-- task 3,variant 1
select 
    concat(a.first_name, ' ', a.last_name) as actor_name,
    (extract(year from current_date) - max(f.release_year)) as gap
from public.actor a 
join public.film_actor fa on a.actor_id = fa.actor_id 
join public.film f on fa.film_id = f.film_id 
group by a.actor_id 
having extract(year from current_date) - max(f.release_year) = (
    select max(inactive_years) 
    from (
        select extract(year from current_date) - max(f.release_year) as inactive_years
        from public.actor a
        join public.film_actor fa on a.actor_id = fa.actor_id 
        join public.film f on fa.film_id = f.film_id 
        group by a.actor_id 
    )
)
order by gap desc;
/* here in last solution i wrote limit 1 but that was incorrect because there can be several actors with the same gap
 * so with help of the subquery I changed limit 1 and now it shows all the actors with the maximum gap */

-- task 3 variant 2
select 
    concat(a.first_name, ' ', a.last_name) as actor_name,
    max(f2.release_year) - min(f1.release_year) as max_gap
from public.actor a
join public.film_actor fa1 on a.actor_id = fa1.actor_id
join public.film_actor fa2 on a.actor_id = fa2.actor_id
join public.film f1 on fa1.film_id = f1.film_id
join public.film f2 on fa2.film_id = f2.film_id
where f1.release_year < f2.release_year
group by a.actor_id
having max(f2.release_year) - min(f1.release_year) = (
    select max(max_gap)
    from (
        select max(f2.release_year) - min(f1.release_year) as max_gap
        from public.actor a
        join public.film_actor fa1 on a.actor_id = fa1.actor_id
        join public.film_actor fa2 on a.actor_id = fa2.actor_id
        join public.film f1 on fa1.film_id = f1.film_id
        join public.film f2 on fa2.film_id = f2.film_id
        where f1.release_year < f2.release_year
        group by a.actor_id
    ) 
);
    /* i had the same mistake here. I wrote limit 1 here too.here I changed it with the subquery too and now it also 
     * shows all the actors with the maximum gap
     */
  
  
  



