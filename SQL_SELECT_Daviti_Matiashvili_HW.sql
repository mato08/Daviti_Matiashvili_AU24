/* task1
1) select f.title from film f
join film_category fc 
on f.film_id =fc.film_id 
join category c  
on fc.category_id = c.category_id 
where c."name" ='Animation'
and f.release_year between 2017 and 2019
order by f.title

2)with problem2 as (
    select a.address as full_address, sum(p.amount) as revenue
    from  address a
    join customer c on a.address_id = c.address_id 
    join payment p on c.customer_id = p.customer_id  
    where p.payment_date >= '2017-03-01'
    group by a.address
    union all
    select 
        a.address2 as full_address, 
        sum(p.amount) as revenue
    from address a
    join customer c on a.address_id = c.address_id 
    join payment p on c.customer_id = p.customer_id  
    where p.payment_date >= '2017-03-01'
    group by a.address2 
)
select full_address, sum(revenue) as total_revenue
from problem2
group by full_address;
3)
SELECT a.first_name, a.last_name, COUNT(f.film_id) AS number_of_movies
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film f ON fa.film_id = f.film_id
WHERE f.release_year >= 2015
GROUP BY a.actor_id, a.first_name, a.last_name 
ORDER BY number_of_movies DESC
LIMIT 5; 
4)
select 
    f.release_year,
    coalesce(count(case when c.name = 'Drama' then 1 end), 0) as number_of_drama_movies,
    coalesce(count(case when c.name = 'Travel' then 1 end), 0) as number_of_travel_movies,
    coalesce(count(case when c.name = 'Documentary' then 1 end), 0) as number_of_documentary_movies
from film f
join film_category fc on f.film_id = fc.film_id
join category c on fc.category_id = c.category_id
where c.name in ('Drama', 'Travel', 'Documentary')
group by f.release_year
order by f.release_year desc;
 

5)
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    f.title AS horror_film,
    SUM(p.amount) AS total_paid
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id
JOIN  inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON i.film_id = fc.film_id
JOIN category c2 ON fc.category_id = c2.category_id
JOIN payment p ON r.rental_id = p.rental_id
WHERE c2.name = 'Horror'
GROUP BY c.customer_id, c.first_name, c.last_name, f.title;

Part 2
 1)  I wrote it with help of the chatGPT not everything but at least 50%
 
 WITH LastPayment AS (
    SELECT 
        p.staff_id,
        MAX(p.payment_date) AS last_payment_date
    FROM payment p
    WHERE EXTRACT(YEAR FROM p.payment_date) = 2017
    GROUP BY p.staff_id
) /*This CTE computes the most recent payment date for each staff member for payments made in 2017 and I'm 
going to use it in another select clause,for more simplicity. */

SELECT 
    s.staff_id,
    CONCAT(s.first_name, ' ', s.last_name) AS staff_name,
    SUM(p.amount) AS total_revenue,
    st.store_id AS last_store
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN staff s ON p.staff_id = s.staff_id
JOIN store st ON s.store_id = st.store_id
JOIN LastPayment lp ON p.staff_id = lp.staff_id 
                    AND p.payment_date = lp.last_payment_date
WHERE EXTRACT(YEAR FROM p.payment_date) = 2017
GROUP BY s.staff_id, s.first_name, s.last_name, st.store_id
ORDER BY total_revenue DESC
LIMIT 3; 



2)
select f.title,f.rating, count(f.film_id) as number_of_rentals 
from film f
join inventory i on f.film_id = i.film_id 
join rental r on i.inventory_id =i.inventory_id 
group by f.title ,f.film_id,f.rating 
order by number_of_rentals desc 
limit 5

Part 3)
variant 1-select 
	concat(a.first_name,' ', a.last_name),
	(2024- max(f.release_year)) as gap
from actor a 
join  film_actor fa  on a.actor_id =fa.actor_id 
join film f  on fa.film_id =f.film_id 
group by a.actor_id 
order by gap desc 
limit 1

variant 2-SELECT 
    CONCAT(a.first_name, ' ', a.last_name) AS actor_name,
    f1.release_year AS film_year_1,
    f2.release_year AS film_year_2,
    (f2.release_year - f1.release_year) AS gap
FROM 
    actor a
join film_actor fa1 ON a.actor_id = fa1.actor_id
join film_actor fa2 ON a.actor_id = fa2.actor_id
join film f1 ON fa1.film_id = f1.film_id
join film f2 ON fa2.film_id = f2.film_id
where 
    f1.release_year < f2.release_year
    and (f2.release_year - f1.release_year) = (
        select max(f4.release_year - f3.release_year)
        from film_actor fa3
        join film f3 ON fa3.film_id = f3.film_id
        join film_actor fa4 ON fa3.actor_id = fa4.actor_id AND fa4.actor_id = a.actor_id
        join film f4 ON fa4.film_id = f4.film_id
        where f3.release_year < f4.release_year
    )
order by
    gap desc
limit 1
*/


