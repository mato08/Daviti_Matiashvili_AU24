--task 1
create or replace
view public.sales_revenue_by_category_qtr as
select
	c."name" as category_name,
	sum(p.amount) as total_sales_revenue,
	extract(year
from
	current_date) as current_year,
	extract(quarter
from
	current_date) as current_quarter
from
	public.category c
join public.film_category fc on
	c.category_id = fc.category_id
join public.film f on
	fc.film_id = f.film_id
join public.inventory i on
	f.film_id = i.film_id
join public.rental r on
	i.inventory_id = r.inventory_id
join public.payment p on
	r.rental_id = p.rental_id
where
	extract(year
from
	p.payment_date) = extract(year
from
	current_date)
	and extract(quarter
from
	current_date)= extract(quarter
from
	p.payment_date)
group by
	c.category_id,
	extract(quarter
from
	current_date)
having
	count(p.payment_id)>= 1;

--task 2

create or replace
function public.get_sales_revenue_by_category_qtr(quarter_and_year date)
returns table(
		f_category text,
		total_sales numeric,
		current_quarter int,
		current_year int	
)
as $$
select
	c."name" as f_category,
	sum(p.amount) as total_sales,
	extract(quarter
from
	quarter_and_year) as current_quarter,
	extract(year
from
	quarter_and_year) as current_year
from
	public.category c
join public.film_category fc on
	c.category_id = fc.category_id
join public.film f on
	fc.film_id = f.film_id
join public.inventory i on
	f.film_id = i.film_id
join public.rental r on
	i.inventory_id = r.inventory_id
join public.payment p on
	r.rental_id = p.rental_id
where
	extract(year
from
	p.payment_date) = extract(year
from
	quarter_and_year)
	and extract(quarter
from
	p.payment_date)= extract(quarter
from
	quarter_and_year)
group by
	c.category_id
having
	count(p.payment_id)>= 1
$$
language sql;


--for testing purposes
select
	public.get_sales_revenue_by_category_qtr('2017-01-24')
	
	
	--- task 3
create or replace
	function public.get_most_popular_film(input_country_name text[])
returns table(
    country_name text,
	film_title text,
	film_rating public."mpaa_rating",
	film_language bpchar(20),
	film_length int2,
	film_release_year public."year"
)
as $$
declare
    idx int;

uniq_countries text[] := array(
select
	distinct unnest(input_country_name));

begin
    for idx in 1.. array_length(uniq_countries, 1) loop
        if not exists (
select
	1
from
	public.country c
where
	upper(c.country) = upper(uniq_countries[idx])
        ) then
            raise notice 'country "%" is not in the database',
uniq_countries[idx];

continue;
end if;

return query
        select
	c.country as country_name,
	f.title as film_title,
	f.rating as film_rating,
	l.name as film_language,
	f.length as film_length,
	f.release_year as film_release_year
from
	public.country c
join public.city ci on
	c.country_id = ci.country_id
join public.address a on
	ci.city_id = a.city_id
join public.customer cu on
	a.address_id = cu.address_id
join public.rental r on
	cu.customer_id = r.customer_id
join public.inventory inv on
	r.inventory_id = inv.inventory_id
join public.film f on
	inv.film_id = f.film_id
join public."language" l on
	f.language_id = l.language_id
where
	upper(c.country) = upper(uniq_countries[idx])
group by
	c.country_id,
	l.language_id,
	f.film_id
order by
	count(r.rental_id) desc
        fetch first 1 row with ties;
end loop;
end;

$$ language plpgsql;

--function call
select * from get_most_popular_film(array['canada', 'canada', 'France']);


---task 4
create or replace
function public.films_in_stock_by_title(title_part text)
returns table(
    row_num int,
    film_title text,
    film_language bpchar(20),
    customer_name text,
    rental_date timestamptz
)
as $$
declare
    current_row int := 1;

begin
   if not exists (
select
	1
from
	public.film f
join public.inventory i on
	f.film_id = i.film_id
left join public.rental r on
	i.inventory_id = r.inventory_id
where
	upper(f.title) like '%' || upper(title_part) || '%'
	and(
              not exists (
	select
		1
	from
		rental r_sub
	where
		r_sub.inventory_id = i.inventory_id
              )
	or 
              not exists (
	select
		1
	from
		rental r_sub
	where
		r_sub.inventory_id = i.inventory_id
		and r_sub.return_date is null
              )
          )
    ) 
		then
        raise notice 'no in-stock films found with title containing: "%".',
title_part;
end if;

for film_title,
film_language,
customer_name,
rental_date 
		in
        	select
	distinct(f.title) as film_title,
	l.name as film_language,
	concat(c.first_name,
	' ',
	c.last_name) as customer_name,
	r.rental_date as rental_date
from
	public.film f
join public.inventory i on
	f.film_id = i.film_id
join public.rental r on
	i.inventory_id = r.inventory_id
join public.customer c on
	r.customer_id = c.customer_id
join public."language" l on
	f.language_id = l.language_id
where
	upper(f.title) ilike '%' || upper(title_part) || '%'
    loop 
		row_num := current_row;

return next;

current_row := current_row + 1;
end loop;

return;
end;

$$ language plpgsql;

--for testing purposes
select * from public.films_in_stock_by_title('love')

--task 5
create or replace function public.new_movie(film_title text,rel_year year default extract(year from current_date),lang_name text default 'Klingon')
returns table(
    film_id int,
    title text,
    description text,
    release_year public."year",
    language_id int2,
    original_language_id int2,
    rental_duration int2,
    rental_rate numeric(4,2),
    length int2,
    replacement_cost numeric(5,2),
    rating public."mpaa_rating",
    last_update timestamptz,
    special_features _text,
    fulltext tsvector
)
as $$
begin

	if exists(
		select 1 from public.film f
		where upper(f.title)=upper(film_title))
	then
		raise notice 'film with title % already exists',film_title;
		return;
	end if;

    if not exists (
        select 1 from public.language l
        where upper(l.name) = upper(lang_name)
    ) then
       insert into public."language"(name,last_update)
				select lang_name,current_date
		where not exists(
		select 1
		from public.language l
		where upper(l.name)=upper(lang_name));
    end if;

    return query
    insert into public.film (title, release_year, language_id, rental_duration, rental_rate, replacement_cost, fulltext)
     	select 
        film_title,
        rel_year,
        (select l.language_id from public.language l where upper(l.name) = upper(lang_name)),
        3,
        4.99,
        19.99,
        to_tsvector(film_title)
		
    returning *;
end;
$$ language plpgsql;


--function call for testing purposes
select * from public.new_movie('natvris khe',1971,'georgian');








