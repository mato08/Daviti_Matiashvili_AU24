--task 1
create or replace view  sales_revenue_by_category_qtr as
select 
  c."name" as category_name, 
  sum(p.amount) as total_sales_revenue,
  extract(year from current_date) as current_year,
  extract(quarter from current_date) as current_quarter
from public.category c
join public.film_category fc on c.category_id = fc.category_id
join public.film f on fc.film_id = f.film_id
join public.inventory i on f.film_id = i.film_id
join public.rental r on i.inventory_id = r.inventory_id
join public.payment p on r.rental_id = p.rental_id
where extract(year from p.payment_date) = extract(year from current_date)
  and extract(quarter from current_date)=extract(quarter from p.payment_date)
group by c."name",extract(quarter from current_date);


--task 2

create or replace function get_sales_revenue_by_category_qtr(quarter_and_year date)
returns table(f_category text,
			  total_sales numeric,
			  current_quarter int,
			  current_year int)
as $$
select
	c."name" as f_category,
	sum(p.amount) as total_sales,
	extract(quarter from quarter_and_year) as current_quarter,
	extract(year from quarter_and_year) as current_year
	from public.category c
join public.film_category fc on c.category_id = fc.category_id
join public.film f on fc.film_id = f.film_id
join public.inventory i on f.film_id = i.film_id
join public.rental r on i.inventory_id = r.inventory_id
join public.payment p on r.rental_id = p.rental_id
where extract(year from p.payment_date) = extract(year from quarter_and_year)
  and extract(quarter from p.payment_date)=extract(quarter from quarter_and_year)
group by c."name",extract(quarter from quarter_and_year)
$$
language sql;

--- task 3
drop function if exists public.get_most_popular_film(text[])

create or replace function get_most_popular_film(input_country_name text[])
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
begin
    for idx in 1..array_length(input_country_name, 1) loop
        if not exists(select 1 from public.country c
                      where upper(c.country) = upper(input_country_name[idx])) then 
            raise exception 'Country "%" is not in the database', input_country_name[idx];
        end if;

        return query
        select 
            c.country as country_name,
            f.title as film_title,
            f.rating as film_rating,
            l.name as film_language,
            f.length as film_length,
            f.release_year as film_release_year
        from public.country c
        join public.city ci on c.country_id = ci.country_id
        join public.address a on ci.city_id = a.city_id
        join public.customer cu on a.address_id = cu.address_id
        join public.rental r on cu.customer_id = r.customer_id 
        join public.inventory inv on r.inventory_id = inv.inventory_id
        join public.film f on inv.film_id = f.film_id
        join public."language" l on f.language_id = l.language_id
        where upper(c.country) = upper(input_country_name[idx])
        group by c.country, l."name", f.film_id
        order by count(r.rental_id) desc
        fetch first 1 row with ties;
    end loop;
end;
$$
language plpgsql;
/*
 * this task was the hardest for me.maybe because I didn't know how to return most populars without using subquery or cte
 * but then it gave me an error so i used this approach.I hope fetch first with ties is correct also.If there is 
 * more easier way of doing this kind of tasks I will be grateful if you share it with me.
 */

---task 4
create or replace function films_in_stock_by_title(title_part text)
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
        select 1
        from public.film f
        join inventory i on f.film_id = i.film_id
        left join rental r on i.inventory_id = r.inventory_id
        where upper(f.title) like '%' || upper(title_part) || '%'
          and(
              not exists (
                  select 1 
                  from rental r_sub
                  where r_sub.inventory_id = i.inventory_id
              )
              or 
              not exists (
                  select 1 
                  from rental r_sub
                  where r_sub.inventory_id = i.inventory_id
                    and r_sub.return_date is null
              )
          )
    ) 
		then
        raise exception 'no in-stock films found with title containing: "%".', title_part;
    	end if;
	 
    for film_title, film_language, customer_name, rental_date 
		in
        	select 
            distinct(f.title) as film_title,
            l.name as film_language,
            concat(c.first_name, ' ', c.last_name) as customer_name,
            r.rental_date as rental_date
        from public.film f
        join inventory i on f.film_id = i.film_id
        join rental r on i.inventory_id = r.inventory_id
        join customer c on r.customer_id = c.customer_id
        join "language" l on f.language_id = l.language_id
        where upper(f.title) ilike '%' || upper(title_part) || '%'
    loop 
		row_num:=current_row;
        return next;
        current_row := current_row+1;
    end loop;
    return;
end;
$$ language plpgsql;
--function inventory_in_stock helped me a lot. I just implemented its logic here with small additions.

--task 5
create or replace function new_movie(film_title text, lang_name text default 'klingon')
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
declare
    rel_year public."year" := extract(year from current_date);
begin
    if exists (
        select 1 from public.film f
        where upper(f.title) = upper(film_title)
    ) then
        raise exception 'The film "%" is already in the films table', film_title;
    end if;

    if not exists (
        select 1 from public.language l
        where upper(l.name) = upper(lang_name)
    ) then
        raise exception 'Language "%" does not exist in the language table', lang_name;
    end if;

    return query
    insert into public.film (title, release_year, language_id, rental_duration, rental_rate, replacement_cost, fulltext)
    values (
        film_title,
        rel_year,
        (select l.language_id from public.language l where upper(l.name) = upper(lang_name)),
        3,
        4.99,
        19.99,
        to_tsvector(film_title)
    )
    returning *;

end;
$$ language plpgsql;
