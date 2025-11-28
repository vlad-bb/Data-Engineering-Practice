/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
select 
	c."name", 
	count(fc.film_id) as films_count
from category c
join film_category fc on c.category_id = fc.category_id
group by c."name"
order by films_count desc;



/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
select 
	a.actor_id, 
	CONCAT(a.first_name, ' ', a.last_name) AS full_name, 
	count(*) as films_count
from rental r
join inventory i on r.inventory_id = i.inventory_id
join film f on i.film_id = f.film_id
join film_actor fa on f.film_id = fa.film_id
join actor a on fa.actor_id = a.actor_id
group by a.actor_id
order by films_count desc
limit 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
with film_sales as (
  select i.film_id, SUM(p.amount) as total_sales
  from payment p
  join rental r on p.rental_id = r.rental_id
  join inventory i on r.inventory_id = i.inventory_id
  group by i.film_id
)
select 
  c.name as category,
  SUM(fs.total_sales) as total_sales
from film_sales fs
join film_category fc on fs.film_id = fc.film_id
join category c on fc.category_id = c.category_id
group by c.name
order by total_sales desc
limit 1;



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
select f.title 
from film f
left join inventory i on f.film_id = i.film_id
where i.inventory_id is Null


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
with target_category as (
	select c.category_id
	from category c
	where c."name" = 'Children'
)
select 
	CONCAT(a.first_name, ' ', a.last_name) AS full_name,
	count(*) as films_count
from target_category tc
join film_category fc on tc.category_id = fc.category_id
join film f on fc.film_id = f.film_id
join film_actor fa on f.film_id = fa.film_id
join actor a on fa.actor_id = a.actor_id
group by a.actor_id
order by films_count desc
limit 3;

