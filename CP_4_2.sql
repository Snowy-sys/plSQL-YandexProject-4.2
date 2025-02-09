/*АНАЛИЗ И ОПТИМИЗАЦИЯ ЗАПРОСОВ*/
/*После активации модуля pg_stat_statements, узнать id базы для выборки медленных запросов*/
SELECT oid, datname from pg_database;

--1. Определить 5 медленных запросов из выборки user_scripts_pr4.sql
SELECT
    query,
    ROUND(mean_exec_time::numeric,2),
    ROUND(total_exec_time::numeric,2),
    ROUND(min_exec_time::numeric,2),
    ROUND(max_exec_time::numeric,2),
    calls,
    rows
FROM pg_stat_statements
WHERE dbid = 28877 --Значение dbid у каждого своё (у меня 28877)
ORDER BY mean_exec_time desc 
LIMIT 5;

/*ИТОГ*/
--Таким образом получаем 5 САМЫХ МЕДЛЕННЫХ ЗАПРОСОВ: №2, №7, №8, №9, №15

/*ОПТИМИЗАЦИЯ ЗАПРОСОВ*/
/*ЗАПРОС №2*/
-- 1. Проанализировать план запросов. Как итог, наличие медленной операции соединения Nested loop 
--помимо этого, отсутствие индекса в таблице order_statuses на поля order_id, status_id
EXPLAIN ANALYSE VERBOSE
select o.order_id, o.order_dt, o.final_cost, s.status_name
from order_statuses os
join orders o on o.order_id = os.order_id
join statuses s on s.status_id = os.status_id
where o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
  and os.status_dt in (
    select max(status_dt)
    from order_statuses
    where order_id = o.order_id
);

-- 2. Создание индекса на таблицу order_statuses с полями order_id, status_id
CREATE INDEX if not exists order_statuses_order_id_status_id_idx 
on order_statuses (order_id, status_id);

-- 3. Успростить изначальный запрос с использованием CTE и рангов
WITH user_orders as(
SELECT
	o.order_id,
    o.order_dt,
    os.status_dt,
    o.final_cost,
    os.status_id,
    rank() over (partition by o.order_id order by os.status_dt desc) as rank
FROM orders o
LEFT JOIN order_statuses os on o.order_id = os.order_id
WHERE user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
ORDER BY order_id, os.status_dt
)
SELECT
    uo.order_id,
    uo.order_dt,
    uo.final_cost,
    s.status_name
from user_orders uo
left join statuses s on uo.status_id = s.status_id
where uo.rank = 1;


--4. Проанализировать план запроса после оптимизации. Время выполнения запроса уменьшилось
EXPLAIN ANALYSE VERBOSE
WITH user_orders as(
SELECT
	o.order_id,
    o.order_dt,
    os.status_dt,
    o.final_cost,
    os.status_id,
    rank() over (partition by o.order_id order by os.status_dt desc) as rank
FROM orders o
LEFT JOIN order_statuses os on o.order_id = os.order_id
WHERE user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
ORDER BY order_id, os.status_dt
)
SELECT
    uo.order_id,
    uo.order_dt,
    uo.final_cost,
    s.status_name
FROM user_orders uo
LEFT JOIN statuses s on uo.status_id = s.status_id
WHERE uo.rank = 1;

/*ЗАПРОС №7*/
--1. Проанализировать план запроса. В запросе производится сканирование каждой партиции, 
--что в свою очередь замедляет работу запроса. Один из вариантов решения, добавить индексы на партиции
EXPLAIN ANALYSE VERBOSE
select event, datetime
from user_logs
where visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
order by 2;

--2. Создать индексы на партиции на поле уникального идентификатора visitor_uuid
CREATE INDEX if not exists  user_logs_visitor_uuid_idx on user_logs ((visitor_uuid::text));
CREATE INDEX if not exists  user_logs_y2021q2_visitor_uuid_idx on user_logs_y2021q2 ((visitor_uuid::text));
CREATE INDEX if not exists  user_logs_y2021q3_visitor_uuid_idx on user_logs_y2021q3 ((visitor_uuid::text));
CREATE INDEX if not exists  user_logs_y2021q4_visitor_uuid_idx on user_logs_y2021q4 ((visitor_uuid::text));

--3. Проанализировать план запроса после оптимизации. На партиции применяется сканирование Bitmap Index Scan,
--что в свою очередь ускоряет время вызова запроса в 2-3 раза
EXPLAIN ANALYSE VERBOSE
SELECT
	event, 
	datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

/*ЗАПРОС №8*/
--1. Проанализировать план запроса. Проблема аналогичная: отсутствие индексов в партициях,
--что приводит к замедлению запроса.
EXPLAIN ANALYSE VERBOSE
select *
from user_logs
where datetime = current_date;

--ВАРИАНТ №1
--2. Создать индексы на партиции на поле datetime
CREATE INDEX if not exists  user_logs_y2021q2_datetime_idx on user_logs_y2021q2 ((datetime::date));
CREATE INDEX if not exists  user_logs_y2021q3_datetime_idx on user_logs_y2021q3 ((datetime::date));
CREATE INDEX if not exists  user_logs_y2021q4_datetime_idx on user_logs_y2021q4 ((datetime::date));

--3. Скорректировать изначальный запрос с явным преобразованием типа date
SELECT *
FROM user_logs
WHERE datetime::date = current_date;

--4. Проанализировать план запроса после оптимизации. Время исполнения запроса уменьшилось, 
--т.к. к партициям применяется сканирование по созданному индексу datetime
EXPLAIN ANALYSE VERBOSE
SELECT *
FROM user_logs
WHERE datetime::date = current_date;

--ВАРИАНТ №2
--2. Создать индексы на партиции на поле log_date
CREATE INDEX if not exists  user_logs_y2021q2_log_date_idx on user_logs_y2021q2 (log_date);
CREATE INDEX if not exists  user_logs_y2021q3_log_date_idx on user_logs_y2021q3 (log_date);
CREATE INDEX if not exists  user_logs_y2021q4_log_date_idx on user_logs_y2021q4 (log_date);

--3. Скорректировать изначальный запрос
SELECT *
FROM user_logs
WHERE log_date = current_date;

--4. Проанализировать план запроса после оптимизации. Время исполнения запроса уменьшилось, 
--т.к. к партициям применяется сканирование по созданному индексу log_date
EXPLAIN ANALYSE VERBOSE
SELECT *
FROM user_logs
WHERE log_date = current_date;

/*ЗАПРОС №9*/
--1. Проанализировать план запроса. Из явных проблем - наличие медленной операции соединения Nested loop,
--отсутствие индексов, а также не совсем эффективный запрос с использованием подзапроса в условии.
EXPLAIN ANALYSE VERBOSE
select count(*)
from order_statuses os
         join orders o ON o.order_id = os.order_id
where (select count(*)
       from order_statuses os1
       where os1.order_id = o.order_id and os1.status_id = 2) = 0
  and o.city_id = 1;

--2. Создать индексы на таблицу order_statuses и orders на поля status_id и city_id соответственно.
CREATE INDEX if not exists orders_statuses_status_id_idx on order_statuses (status_id);
CREATE INDEX if not exists  orders_city_id_idx on orders (city_id);

--3. Оптимизировать запрос с использованием CTE, с целью избавления от операции соединения Nested loop
WITH non_paid_orders as (
SELECT order_id
FROM order_statuses
WHERE order_statuses.order_id not in (
	SELECT order_id
    from order_statuses
    where status_id = 2)
)
SELECT count(*)
FROM non_paid_orders np
LEFT JOIN orders o on o.order_id = np.order_id
WHERE o.city_id = 1;

--4. Проанализировать план запроса после оптимизации. Время исполнения запроса уменьшилось за счёт принятых действий
EXPLAIN ANALYSE VERBOSE
WITH non_paid_orders as (
SELECT order_id
FROM order_statuses
WHERE order_statuses.order_id not in (
	SELECT order_id
    from order_statuses
    where status_id = 2)
)
SELECT count(*)
FROM non_paid_orders np
LEFT JOIN orders o on o.order_id = np.order_id
WHERE o.city_id = 1;

/*ЗАПРОС №15*/
--1. Проанализировать план запроса. Из явных проблем замедления запроса - это повторы и отсутвие индексов
--Необходимо составить запрос с использованием CTE и создать индексы
EXPLAIN ANALYSE VERBOSE
select
    d.name,
    SUM(count) as orders_quantity
from order_items oi
join dishes d on d.object_id = oi.item
where oi.item in (
    select item
    from (select item, SUM(count) AS total_sales
          from order_items oi
          group by 1) dishes_sales
    where dishes_sales.total_sales > (
        select SUM(t.total_sales)/ COUNT(*)
        from (select item, SUM(count) as total_sales
              from order_items oi
              group by
                  1) t)
)
group by 1
order by orders_quantity desc;

--2. Создать индексы на таблицы order_items и dishes на поля item и object_id соответственно
CREATE INDEX if not exists  order_items_item_idx on order_items (item);
CREATE INDEX if not exists  dishes_object_id_idx on dishes (object_id);

--3. Оптимизировать запрос с использованием CTE
WITH extra_sales as (
SELECT
	item,
    SUM(count) AS total_sales
FROM order_items oi
GROUP BY 1
)
SELECT
    d.name,
    SUM(count) as orders_quantity
FROM order_items oi
JOIN dishes d on d.object_id = oi.item
WHERE oi.item in (
	SELECT item
    from extra_sales es
    where es.total_sales > (select SUM(total_sales)/ COUNT(*)from extra_sales)
)
GROUP BY 1
ORDER BY orders_quantity desc;

--4. Проанализировать запрос после оптимизации. Время исполнения запроса уменьшилось за счёт принятых действий
EXPLAIN ANALYSE VERBOSE
WITH extra_sales as (
SELECT
	item,
    SUM(count) AS total_sales
FROM order_items oi
GROUP BY 1
)
SELECT
    d.name,
    SUM(count) as orders_quantity
FROM order_items oi
JOIN dishes d on d.object_id = oi.item
WHERE oi.item in (
	SELECT item
    from extra_sales es
    where es.total_sales > (select SUM(total_sales)/ COUNT(*)from extra_sales)
)
GROUP BY 1
ORDER BY orders_quantity desc;