

-- FOODIE-FI CASE STUDY


-- A


SELECT 
    s.customer_id, p.plan_name, s.start_date
FROM
    subscriptions s
        JOIN
    plans p ON s.plan_id = p.plan_id; 





-- B


-- 1


SELECT 
    COUNT(DISTINCT customer_id) unique_customer
FROM
    subscriptions;



-- 2
 
 
SELECT 
    MONTH(start_date) AS months,
    COUNT(customer_id) AS number_of_customers
FROM
    subscriptions
GROUP BY months;



-- 3
SELECT 
    p.plan_name, p.plan_id, COUNT(*) AS count_of_events
FROM
    subscriptions s
        JOIN
    plans p ON p.plan_id = s.plan_id
WHERE
    s.start_date >= '2021-01-01'
GROUP BY p.plan_id , p.plan_name
ORDER BY p.plan_id;



-- 4

SELECT 
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100 / (SELECT 
                    COUNT(DISTINCT customer_id)
                FROM
                    subscriptions),
            1) AS percentage_customer_churn
FROM
    subscriptions
WHERE
    plan_id = 4;



-- 5


with base as(
select customer_id, start_date, plan_id, row_number() over(partition by customer_id order by plan_id asc) as rank_plan
from foodie_fi.subscriptions)
select 
count(case when plan_id = 4 then 1 end) as free_trial_churn,
round(100*(count(case when plan_id = 4 then 1 end)/count(*)),2) as churn_percentage
from base where rank_plan = 2;



-- 6

WITH next_plan_cte AS (
SELECT 
  customer_id, 
  plan_id, 
  LEAD(plan_id, 1) OVER(PARTITION BY customer_id ORDER BY plan_id) as next_plan
FROM foodie_fi.subscriptions)

SELECT 
  next_plan, 
  COUNT(*) AS conversions,
  ROUND(100 * COUNT(*) / (
    SELECT COUNT(DISTINCT customer_id) 
    FROM foodie_fi.subscriptions),1) AS conversion_percentage
FROM next_plan_cte
WHERE next_plan IS NOT NULL 
  AND plan_id = 0
GROUP BY next_plan
ORDER BY next_plan;


-- 7



-- 7


with customer_count as(
 select plan_id,plan_name,count(distinct customer_id) as count_customers
 from subscriptions
 join plans using (plan_id)
 where start_date <='2020-12-31'
 group by plan_name
 )
 select plan_id,plan_name,count_customers,
  round (100*(count_customers/(select sum(count_customers) from
 customer_count)),2) as percentage
 from customer_count
 order by plan-id;







WITH next_plan AS (
SELECT 
  customer_id, 
  plan_id, 
  start_date,
  LEAD(start_date, 1) OVER (PARTITION BY customer_id ORDER BY start_date) as next_date
FROM foodie_fi.subscriptions
WHERE start_date <= '2020-12-31'
),
customer_breakdown AS (
SELECT plan_id, COUNT(DISTINCT customer_id) AS customers
  FROM next_plan
  WHERE (next_date IS NOT NULL AND (start_date < '2020-12-31' AND next_date > '2020-12-31'))
    OR (next_date IS NULL AND start_date < '2020-12-31')
  GROUP BY plan_id)

SELECT plan_id, customers, 
  ROUND(100 * customers / (
    SELECT COUNT(DISTINCT customer_id) 
    FROM foodie_fi.subscriptions),1) AS percentage
FROM customer_breakdown
GROUP BY plan_id, customers
ORDER BY plan_id;


-- 8

SELECT 
  COUNT(DISTINCT customer_id) AS unique_customer
FROM foodie_fi.subscriptions
WHERE plan_id = 3
  AND start_date <= '2020-12-31';
  
  
  
  -- 9 
  WITH 
  trial_plan AS 
  (SELECT 
      customer_id, 
      start_date AS trial_date
  FROM subscriptions
  WHERE plan_id = 0
  ),
  annual_plan AS
  (SELECT 
      customer_id, 
      start_date AS annual_date
  FROM subscriptions
  WHERE plan_id = 3
  )

SELECT 
  ROUND(AVG(annual_date - trial_date),0) AS avg_days_to_upgrade
FROM trial_plan tp
JOIN annual_plan ap
  ON tp.customer_id = ap.customer_id;
  
  
  
  -- 10
  
  
  CREATE TABLE interval1(
   month_interval int,
   breakdown_period varchar(15));
INSERT INTO interval1(month_interval, breakdown_period)
VALUES
(1, '0 - 30 days'),
(2, '30 - 60 days'),	
(3, '60 - 90 days'),	
(4, '90 - 120 days'),
(5, '120 - 150 days'),	
(6, '150 - 180 days'),	
(7, '180 - 210 days'),	
(8, '210 - 240 days'),	
(9, '240 - 270 days'),	
(10, '270 - 300 days'),	
(11, '300 - 330 days'),	
(12, '330 - 360 days');

with base as (
select 
  customer_id, 
  plan_id, 
  start_date, 
  (lead(start_date) over(partition by customer_id order by start_date asc)) as lead_start_date
from foodie_fi.subscriptions
where plan_id in(0,3)),

tb as (select *, (lead_start_date - start_date) as diff from base where lead_start_date is not null),

tb1 as (select 
  (case 
  when diff < 30 then 1
  when diff < 60 then 2
  when diff <90  then 3
  when diff <120 then 4
  when diff <150 then 5
  when diff <180 then 6
  when diff <210 then 7
  when diff <240 then 8
  when diff <270 then 9
  when diff <300 then 10
  when diff <330 then 11
  when diff <360 then 12
  else diff
  end) as month_interval,
  count(*) as customers
from tb 
group by month_interval
order by month_interval)

select breakdown_period, customers
from tb1
join interval1
on tb1.month_interval = interval1.month_interval;



-- 11

with base as (select 
  customer_id, 
  plan_id, 
  start_date, 
  (lead(plan_id) over(partition by customer_id order by start_date asc)) as lead_plan_id,
  (lead(start_date) over(partition by customer_id order by start_date asc)) as lead_start_date
from foodie_fi.subscriptions
where extract(year from start_date) = 2020)

select count(*) as downgraged 
from base 
where lead_plan_id is not null and 
lead_plan_id = 2 
and plan_id = 1;







-- cache index





DROP TABLE payments;

CREATE  TABLE payments
SELECT 
  customer_id,
  plan_id,
  plan_name,
  DATE(payment_date) as payment_date,
  amount,
  RANK() OVER(
    PARTITION BY customer_id
    ORDER BY
      payment_date
  ) AS payment_order
FROM
  (
    SELECT
      customer_id,
      s.plan_id,
      plan_name,
      DATE_ADD(start_date, INTERVAL n.n MONTH) as payment_date,
      CAST(price AS decimal(5,2))  AS amount
    FROM
      subscriptions AS s
      JOIN plans AS p ON s.plan_id = p.plan_id
      JOIN (
        SELECT 0 as n UNION ALL
        SELECT 1 as n UNION ALL
        SELECT 2 as n UNION ALL
        SELECT 3 as n UNION ALL
        SELECT 4 as n UNION ALL
        SELECT 5 as n UNION ALL
        SELECT 6 as n UNION ALL
        SELECT 7 as n UNION ALL
        SELECT 8 as n UNION ALL
        SELECT 9 as n UNION ALL
        SELECT 10 as n UNION ALL
        SELECT 11 as n UNION ALL
        SELECT 12 as n
      ) as n
      ON n.n <= 12
    WHERE
      s.plan_id != 0
      AND start_date < '2021-01-01'
      GROUP BY 
      customer_id,
      s.plan_id,
      plan_name,
      start_date,
      price
  ) AS t
ORDER BY
  customer_id;

SELECT * FROM payments;
