--task 1
WITH RankedSales AS (
    SELECT 
        c.channel_desc,
        c2.cust_last_name,
        c2.cust_first_name,
        ROUND(SUM(s.amount_sold), 2) AS amount_sold,
        ROUND(100 * SUM(s.amount_sold) / SUM(SUM(s.amount_sold)) OVER (PARTITION BY c.channel_desc), 5) || '%' AS sales_percentage,
        RANK() OVER (PARTITION BY c.channel_desc ORDER BY SUM(s.amount_sold) DESC) AS rank
    FROM 
        sh.channels c
    JOIN 
        sh.sales s 
        ON c.channel_id = s.channel_id
    JOIN 
        sh.customers c2 
        ON s.cust_id = c2.cust_id
    GROUP BY 
        c.channel_desc, c2.cust_id
)
SELECT 
    channel_desc,
    cust_last_name,
    cust_first_name,
    amount_sold,
    sales_percentage
FROM 
    RankedSales
WHERE 
    rank <= 5
ORDER BY 
    channel_desc, amount_sold DESC;
   
/* I used cte for simplicity. I also used rank()-because since we need to retrieve top five customers for each channel 
 * some customers may same amount_sold and their rank will be the same.  If two or more rows have the same values, 
 * they receive the same rank,and the subsequent rank is skipped.so it may not return  exactly 5 rows for each
 * channel.this avoids to lose data of some customers who may have same amount_sold.*/
 


   
 --task 2

CREATE EXTENSION IF NOT EXISTS tablefunc;
-- it gave me an error while using crosstab function. I searched what was causing this and found a solution so i used extension.
SELECT 
    product_name,
    q1 AS q1,
    q2 AS q2,
   	q3 AS q3,
   	q4 AS q4,
    ROUND(SUM(COALESCE(q1, 0) + COALESCE(q2, 0) + COALESCE(q3, 0) + COALESCE(q4, 0)) 
          OVER (PARTITION BY product_name), 2) AS year_sum
FROM crosstab(
    $$
    SELECT 
        p.prod_name AS product_name,
        CASE 
            WHEN EXTRACT(MONTH FROM s.time_id) IN (1, 2, 3) THEN 'q1'
            WHEN EXTRACT(MONTH FROM s.time_id) IN (4, 5, 6) THEN 'q2'
            WHEN EXTRACT(MONTH FROM s.time_id) IN (7, 8, 9) THEN 'q3'
            WHEN EXTRACT(MONTH FROM s.time_id) IN (10, 11, 12) THEN 'q4'
        END AS quarter,
        ROUND(SUM(s.amount_sold), 2) AS total_sales
    FROM 
        sh.products p
    JOIN 
        sh.sales s ON p.prod_id = s.prod_id
    JOIN 
        sh.customers c ON s.cust_id = c.cust_id
    JOIN 
        sh.countries c2 ON c.country_id = c2.country_id
    WHERE 
        p.prod_category = 'Photo'
        AND c2.country_region = 'Asia'
        AND EXTRACT(YEAR FROM s.time_id) = 2000
    GROUP BY 
        p.prod_name, quarter
    ORDER BY 
        p.prod_name, quarter
    $$
) AS ct(product_name varchar(50), q1 NUMERIC, q2 NUMERIC, q3 NUMERIC, q4 NUMERIC)
ORDER BY 
    year_sum DESC;

/* here I just used crosstab function and window function together.some quarters values
 * were null and when I was summing them year_sum also was null so because of that I used coalesce() and I think that's all
 * about this task. */
   
   
   
--task 3
WITH sales_rank AS (
--step 1
        SELECT 
        s.cust_id,
        ch.channel_desc,
        EXTRACT(YEAR FROM s.time_id) AS year,
        ROUND(SUM(s.amount_sold), 2) AS total_sales,
        RANK() OVER (PARTITION BY ch.channel_desc, EXTRACT(YEAR FROM s.time_id) ORDER BY SUM(s.amount_sold) DESC) AS rank
    FROM 
        sh.sales s
    JOIN 
        sh.channels ch ON s.channel_id = ch.channel_id
    WHERE 
        EXTRACT(YEAR FROM s.time_id) IN (1998, 1999, 2001)
    GROUP BY 
        s.cust_id, ch.channel_desc, EXTRACT(YEAR FROM s.time_id)
),

top_customers AS (
    -- Step 2
    SELECT 
        cust_id,
        channel_desc,
        COUNT(DISTINCT year) AS years_in_top_300
    FROM 
        sales_rank
    WHERE 
        rank <= 300
    GROUP BY 
        cust_id, channel_desc
    HAVING 
        COUNT(DISTINCT year) = 3 
)

-- Step 3
SELECT 
    ch.channel_desc,
    s.cust_id,
 	c.cust_last_name,
 	c.cust_first_name,  
    ROUND(SUM(s.amount_sold), 2) AS total_sales
FROM 
    sh.sales s
JOIN 
    sh.channels ch ON s.channel_id = ch.channel_id
JOIN 
    top_customers t ON s.cust_id = t.cust_id AND ch.channel_desc = t.channel_desc
JOIN 
    sh.customers c ON s.cust_id = c.cust_id
WHERE 
    EXTRACT(YEAR FROM s.time_id) IN (1998, 1999, 2001)
GROUP BY 
    ch.channel_desc, c.cust_first_name, c.cust_last_name, s.cust_id
ORDER BY 
    ch.channel_desc, total_sales DESC;
    
   
/* i divide this task into 3 parts.
   1-Calculate total sales per customer, year, and channel, and rank customers within each channel by total sales
   2-Filter for customers who rank in the top 300 for each of the three years in each channel
   3-Final sales report for top customers by channel */
   
   select * from sh.countries c2
   select * from sh.sales s 
   select * from sh.products p 
   
   /*sum(case when upper(c2.country_name)=upper('Americas') then s.amount_sold else 0 end ) as "Americas SALES",
   			sum(case when upper(c2.country_name)=upper('europe') then s.amount_sold else 0 end) as "Europe SALES" */
--task 4
WITH RegionSales AS (
    SELECT 
        TO_CHAR(s.time_id, 'YYYY-MM') AS calendar_month_desc,
        p.prod_category AS prod_category,
        SUM(CASE 
                WHEN UPPER(c2.country_region) = 'AMERICAS' THEN s.amount_sold 
                ELSE 0 
            END) OVER (PARTITION BY TO_CHAR(s.time_id, 'YYYY-MM'), p.prod_category) AS "Americas SALES",
        SUM(CASE 
                WHEN UPPER(c2.country_region) = 'EUROPE' THEN s.amount_sold 
                ELSE 0 
            END) OVER (PARTITION BY TO_CHAR(s.time_id, 'YYYY-MM'), p.prod_category) AS "Europe SALES",
        ROW_NUMBER() OVER (PARTITION BY TO_CHAR(s.time_id, 'YYYY-MM'), p.prod_category ORDER BY p.prod_category) AS RowNum
    FROM 
        sh.sales s
    JOIN 
        sh.products p ON p.prod_id = s.prod_id
    JOIN 
        sh.customers c ON s.cust_id = c.cust_id
    JOIN 
        sh.countries c2 ON c.country_id = c2.country_id
    WHERE 
        EXTRACT(YEAR FROM s.time_id) = 2000
        AND EXTRACT(MONTH FROM s.time_id) IN (1, 2, 3)
        AND UPPER(c2.country_region) IN ('AMERICAS', 'EUROPE')
)
SELECT 
    calendar_month_desc,
    prod_category,
    "Americas SALES",
    "Europe SALES"
FROM 
    RegionSales
WHERE 
    RowNum = 1
ORDER BY 
    calendar_month_desc,
    prod_category;
   
/* in this task we could get the correct answer without using window function,just with sum and case it was possible.
 * but since it's window functions homework I used window function.but it cause me problem because there was many duplicate values
 * row since in partition by i wrote TO_CHAR(s.time_id, 'YYYY-MM'), p.prod_category.because of that I used another window function
 * row_number,since it gives unique value to each row it was exactly what i needed.since all the months and prod_categorys
 * are grouped together I can only choose the first one for each group since each of them have basically the same value.
 * and with help of that I returned the desired result. 
 */




   