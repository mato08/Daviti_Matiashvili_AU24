-- Task 3.1: Function to get total sales by category
CREATE OR REPLACE FUNCTION sh.get_total_sales_by_category(
    start_date DATE,
    end_date DATE
) 
RETURNS TABLE (
    product_category VARCHAR,
    total_sales_amount NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.prod_category,
        SUM(s.amount_sold) AS total_sales_amount
    FROM 
        sh.sales AS s
    LEFT JOIN 
        sh.products AS p 
        ON s.prod_id = p.prod_id
    WHERE 
        s.time_id BETWEEN start_date AND end_date
    GROUP BY 
        p.prod_category
    ORDER BY 
        total_sales_amount DESC;
END;
$$;


SELECT *
FROM sh.get_total_sales_by_category('1998-01-10', '1998-02-21');


-- Task 3.2: Function to get average sales quantity by region
CREATE OR REPLACE FUNCTION sh.get_avg_sales_quantity_by_region(
    product_id INT
) 
RETURNS TABLE (
    region VARCHAR,
    average_quantity NUMERIC
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c2.country_region AS region,
        AVG(s.quantity_sold) AS average_quantity
    FROM 
        sh.sales AS s
    LEFT JOIN 
        sh.customers AS c 
        ON s.cust_id = c.cust_id
    LEFT JOIN 
        sh.countries AS c2 
        ON c.country_id = c2.country_id
    WHERE 
        s.prod_id = product_id
    GROUP BY 
        c2.country_region;
END;
$$;

select * 
from  sh.get_avg_sales_quantity_by_region(13);

-- Task 3.3: Query to get top 5 customers by total sales
SELECT 
    CONCAT(c.cust_first_name, ' ', c.cust_last_name) AS full_name,
    SUM(s.amount_sold) AS total_sales
FROM 
    sh.sales AS s
LEFT JOIN 
    sh.customers AS c 
    ON s.cust_id = c.cust_id
GROUP BY 
    c.cust_id 
ORDER BY 
    total_sales DESC
FETCH FIRST 5 ROWS WITH TIES;



SELECT 
    CONCAT(c.cust_first_name, ' ', c.cust_last_name) AS full_name,
    SUM(s.amount_sold) AS total_sales
FROM 
    sh.sales AS s
LEFT JOIN 
    sh.customers AS c 
    ON s.cust_id = c.cust_id
GROUP BY 
    c.cust_first_name,c.cust_last_name 
ORDER BY 
    total_sales DESC
FETCH FIRST 5 ROWS WITH TIES;
		
/* you said that "Sometimes grouping by name may lead to different results". at first i tried to used group by first_name and last_name 
 * and it really gave me different result so because of that I thought there are some customers with same firstname and last name
 *  and  I tried to use unique identifier for each customer and its their cust_id and that's why I used it.
 */

/* updated version- I added both queries for demonstration that it can give different results,so maybe client can choose how does
 * he/she wants it. "Grouping by name still persists."-I didn't really understand what you mean with this comment and thats why 
 * I decided to leave both of them as correct solution. and I added two function calls for testing purposes after the each function.
 */

