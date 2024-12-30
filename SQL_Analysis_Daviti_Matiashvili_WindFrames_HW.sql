WITH yearly_sales_data AS (
    -- get sales data by year, country region, and channel
    SELECT
        t.calendar_year,
        ca.country_region,
        c.channel_desc,
        SUM(s.amount_sold) AS amount_sold  -- Total amount sold
    FROM sh.sales s
    left JOIN sh.channels c ON s.channel_id = c.channel_id
    left JOIN sh.times t ON s.time_id = t.time_id
    left JOIN sh.customers cu ON s.cust_id = cu.cust_id
    left JOIN sh.countries ca ON cu.country_id = ca.country_id
    WHERE
        t.calendar_year BETWEEN 1998 AND 2001  
        AND upper(ca.country_region) IN (upper('Americas'), upper('Asia'), upper('Europe'))  
    GROUP BY
        t.calendar_year_id,t.calendar_year, ca.country_region,ca.country_region_id, c.channel_id
),
total_sales_by_region AS (
    -- Calculate total sales by year and region before percentage calculations
    SELECT
        calendar_year,
        country_region,
        SUM(amount_sold) AS total_sales_by_region  -- Total sales for each region in each year
    FROM yearly_sales_data
    GROUP BY
        calendar_year, country_region  -- Grouping by year and region to get totals
),
sales_percentages AS (
    -- Calculate the percentage of sales by channel for each year and region
    SELECT
        ys.calendar_year,
        ys.country_region,
        ys.channel_desc,
        ys.amount_sold,
        tsr.total_sales_by_region,
        (ys.amount_sold * 100.0 / tsr.total_sales_by_region) AS percent_by_channel  -- Percentage calculation
    FROM yearly_sales_data ys
    left JOIN total_sales_by_region tsr ON ys.calendar_year = tsr.calendar_year
    AND ys.country_region = tsr.country_region  -- Joining to get total sales by region
),
sales_percentages_with_previous AS (
    -- Calculate previous year's percentage using window functions
    SELECT
        sp.calendar_year,
        sp.country_region,
        sp.channel_desc,
        sp.amount_sold,
        sp.percent_by_channel,

        -- Using LAG function with a window frame to get the previous year's percentage correctly.
        LAG(sp.percent_by_channel) OVER (
            PARTITION BY sp.country_region, sp.channel_desc 
            ORDER BY sp.calendar_year 
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING  -- Frame to get only the previous year's percentage (e.g., from 1998 for 1999)
        ) AS percent_previous_period

    FROM sales_percentages sp  -- Source data from sales_percentages CTE
)
-- Final select of results to display
SELECT
    calendar_year,
    country_region,
    channel_desc,
    amount_sold,

    -- Round and display the current year's percentage by channel
    ROUND(percent_by_channel, 2) AS percent_by_channel,

    -- Round and display the previous year's percentage (from 1998)
    ROUND(COALESCE(percent_previous_period, 0), 2) AS percent_previous_period,

    -- Calculate and display the difference between current and previous year percentages.
    ROUND(percent_by_channel - COALESCE(percent_previous_period, 0), 2) AS percent_diff

FROM sales_percentages_with_previous
WHERE calendar_year >= 1999  -- Only display results for years 1999 to 2001
ORDER BY
    country_region,
    calendar_year,
    channel_desc;  -- Order results by region, year, and channel description for clarity.
    
    
    
    --task 2
WITH DailySales AS (
   SELECT 
        t.calendar_week_number,
        t.time_id,
        t.day_name,
        SUM(s.amount_sold) AS sales
    FROM sh.sales s
    LEFT JOIN sh.times t ON s.time_id = t.time_id
    WHERE EXTRACT(YEAR FROM t.time_id) = 1999 
        AND t.calendar_week_number between 48 and 52
    GROUP BY t.calendar_week_number, t.time_id, t.day_name),
CumulativeSales AS (
    SELECT ds.*,
           SUM(ds.sales) OVER (
               PARTITION BY ds.calendar_week_number
               ORDER BY ds.time_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS cum_sum
    FROM DailySales ds),
MovingAverages AS (
    SELECT cs.calendar_week_number,
           cs.time_id,
           cs.day_name,
           cs.sales,
           cs.cum_sum,
           CASE 
               WHEN LOWER(cs.day_name) = 'monday'
                   THEN AVG(cs.sales) OVER (
                        ORDER BY cs.time_id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
                   )
               WHEN LOWER(cs.day_name) = 'friday'
                   THEN AVG(cs.sales) OVER (
                        ORDER BY cs.time_id ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING
                   )
               ELSE AVG(cs.sales) OVER (
                        ORDER BY cs.time_id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                   )
           END AS centered_3_day_avg
    FROM CumulativeSales cs)

SELECT calendar_week_number,
       time_id,
       day_name,
       TO_CHAR(sales, 'FM9,999,999.00') AS sales,
       TO_CHAR(cum_sum, 'FM9,999,999.00') AS cum_sum,
       TO_CHAR(centered_3_day_avg, 'FM9,999,999.00') AS centered_3_day_avg
FROM MovingAverages
WHERE calendar_week_number IN (49, 50, 51)
ORDER BY calendar_week_number, time_id;



--task 3
SELECT
    t.calendar_month_number,
    SUM(amount_sold) AS m_sales, -- Total sales for the month

    -- First value in the RANGE frame (1 preceding to current row)
    FIRST_VALUE(SUM(amount_sold)) OVER (
        ORDER BY t.calendar_month_number
        RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS range_first_value,

    -- First value in the GROUPS frame (1 preceding group to current row)
    FIRST_VALUE(SUM(amount_sold)) OVER (
        ORDER BY t.calendar_month_number
        GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS groups_first_value,
    
     FIRST_VALUE(SUM(amount_sold)) OVER (
        ORDER BY t.calendar_month_number
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS rows_first_value

FROM sh.sales s
left JOIN sh.customers c ON c.cust_id = s.cust_id
left JOIN sh.times t ON t.time_id = s.time_id
left JOIN sh.channels ch ON ch.channel_id = s.channel_id
WHERE t.calendar_year = 1998
  AND upper(ch.channel_desc) = upper('tele sales')
GROUP BY t.calendar_month_id,t.calendar_month_number
ORDER BY t.calendar_month_number;

/* I wanted to use rowsm range and group in this example to show the difference between them,their values are mostly the same
 * but differnt in the 4th row. because the logics behind RANGE and GROUP and ROWS differ from each other. */



