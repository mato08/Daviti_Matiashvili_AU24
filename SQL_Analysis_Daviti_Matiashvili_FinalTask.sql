--task 1
WITH sales_by_country AS (
    SELECT
        ch.channel_desc,
        co.country_region,
        SUM(s.quantity_sold) AS sales,
        round(sum(s.quantity_sold) *100/ sum(sum(s.quantity_sold)) over (partition by ch.channel_desc ),2) || '%' as "SALES",
        rank() over (partition by ch.channel_desc order by sum(s.quantity_sold) desc) as rank
    FROM
        sh.sales s
    LEFT JOIN
        sh.channels ch ON s.channel_id = ch.channel_id
    LEFT JOIN
        sh.customers c ON s.cust_id = c.cust_id
    LEFT JOIN
        sh.countries co ON c.country_id = co.country_id
    GROUP BY
        ch.channel_id, co.country_region
)
select 	
	channel_desc,
	country_region,
	sales,
	"SALES"
	from 
	sales_by_country
	where rank=1
	order by sales desc;

/* here the calculations were easy but additionally we needed rank() because we had to ouptut the highest quantity so I used it
 * to filter data with "where rank=1". I think everything else is clear here.
 */

-- task 2
with yearly_sales as(
		select 
			p.prod_subcategory,
			t.calendar_year,
			sum(s.amount_sold) as sales
		from 
			sh.sales s 
		left join 
			sh.products p on s.prod_id =p.prod_id 
		left join 
			sh.times t on s.time_id =t.time_id 
		where 
			t.calendar_year between 1997 and 2001
		group by 
			p.prod_subcategory,t.calendar_year
),
sale_comparison as (
	select 
		 y1.prod_subcategory,
		 y1.calendar_year,
		 y1.sales as current_year_sales,
		 coalesce(y2.sales,0) as previous_year_sales
	from 
		yearly_sales y1
	left join 
		yearly_sales y2 on y1.prod_subcategory=y2.prod_subcategory
		and  y1.calendar_year=y2.calendar_year+1
	where y1.calendar_year between 1998 and 2001
),
final_output as (
	select 
		prod_subcategory
	from 
		sale_comparison
	where 
		current_year_sales > previous_year_sales
	group by 
		prod_subcategory
	having
		count(calendar_year)=4
)
select 
	prod_subcategory
from 
	final_output;

/* here at first cte I calculated sales from 1997 to 2001, with second cte I just created two columns which gives us information 
 * about previous year sales and current year sales and years are from 1998 to 2001,because we need those years only.
 * and with last cte I compare current year sales with previous one and then I calculate years,if this goes for 4 years then
 * it means that sales where growing year by year from 1998 to 2001 and I output the product subcategories.
 */

-- task 3
WITH QuarterlySales AS (
    SELECT
        t.calendar_year,
        t.calendar_quarter_desc,
        p.prod_category,
        SUM(s.amount_sold) AS sales,
        ROUND(SUM(SUM(s.amount_sold)) OVER (
            PARTITION BY calendar_year
            ORDER BY calendar_quarter_desc
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) AS "cum_sum$"
    FROM 
        sh.sales s
    LEFT JOIN 
        sh.products p ON s.prod_id = p.prod_id
    LEFT JOIN 
        sh.times t ON s.time_id = t.time_id
    LEFT JOIN 
        sh.channels c ON s.channel_id = c.channel_id
    WHERE
        t.calendar_year IN (1999, 2000)
        AND UPPER(p.prod_category) IN (UPPER('electronics'), UPPER('hardware'), UPPER('software/other'))
        AND UPPER(c.channel_desc) IN (UPPER('partners'), UPPER('internet'))
    GROUP BY 
        t.calendar_year,
        t.calendar_quarter_desc,
        p.prod_category
),
SalesWithDifference AS (
    SELECT
        calendar_year,
        calendar_quarter_desc,
        prod_category,
        sales,
        "cum_sum$",
        first_value (sales) OVER (
            PARTITION BY calendar_year, prod_category 
            ORDER BY calendar_quarter_desc
        ) AS first_quarter_sales
    FROM 
        QuarterlySales
),
FinalSalesReport AS (
    SELECT
        calendar_year,
        calendar_quarter_desc,
        prod_category,
        ROUND(sales, 2) AS "sales$",
        CASE 
            WHEN sales=first_quarter_sales  THEN 'N/A'
            ELSE TO_CHAR(ROUND(((sales - first_quarter_sales) / first_quarter_sales) * 100, 2), 'FM999,999.00') || '%'
        END AS "diff_percent",
        "cum_sum$"
    FROM 
        SalesWithDifference
)
SELECT 
    calendar_year,
    calendar_quarter_desc,
    prod_category,
    "sales$",
    "diff_percent",
    "cum_sum$"
FROM 
    FinalSalesReport
ORDER BY 
    calendar_year,
    calendar_quarter_desc,
    "sales$" DESC;


/* with first cte I calculate everything other than diff_precent, I will talk about cum_sum,other calculations are easy to undesrstand.
 * in cum_sum we also needed window frame to calculate sales properly because we for quarter 2 we sum up quarter 2 and quarter 1
 * for quarter 3 we sum up 3,2 and 1. 
 * range is working well for this type of situations,at first I tried to use "rows" but it worked incorrectly.
 * at second cte I used first_value.since we need to compare other quarters to only first_quarter and first_value is exactly what we need
 * since it only outputs the first_value from the given rows.
 * and with last cte I just calculate the percentage  and then I output everything what is needed.
 *
 */
	