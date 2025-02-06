create extension if not exists file_fdw;
create schema if not exists sa_online_sales_schema;
CREATE SERVER online_sales_external_server FOREIGN DATA WRAPPER file_fdw;


drop foreign table if exists  sa_online_sales_schema.ext_online_sales
create foreign table if not exists sa_online_sales_schema.ext_online_sales(
    Row_ID varchar,
    Order_ID varchar,
    Order_Date varchar,
    Ship_Date varchar,
    Ship_Mode varchar,
    Customer_ID varchar,
    Customer_Name varchar,
    Segment varchar,
    Country_Region varchar,
    City varchar,
    State_Province varchar,
    Postal_Code varchar,
    Region varchar,
    Product_ID varchar,
    Category varchar,
    Sub_Category varchar,
    Product_Name varchar,
    Sales varchar,
    Quantity varchar,
    Discount varchar,
    Profit varchar,
    Employee_ID varchar,
    Order_Type varchar,
    Delivery_Method varchar,
    Payment_Method varchar,
    Year varchar,
    Quarter varchar,
    Month varchar,
    Month_Name varchar,
    Week varchar,
    Day varchar,
    Day_Name varchar,
    Is_Weekend varchar,
    Employee_Name varchar,
    Employee_Surname varchar,
    store_id varchar,
    store_name  varchar
)
server online_sales_external_server
options(
    FILENAME 'C:\Users\matia\Desktop\modified_online_sales_with_stores.csv',
    format 'csv',
    header 'true'
);


create table if not exists sa_online_sales_schema.src_online_sales(
 Row_ID varchar,
    Order_ID varchar,
    Order_Date varchar,
    Ship_Date varchar,
    Ship_Mode varchar,
    Customer_ID varchar,
    Customer_Name varchar,
    Segment varchar,
    Country_Region varchar,
    City varchar,
    State_Province varchar,
    Postal_Code varchar,
    Region varchar,
    Product_ID varchar,
    Category varchar,
    Sub_Category varchar,
    Product_Name varchar,
    Sales varchar,
    Quantity varchar,
    Discount varchar,
    Profit varchar,
    Employee_ID varchar,
    Order_Type varchar,
    Delivery_Method varchar,
    Payment_Method varchar,
    Year varchar,
    Quarter varchar,
    Month varchar,
    Month_Name varchar,
    Week varchar,
    Day varchar,
    Day_Name varchar,
    Is_Weekend varchar,
    Employee_Name varchar,
    Employee_Surname varchar,
    store_id varchar,
    store_name varchar
);

insert into sa_online_sales_schema.src_online_sales
select 
	Row_ID,	
    Order_ID, 
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID, 
    Customer_Name, 
    Segment,
    Country_Region, 
    City,
    State_Province,
    Postal_Code,
    Region,
    Product_ID,
    Category,
    Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Employee_ID,
    Order_Type,
    Delivery_Method,
    Payment_Method,
    Year,
    Quarter,
    Month,
    Month_Name,
    Week,
    Day,
    Day_Name,
    Is_Weekend,
    Employee_Name, 
    Employee_Surname,
    store_id,
    store_name
from sa_online_sales_schema.ext_online_sales 
	where not exists(
	select 1 
	from sa_online_sales_schema.src_online_sales);


select * from sa_online_sales_schema.src_online_sales sos 




create schema if not exists sa_offline_sales_schema;
CREATE SERVER offline_sales_external_server FOREIGN DATA WRAPPER file_fdw;
create foreign table if not exists sa_offline_sales_schema.ext_offline_sales(
     Row_ID varchar,
    Order_ID varchar,
    Order_Date varchar,
    Ship_Date varchar,
    Ship_Mode varchar,
    Customer_ID varchar,
    Customer_Name varchar,
    Segment varchar,
    Country_Region varchar,
    City varchar,
    State_Province varchar,
    Postal_Code varchar,
    Region varchar,
    Product_ID varchar,
    Category varchar,
    Sub_Category varchar,
    Product_Name varchar,
    Sales varchar,
    Quantity varchar,
    Discount varchar,
    Profit varchar,
    Employee_ID varchar,
    Order_Type varchar,
    Store_name varchar,
    Cashier_Representative varchar,
    Payment_Method varchar,
    Year varchar,
    Quarter varchar,
    Month varchar,
    Month_Name varchar,
    Week varchar,
    Day varchar,
    Day_Name varchar,
    Is_Weekend varchar,
    Employee_Name varchar,
    Employee_Surname varchar,
    store_id varchar
)
server offline_sales_external_server
options(
    FILENAME 'C:\Users\matia\Desktop\offline_sales_with_store_id.csv',
    format 'csv',
    header 'true'
);



create table if not exists sa_offline_sales_schema.src_offline_sales(
    Row_ID varchar,
    Order_ID varchar,
    Order_Date varchar,
    Ship_Date varchar,
    Ship_Mode varchar,
    Customer_ID varchar,
    Customer_Name varchar,
    Segment varchar,
    Country_Region varchar,
    City varchar,
    State_Province varchar,
    Postal_Code varchar,
    Region varchar,
    Product_ID varchar,
    Category varchar,
    Sub_Category varchar,
    Product_Name varchar,
    Sales varchar,
    Quantity varchar,
    Discount varchar,
    Profit varchar,
    Employee_ID varchar,
    Order_Type varchar,
    Cashier_Representative varchar,
    Payment_Method varchar,
    Year varchar,
    Quarter varchar,
    Month varchar,
    Month_Name varchar,
    Week varchar,
    Day varchar,
    Day_Name varchar,
    Is_Weekend varchar,
    Employee_Name varchar,
    Employee_Surname varchar,
    store_id varchar
);


insert into sa_offline_sales_schema.src_offline_sales
select 
	Row_ID,
    Order_ID,
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID,
    Customer_Name,
    Segment,
    Country_Region,
    City ,
    State_Province,
    Postal_Code,
    Region,
    Product_ID,
    Category,
    Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Employee_ID,
    Order_Type,
    cashier_representative ,
    Payment_Method,
    Year,
    Quarter,
    Month,
    Month_Name,
    Week ,
    Day,
    Day_Name ,
    Is_Weekend,
    Employee_Name,
    Employee_Surname,
    store_id
from sa_offline_sales_schema.ext_offline_sales 
where not exists(
select 1 
from sa_offline_sales_schema.src_offline_sales);


select * from sa_offline_sales_schema.ext_offline_sales eos;
select * from sa_offline_sales_schema.src_offline_sales sos;
select* from sa_online_sales_schema.ext_online_sales eos ;
select* from sa_online_sales_schema.src_online_sales sos ;

select *
from sa_offline_sales_schema.ext_offline_sales
group by  Row_ID,
    Order_ID,
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID,
    Customer_Name,
    Segment,
    Country_Region,
    City ,
    State_Province,
    Postal_Code,
    Region,
    Product_ID,
    Category,
    Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Employee_ID,
    Order_Type,
    cashier_representative ,
    Payment_Method,
    Year,
    Quarter,
    Month,
    Month_Name,
    Week ,
    Day,
    Day_Name ,
    Is_Weekend,
    Employee_Name,
    Employee_Surname,
    store_id,
    store_name 
having count(*)>1

select *
from sa_offline_sales_schema.src_offline_sales
group by  Row_ID,
    Order_ID,
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID,
    Customer_Name,
    Segment,
    Country_Region,
    City ,
    State_Province,
    Postal_Code,
    Region,
    Product_ID,
    Category,
    Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Employee_ID,
    Order_Type,
    cashier_representative ,
    Payment_Method,
    Year,
    Quarter,
    Month,
    Month_Name,
    Week ,
    Day,
    Day_Name ,
    Is_Weekend,
    Employee_Name,
    Employee_Surname,
    store_id 
having count(*)>1


select * 
from sa_online_sales_schema.src_online_sales eos 
group by
     Row_ID,	
    Order_ID, 
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID, 
    Customer_Name, 
    Segment,
    Country_Region, 
    City,
    State_Province,
    Postal_Code,
    Region,
    Product_ID,
    Category,
    Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Employee_ID,
    Order_Type,
    Delivery_Method,
    Payment_Method,
    Year,
    Quarter,
    Month,
    Month_Name,
    Week,
    Day,
    Day_Name,
    Is_Weekend,
    Employee_Name, 
    Employee_Surname,
    store_id,
    store_name
having count(*)>1


select * 
from sa_online_sales_schema.ext_online_sales eos 
group by
     Row_ID,	
    Order_ID, 
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID, 
    Customer_Name, 
    Segment,
    Country_Region, 
    City,
    State_Province,
    Postal_Code,
    Region,
    Product_ID,
    Category,
    Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Employee_ID,
    Order_Type,
    Delivery_Method,
    Payment_Method,
    Year,
    Quarter,
    Month,
    Month_Name,
    Week,
    Day,
    Day_Name,
    Is_Weekend,
    Employee_Name, 
    Employee_Surname,
    store_id,
    store_name
having count(*)>1



