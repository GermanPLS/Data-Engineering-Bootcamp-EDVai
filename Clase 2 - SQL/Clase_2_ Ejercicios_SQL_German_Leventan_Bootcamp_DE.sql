
-- CLASE 2 - EJERCICIOS WF



-- AVG

-- 1. Obtener el promedio de precios por cada categoria de producto. la clausula OVER (PARTICIONBY categoryID)especifica
--    que se debe calcular el promedio de precios por cada valor unico de CategoryID en la tabla.


SELECT Product_ID, Category_ID, Unit_Price,
       AVG(Unit_Price) OVER(PARTITION BY Category_ID) AS AvgPriceByCategory
FROM Products;





-- 2 .Obtener el promedio de venta de cada cliente

select avg(od.unit_price*od.quantity) 
over (partition by o.customer_id) as promedio_venta,
o.order_id, 
o.customer_id, 
o.employee_id, 
o.required_date, 
o.shipped_date 
from orders as o
inner join order_details as od
on o.order_id  = od.order_id;


-- 3. Obtener el promedio de cantidad de productos vendidos por categoria ( product_name, quantily_per_unit, unit_price, quantily, avgquantily) y ordenarlo
-- por nombre de la categoria y nombre del producto

    
   
SELECT 
    p.Product_Name,
    c.Category_Name,
    p.Quantity_Per_Unit,
    p.Unit_Price,
    od.Quantity,
    AVG(od.Quantity) OVER(PARTITION BY c.Category_Name) AS AvgQuantity
FROM 
    Products p
INNER JOIN 
    Categories c ON p.Category_ID = c.Category_ID
INNER JOIN 
    Order_Details od ON p.Product_ID = od.Product_ID
ORDER BY 
    c.Category_Name, p.Product_Name;
   
   
   
   
   
 -- MIN
   
-- 4.    Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la orden para cada cliente de la tabla 'Orders'. 
   
   
SELECT 
    Customer_ID,
    Order_Date,
    (SELECT MIN(Order_Date) FROM Orders AS o2 WHERE o1.Customer_ID = o2.Customer_ID) AS EarliestOrderDate
FROM 
    Orders AS o1;

   
   
   
 --  MAX  
   
-- 5. Seleccione el id de producto, el nombre de producto, el precio unitario, el id de categoría y el precio unitario máximo para cada categoría de la tabla Products.   

   
 SELECT
    Product_ID,
    Product_Name,
    Unit_Price,
    Category_ID,
    (SELECT MAX(Unit_Price) FROM Products AS p2 WHERE p1.Category_ID = p2.Category_ID) AS MaxUnitPriceByCategory
FROM
    Products AS p1;
  
   
   
   
--  ROW_NUMBER

   
-- 6. Obtener el ranking de los productos mas vendidos

   
SELECT
    
    Product_Name,
    QuantitySold,
    ROW_NUMBER() OVER (ORDER BY QuantitySold DESC) AS SalesRank
FROM (
    SELECT
        od.Product_ID,
        p.Product_Name,
        SUM(Quantity) AS QuantitySold
    FROM
        Order_Details od
    JOIN
        Products p ON od.Product_ID = p.Product_ID
    GROUP BY
        od.Product_ID, p.Product_Name
) AS ProductSales;




-- 7. Asignar numeros de fila para cada cliente, ordenarlos por customer_id



   SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS row_number,
    c.*
FROM
    customers c;

  
   
-- 8.  Obtener el ranking de los empleados más jóvenes () ranking, nombre y apellido del empleado, fecha de nacimiento)

SELECT
    ROW_NUMBER() OVER (ORDER BY birth_date desc) AS ranking,
    first_name,
    last_name,
    birth_date
FROM
    employees;
   
   
   
   
-- SUM

   
-- 9. Obtener la suma de venta de cada cliente
   
 select sum(od.unit_price*od.quantity) over (partition by o.customer_id), o.order_id, c.customer_id, o.employee_id, o.order_date, o.required_date 
from customers as c
inner join orders as o 
on c.customer_id = o.customer_id 
inner join order_details as od
on o.order_id = od.order_id;  

   

   
   -- 10.Obtener la suma total de ventas por categoría de producto


    
SELECT
    c.category_name,
    p.product_name,
    p.unit_price,
    SUM(od.quantity) AS total_quantity,
    SUM(od.quantity * od.unit_price) AS total_sales
FROM
    order_details od
JOIN
    products p ON od.product_id = p.product_id
JOIN
    categories c ON p.category_id = c.category_id
GROUP BY
    c.category_name, p.product_name, p.unit_price;


SELECT
    c.category_name,
    p.product_name,
    p.unit_price,
    od.quantity,
    SUM(od.quantity * od.unit_price) AS total_sales
FROM
    order_details od
JOIN
    products p ON od.product_id = p.product_id
JOIN
    categories c ON p.category_id = c.category_id
GROUP BY
    c.category_name, p.product_name, p.unit_price, od.quantity;

   
   
   
--  11.   Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país y por orden de manera ascendente   
   
  select
    o.ship_country,
    o.order_id,
    o.shipped_date,
    o.freight,
    sum(o.freight) over (partition by o.ship_country) as total_shipping_costs
from
    orders o
order by
    o.ship_country asc, o.order_id asc; 
   

   
   
   -- RANK
   
   
-- 12. RANKING DE VENTAS POR CLIENTE   
   
select
    table1.customer_id,
    table1.company_name,
    table1.total_sales,
    rank() over (order by table1.total_sales desc) as ranking
from
    (
    select
        o.customer_id,
        c.company_name,
        sum(od.unit_price * od.quantity) as total_sales
    from
        orders o
        inner join order_details od on o.order_id = od.order_id
        left join customers c on o.customer_id = c.customer_id
    group by
        o.customer_id, c.company_name
) table1;
   
   
-- 13.Ranking de empleados por fecha de contratacion

select
    e.employee_id,
    e.first_name,
    e.last_name,
    e.hire_date,
    rank() over (order by e.hire_date) as ranking
from
    employees e;   
   
--  14.Ranking de productos por precio unitario

 select
    p.product_id,
    p.product_name,
    p.unit_price,
    rank() over (order by p.unit_price desc) as ranking
from
    products p;  
   
   
-- LAG

   
 
   
--   15.Mostrar por cada producto de una orden, la cantidad vendida y la cantidad vendida del producto previo.
   
 select
    od.order_id,
    od.product_id,
    od.quantity,
    lag(od.quantity) over (partition by od.order_id order by od.product_id) as prev_quantity
from
    order_details od;
   
   
--  16.Obtener un listado de ordenes mostrando el id de la orden, fecha de orden, id del cliente y última fecha de orden.  

select
    o.order_id,
    o.order_date,
    o.customer_id,
    lag(o.order_date) over (partition by o.customer_id order by o.order_date) as last_order_date
from
    orders o; 
   
   
--  17.Obtener un listado de productos que contengan: id de producto, nombre del producto, precio unitario, precio del producto anterior, diferencia entre el precio del producto y
-- precio del producto anterior.
   
   
 select
    table1.product_id,
    table1.product_name,
    table1.unit_price,
    table1.last_unit_price,
    table1.unit_price - table1.last_unit_price as price_difference
from (
    select
        p.product_id,
        p.product_name,
        p.unit_price,
        lag(p.unit_price) over (order by p.product_id) as last_unit_price
    from
        products p
) table1;  
   
   
   
   
   
   
-- LEAD 

   
-- 18.Obtener un listado que muestra el precio de un producto junto con el precio del producto siguiente:

   
select
    p.product_name,
    p.unit_price,
    lead(p.unit_price) over (order by p.product_id) as next_price
from
    products p;
   
   
   
   

-- 19.Obtener un listado que muestra el total de ventas por categoría de producto junto con el total de ventas de la categoría siguiente
   
   
 select
    table1.category_name,
    table1.total_sales,
    lead(table1.total_sales) over (order by table1.category_name) as next_total_sales
from (
    select
        c.category_name,
        sum(od.unit_price * od.quantity) as total_sales
    from
        order_details od
        left join products p on od.product_id = p.product_id
        left join categories c on p.category_id = c.category_id
    group by
        c.category_name
    order by
        c.category_name
) table1;  
   
   
   
   
   
   
