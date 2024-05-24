SQL -->  STRUCTURED QUERY LANGUAGE = Lenguaje de Consulta Estructurado
---

Ejercicios de SQL
--

Select Distint 
-
>Consulta para que devuelva valores unicos (NO repetidos).

Ejercicio 1 -	Obtener una lista de todas las categorías distintas:

```sql
select distinct category_name
from categories;
``` 


Ejercicio 2 - Obtener una lista de todas las regiones distintas para los clientes:

```sql
select distinct region
from customers;
```


Elercicio 3 - Obtener una lista de todos los títulos de contacto distintos:

```sql
select distinct contact_title
from customers;
```



ORDER BY
-
>es para ordenar los resultados de manera Ascendente o Descentente.

Ejercicio 4 - Obtener una lista de todos los clientes, ordenados por país:

```sql
select *
from customers
order by country 
```


Ejercicio 5 - Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido:

```sql
SELECT *
FROM orders
ORDER BY employee_id, order_date; 
```



INSERT INTO 
-

>Es utilizado para insertar nuevos registros en una tabla.

Ejercicio 6 - Insertar un Nuevo cliente en la tabla Customers:

```sql
insert into customers ( customer_id, company_name, contact_name, contact_title, address, city, region,postal_code, country, phone, fax )
values('Telec', 'Telecomunicaciones', 'German leventan', 'Ingeniero','D Gallo 1277', 'Santa Rosa', 'Ingeniero','6300','Argentina',29541111111,null );
select *
from customers; 
```


Ejercicio 7 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


NULL / COALESCE 
-

>Es utilizado para seleccionar registros si un campo especifico es nulo.

Ejercicio 8 - Obtener todos los clientes de la tabla Customers donde el campo region es NULL:

```sql
SELECT *
FROM Customers
WHERE region IS NULL; 
```


Ejercicio 9 - Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es Null, use el precio estándar de $10 en su lugar:

```sql
SELECT Product_Name, COALESCE(Unit_Price, 10) AS Unit_Price
FROM Products; 
```

> “Si el precio unitario es NULL, la función COALESCE lo reemplaza con el valor 10 “

INNER JOIN
-
>

Ejercicio 10 - Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos:

```sql
SELECT customers.company_name, customers.contact_name, orders.order_date
FROM orders
INNER JOIN customers 
ON orders.Customer_id = customers.Customer_id; 
```


Ejercicio 11 - Obtener la identificación del pedido, el nombre del producto y el descuento de todos los detalles del pedido y productos:

```sql
SELECT order_details.order_id, Products.product_name, order_details.discount
FROM Order_details
JOIN Products 
ON order_details.product_id = products.product_id; 
```

LEFT JOIN
-
>


Ejercicio 12 - Obtener el identificador del cliente, el nombre de la compañia, el identificador y la fecha de la orden de todas las ordines y aquellos clientes que hagan match:

```sql
SELECT Customers.customer_id, Customers.company_name, orders.order_id, orders.order_date
FROM Customers
INNER JOIN Orders 
ON Customers.customer_id = Orders.customer_id;

SELECT employees.employee_id, employees.last_name, employees.territory_id, territories.territory_description
FROM Employees
INNER JOIN territories ON employees.territory_id = territories.territory_id; 
```


Ejercicio 13 - Obtener el identificador del empleado, apellido, identificador de territorio y descripción del territorio de todos los empleados y aquellos que hagan match en territorios:

```sql
SELECT 
    Employees.employee_id, 
    Employees.Last_Name, 
    Employee_Territories.territory_id, 
    Territories.Territory_Description
FROM Employees
INNER JOIN  Employee_Territories 
ON Employees.employee_id = Employee_Territories.employee_id
INNER JOIN Territories 
ON Employee_Territories.territory_id = Territories.territory_id; 
```


Ejercicio 14 - Obtener el identificador de la orden y el nombre de la empresa de todos las ordenes y aquellos que hagan match:

```sql
SELECT orders.order_id, customers.company_name
FROM orders
INNER JOIN customers 
ON orders.customer_id = customers.customer_id;
```




RIGHT JOIN
-
>
>
Ejercicio 15 - Obtener el identificador de la orden, y el nombre de la compañía de todas las ordenes y aquellos clientes que hagan match:

```sql
SELECT orders.order_id, customers.company_name
FROM orders
INNER JOIN customers 
ON orders.customer_id = customers.customer_id; 
```

Ejercicio 16 - Obtener el nombre de la compañía, y la fecha de la orden de todas las ordines y aquellos transportistas que hagan mach. Solamente para aquellas órdenes del año 1996:

```sql
SELECT Customers.Company_Name, Orders.Order_Date
FROM Customers
RIGHT JOIN Orders 
ON Customers.Customer_ID = Orders.Customer_ID
RIGHT JOIN Shippers 
ON customers.company_name = Shippers.company_name
WHERE EXTRACT(YEAR FROM Orders.Order_Date) = 1996; 
```

FULL OUTER JOIN
-
>

Ejercicio 17 - Obtener nombre y apellido del empleados y el identificador de territorio, de todos los empleados y aquellos que hagan match o no de employee_territories:

```sql
SELECT 
    Employees.First_Name,
    Employees.Last_Name,
    Employee_Territories.Territory_ID
FROM Employees
FULL OUTER JOIN Employee_Territories 
ON Employees.Employee_ID = Employee_Territories.Employee_ID; 
```


Ejercicio 18 - Obtener el identificador de la orden, precio unitario, cantidad y total de todas las órdenes y aquellas órdenes detalles que hagan match o no:

```sql
SELECT 
    Orders.Order_ID,
    Order_Details.Unit_Price,
    Order_Details.Quantity,
    (Order_Details.Unit_Price * Order_Details.Quantity) AS Total
FROM Orders
FULL OUTER JOIN Order_Details 
ON Orders.Order_ID = Order_Details.Order_ID; 
```

UNION
-
>

Ejercicio 19 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


--  20. Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento.

```sql
select first_name  
from employees 

union

select first_name 
from employees 
where employee_id = reports_to ;
```



-- SUBQUERIES


-- 21. Obtener los productos del stock que han sido vendidos

```sql
SELECT Product_Name, PRODUCT_ID
FROM Products
WHERE Product_ID IN (
    SELECT DISTINCT Product_ID
    FROM Order_Details
);
```



-- 22. Obtener los clientes que han realizado un pedido con destino a Argentina.
```sql
SELECT *
FROM customers
WHERE customer_id IN (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE ship_country = 'Argentina'
);
```




--  23.Obtener el nombre de los productos que nunca han sido pedido por clientes de Francia.

```sql
SELECT Product_Name
FROM Products
WHERE Product_ID NOT IN (
    SELECT Product_ID
    FROM Order_Details
    WHERE Order_ID IN (
        SELECT Order_ID
        FROM Orders
        WHERE Customer_ID IN (
            SELECT Customer_ID
            FROM Customers
            WHERE Country = 'France'
        )
    )
);

```



--  GROUP BY

-- 24. Obtener la cantidad de productos vendidos por identificador de orden
```sql
SELECT Order_ID, SUM(Quantity) 
FROM Order_Details
GROUP BY Order_ID;
```


-- 25 Obtener el promedio de los productos en stock por producto
  
```sql
SELECT Product_name, AVG(Units_In_Stock) 
FROM Products
GROUP BY Product_name;

```


-- HAVING 

-- 26  CANTIDAD DE PRODUCTOS EN STOCK POR PRODUCTO, DONDE HAYA MAS D E100 PRODUCTOS EN STOCK


```sql
SELECT Product_NAME, SUM(Units_In_Stock) --AS CantidadEnStock
FROM Products
GROUP BY Product_NAME
HAVING SUM(Units_In_Stock) > 100;

```


-- 27 Obtener el promedio de pedidos por cada compania y solo mostrar aquellas con un promedio de pedidos superior a 10
```sql
select
    c.company_name,
    sum(o.order_id) / count(o.order_id) as avg_orders
from
    orders o
    left join customers c on o.customer_id = c.customer_id
group by
    c.company_name
having
    sum(o.order_id) / count(o.order_id) > 10;

```





-- CASE


-- 28.  Obtener el nombre del prodducto y su categoria, pero muestre "Discuntinued" en lugar del nombre de la categoria si el producto ha sido discuntinuado

```sql
SELECT 
    Product_Name,
    CASE 
        WHEN Discontinued = 1 THEN 'Discontinued'
        ELSE Category_Name
    END AS Category
FROM 
    Products
    INNER JOIN Categories ON Products.Category_ID = Categories.Category_ID;

```
   
   -- 29.  obtener el nombre del empleado y su titulo, pero muestre " Gerente de Ventas" en lugar del titulo si el empleado es un gerente de ventas ( Sales Manager)

```sql
 SELECT 
    First_Name || ' ' || Last_Name AS NombreEmpleado,
    CASE 
        WHEN Title = 'Sales Manager' THEN 'Gerente de Ventas'
        ELSE Title
    END AS Titulo
FROM 
    Employees;
   
   ```
      
