SQL -->  STRUCTURED QUERY LANGUAGE = Lenguaje de Consulta Estructurado
---

Ejercicios de SQL
--

Select Distint - 
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



ORDER BY -  es para ordenar los resultados de manera Ascendente o Descentente.

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



INSERT INTO - Es utilizado para insertar nuevos registros en una tabla.

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


NULL / COALESCE -  Es utilizado para seleccionar registros si un campo especifico es nulo.

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


Ejercicio 10 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 11 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 12 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 13 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 14 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 15 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 16 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 17 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 18 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```


Ejercicio 19 - Insertar un nuevo cliente en la tabla región:

```sql
insert into region(region_id, region_description )
values ( 5,'south east' );

select *
from region; 
```
