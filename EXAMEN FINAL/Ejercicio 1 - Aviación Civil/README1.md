## EJERCICIO 1

# Aviación Civil


La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al
ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino,
como puede ser: cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades
de partidas y aterrizajes entre fechas determinadas, etc.
Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y
realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y
hacer sus recomendaciones con respecto al estado actual.

Listado de vuelos realizados:

https://datos.transporte.gob.ar/dataset/lista-aeropuertos


# TAREAS

### 1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina :


    2021:

    https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D

    2022:

    https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

    Aeropuertos_detalles:

    https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D




### Resolucion:


Se crea archivo script  `aviacion.sh`, guardado en `/home/hadoops/scripts`, para realizar la ingesta de archivos en `hdfs dfs /ingest`.

![alt text](archivos/e1.png)

----------------
### 2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022
#### (2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de los aeropuertos (aeropuertos_detalle.csv)


### **Tabla 1 - aeropuerto_tabla**

| **campos**             | **tipo** |
|------------------------|----------|
| fecha                  | date     |
| horaUTC                | string   |
| clase_de_vuelo         | string   |
| clasificacion_de_vuelo | string   |
| tipo_de_movimiento     | string   |
| aeropuerto             | string   |
| origen_destino         | string   |
| aerolinea_nombre       | string   |
| aeronave               | string   |
| pasajeros              | integer  |


---------
### **Tabla 2 - aeropuerto_detalles_tabla**

| **campos**    | **tipo** |
|---------------|----------|
| aeropuerto    | string   |
| oac           | string   |
| iata          | string   |
| tipo          | string   |
| denominacion  | string   |
| coordenadas   | string   |
| latitud       | string   |
| longitud      | string   |
| elev          | float    |
| uom_elev      | string   |
| ref           | string   |
| distancia_ref | float    |
| direccion_ref | string   |
| condicion     | string   |
| control       | string   |
| region        | string   |
| uso           | string   |
| trafico       | string   |
| sna           | string   |
| concesionado  | string   |
| provincia     | string   |

----------
### Resolucion:

En Hive creamos la `dataBase aviacion`, para luego crear las tablas:

![alt text](archivos/e2.png)

#### Creamos Tabla 1:

```sh
CREATE EXTERNAL TABLE aviacion.aeropuerto_tabla(fecha DATE, horaUTC STRING, clase_de_vuelo STRING, clasificacion_de_vuelo STRING, tipo_de_movimiento STRING, aeropuerto STRING, origen_destino STRING, aerolinea_nombre STRING, aeronave STRING, pasajeros INTEGER)
COMMENT ' Aeropuerto Vuelos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/aviacion/aeropuerto_tabla';
```


#### Creamos Tabla 2:

```sh
CREATE EXTERNAL TABLE aviacion.aeropuerto_detalles_tabla(aeropuerto STRING, oac STRING, iata STRING, tipo STRING, denominacion STRING, coordenadas STRING, latitud STRING, longitud STRING, elev FLOAT, uom_elev STRING, ref STRING, distancia_ref FLOAT, direccion_ref STRING, condicion STRING, control STRING, region STRING, uso STRING, trafico STRING, sna STRING, concesionado STRING, provincia STRING)
COMMENT ' Aeropuerto Detalles '
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/aviacion/aeropuerto_detalles_tabla';
```

![alt text](archivos/e21.png)

 

----------------
### 3. Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos columnas creadas.

Los archivos 202206-informe-ministerio.csv y 202206-informe-ministerio.csv → en la tabla aeropuerto_tabla

El archivo aeropuertos_detalle.csv → en la tabla aeropuerto_detalles_tabla.

### Resolucion:

### se genera el Archivo Dag : 

[/Ejercicio 1 - Aviación Civil/aviacion_civil_dag.py](aviacion_civil_dag.py)

![alt text](archivos/e31.png)

--------------------
### 4. Realizar las siguiente transformaciones en los pipelines de datos:

● Eliminar la columna inhab ya que no se utilizará para el análisis.

● Eliminar la columna fir ya que no se utilizará para el análisis.

● Eliminar la columna “calidad del dato” ya que no se utilizará para el análisis.

● Filtrar los vuelos internacionales ya que solamente se analizarán los vuelos domésticos.

● En el campo pasajeros si se encuentran campos en Null convertirlos en 0 (cero).

● En el campo distancia_ref si se encuentran campos en Null convertirlos en 0 (cero).

### Resolucion:

 • transformaciones para la tabla 1, se genera el archivo `transform_tabla1.py`, guardado en `/home/hadoops/scripts`:

```sh
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")

df2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")

df_3 = df.union(df2)

df_3 = df_3.drop("Calidad dato")


df_3.createOrReplaceTempView("vista_aeropuerto")


new_df = spark.sql("""
    SELECT 
        to_date(`Fecha`, 'dd/MM/yyyy') AS fecha, 
        CAST(`Hora UTC` AS STRING) AS horaUTC,
        CAST(`Clase de Vuelo (todos los vuelos)` AS STRING) AS clase_vuelo,
        CAST(`Clasificación Vuelo` AS STRING) AS clasificacion_vuelo,
        CAST(`Tipo de Movimiento` AS STRING) AS tipo_movimiento,
        CAST(`Aeropuerto` AS STRING) AS aeropuerto,
        CAST(`Origen / Destino` AS STRING) AS origen_destino,
        CAST(`Aerolinea Nombre` AS STRING) AS aerolinea_nombre,
        CAST(`Aeronave` AS STRING) AS aeronave,
        CAST(`Pasajeros` AS INTEGER) AS pasajeros
    FROM vista_aeropuerto
""")


new_df = new_df.na.fill(value=0, subset=['pasajeros'])

new_df.write.mode("overwrite").saveAsTable("aviacion.aeropuerto_tabla")
```

 • transformaciones para la tabla 2, se genera el archivo `transform_tabla2.py`, guardado en `/home/hadoops/scripts`:
```
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)



df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

df = df.drop("inhab", "fir")

df = df.na.fill(value=0, subset=["distancia_ref"])

df.createOrReplaceTempView("vista_detalles")


new_df = spark.sql("""
    SELECT 
           cast(local as string) as aeropuerto,
           cast(oaci as string) as oac,
           cast(iata as string) as iata,
           cast(tipo as string) as tipo,
           cast(denominacion as string) as denominacion,
           cast(coordenadas as string) as coordenadas,
           cast(latitud as string) as latitud,
           cast(longitud as string) as longitud,
           cast(elev as float) as elev,
           cast(uom_elev as string) as uom_elev,
           cast(ref as string) as ref,
           cast(distancia_ref as float) as distancia_ref,
           cast(direccion_ref as string) as direccion_ref,
           cast(condicion as string) as condicion,
           cast(region as string) as region,
           cast(uso as string) as uso,
           cast(trafico as string) as trafico,
           cast(sna as string) as sna,
           cast(concesionado as string) as concesionado,
           cast(provincia as string) as provincia
    FROM vista_detalles
""")

filtro_df = new_df.filter(new_df.trafico == 'Nacional')

filtro_df.write.mode("overwrite").saveAsTable("aviacion.aeropuerto_detalles_tabla")


```
![alt text](archivos/e4.png)

-------------------
### 5. Mostrar mediante una impresión de pantalla, que los tipos de campos de las tablas sean los solicitados en el datawarehouse (ej: fecha date, aeronave string, pasajeros integer, etc.)

### Resolucion:

![alt text](archivos/e51.png)

![alt text](archivos/e52.png)

-------------------
### 6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query.

    select count(*) as cantidad_vuelos
    from aeropuerto_tabla
    where (fecha >= '2021-12-01') and (fecha <= '2022-01-31');


![alt text](archivos/e61.png)    

-------------------
### 7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query.


    SELECT SUM(pasajeros) AS total_pasajeros
    FROM aeropuerto_tabla
    WHERE aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA'
    AND fecha >= '2021-01-01'
    AND fecha <= '2022-06-30';  


![alt text](archivos/e71.png)

-------------------
### 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y Resultado de la query.
```sql
  SELECT
    a.fecha AS fecha_salida,
    a.horautc AS hora_salida,
    a.aeropuerto AS aeropuerto_salida,
    salida.ref AS ciudad_salida,
    a.origen_destino AS aeropuerto_llegada,
    arribo.ref AS ciudad_llegada,
    a.pasajeros
FROM
    aeropuerto_tabla a
INNER JOIN
    aeropuerto_detalles_tabla salida ON (salida.aeropuerto = a.aeropuerto)
INNER JOIN
    aeropuerto_detalles_tabla arribo ON (arribo.aeropuerto = a.origen_destino)
WHERE
        a.fecha >= '2022-01-01'
    AND a.fecha <= '2022-06-30'
    AND a.tipo_movimiento = 'Despegue'
ORDER BY 
     fecha_salida DESC ;
```
![alt text](archivos/e82.png)


-------------------
### 9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización
```sql
select
    aerolinea_nombre,
    sum(pasajeros) as cantidad_pasajeros
from
    aeropuerto_tabla
where
    (aerolinea_nombre != '0')
    and (fecha >= '2021-01-01') and (fecha <= '2022-06-30')
group by
    aerolinea_nombre
order by cantidad_pasajeros desc limit 10
```

![alt text](archivos/e91.png)

-------------------
### 10.Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires, exceptuando aquellas aeronaves que no cuentan con nombre. Mostrar consulta y Visualización
```sql
SELECT
    aerotbl.aeronave,
    COUNT(*) AS number_uses
FROM
    aeropuerto_tabla aerotbl
    INNER JOIN aeropuerto_detalles_tabla aerodettbl ON aerotbl.aeropuerto = aerodettbl.aeropuerto
WHERE
    aerotbl.aeronave != '0'
    AND aerotbl.aeronave IS NOT NULL
    AND (aerotbl.fecha >= '2021-01-01' AND aerotbl.fecha <= '2022-06-30')
    AND (aerodettbl.provincia = 'CIUDAD AUTÓNOMA DE BUENOS AIRES' OR aerodettbl.provincia = 'BUENOS AIRES')
GROUP BY
    aerotbl.aeronave
ORDER BY
    number_uses DESC
LIMIT 10;
```
![alt text](archivos/e10.png)

-------------------
### 11.Qué datos externos agregaría en este dataset que mejoraría el análisis de los datos


Se pueden agregar como datos externos para mejorar el análisis de los datos recopilados :

    Datos sobre equipaje, cantidad de maletas o bolsos, peso del equipaje.

    Datos sobre la perdida de equipaje, reclamos sobre el equipaje.

    satisfaccion del cliente.

    Datos sobre mascotas.

    Retrasos y Cancelaciones.

    Medidas de seguridad, incidentes y accidente.
-------------------
### 12.  Elabore sus conclusiones y recomendaciones sobre este proyecto.

Estos análisis y recomendaciones pueden ayudar a la Administración Nacional de Aviación Civil a tomar decisiones informadas y mejorar la gestión del tráfico aéreo en Argentino:

La cantidad de vuelos entre diciembre de 2021 y enero de 2022 puede indicar una tendencia de aumento en los viajes durante las festividades. Se recomienda analizar más años para confirmar esta tendencia.


Aerolíneas Argentinas es una de las aerolíneas más utilizadas. Se recomienda continuar invirtiendo en su infraestructura y servicios para mantener y aumentar su cuota de mercado.


Analizar las rutas más frecuentadas puede ayudar a optimizar la asignación de recursos y mejorar la eficiencia operativa.

Las aeronaves más utilizadas deben ser objeto de un mantenimiento riguroso para garantizar la seguridad y minimizar el tiempo de inactividad.


-------------------
### 13. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Sí aplica)


