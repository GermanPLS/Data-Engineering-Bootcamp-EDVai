# Remove old Northwind Analytics files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -r /sqoop/ingest/envios/*

# Download Clientes from Northwind DB

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password edvai \
--query "select o.order_id, cast(o.shipped_date as varchar), c.company_name, c.phone from orders o left join customers c on o.customer_id = c.customer_id where \$CONDITIONS" \
--m 1 \
--target-dir /sqoop/ingest/envios \
--as-parquetfile \
--delete-target-dir