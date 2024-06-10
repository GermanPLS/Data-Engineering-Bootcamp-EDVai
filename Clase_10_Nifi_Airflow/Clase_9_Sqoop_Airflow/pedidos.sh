# Remove old Northwind Analytics files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -r /sqoop/ingest/envios/*

# Download Clientes from Northwind DB

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/sqoop/scripts/sqoop.pass \
--query "SELECT o.order_id, o.shipped_date, c.company_name, c.phone FROM orders o JOIN customers c on c.customer_id = o.customer_id WHERE \$CONDITIONS " \
--m 1 \
--target-dir /sqoop/ingest/envios \
--as-parquetfile \
--delete-target-dir