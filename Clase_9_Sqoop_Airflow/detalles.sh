# Remove old Northwind Analytics files in HDFS

/home/hadoop/hadoop/hdfs dfs -rm -r /sqoop/ingest/order_details/*

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/sqoop/scripts/sqoop.pass \
--query "select od.order_id, od.unit_price, od.quantity, od.discount from order_details od where \$CONDITIONS" \
--m 1 \
--target-dir /sqoop/ingest/order_details \
--as-parquetfile \
--delete-target-dir


