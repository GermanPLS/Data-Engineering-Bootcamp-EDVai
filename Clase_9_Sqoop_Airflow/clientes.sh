# Remove old Northwind Analytics files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -r /sqoop/ingest/clientes/*

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/sqoop/scripts/sqoop.pass \
--query 'SELECT cu.customer_id, cu.company_name, SUM(od.quantity) AS productos_vendidos FROM orders AS o INNER JOIN order_details AS od ON o.order_id = od.order_id LEFT JOIN customers AS cu ON o.customer_id = cu.customer_id WHERE $CONDITIONS GROUP BY cu.customer_id, cu.company_name ORDER BY productos_vendidos DESC' \
--m 1 \
--target-dir /sqoop/ingest/clientes \
--as-parquetfile \
--delete-target-dir