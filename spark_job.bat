cd C:\Users\yogeshja\Desktop\Dissertation
call activate dissertation
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --driver-class-path postgresql-42.2.5.jar --jars postgresql-42.2.5.jar spark_processing.py
pause