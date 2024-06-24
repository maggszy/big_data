/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://${MASTER_IP}:7077
/opt/spark/bin/spark-shell --master spark://${MASTER_IP}:7077