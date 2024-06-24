export $(grep -v '^#' .env | xargs)
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://${MASTER_IP}:7077 /opt/app/src/app.py
