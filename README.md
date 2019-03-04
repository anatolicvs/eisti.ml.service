# Apache Spark 

    docker build -t aytacozkan/spark:latest . 
    
    docker-compose up --scale spark-worker=3 

    docker run --rm -it --network spark_network \
        aytacozkan/spark:latest /bin/sh

    docker run -it --rm aytacozkan/spark:latest /bin/sh

    chmod +x start-worker.sh
