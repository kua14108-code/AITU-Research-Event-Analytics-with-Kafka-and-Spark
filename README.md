# Assignment 4 - Kafka + Spark research-event pipeline (synthetic AITU data)

Files: stream_logic.py (validation + windows), producer.py, consumer.py, offline_run.py,
spark_stream.py, load_test.py, spark_load.py, spark_opt.py, docker-compose.yml, data/events.csv, logs/.

## Run with a real broker
    docker compose up -d
    docker exec <kafka> /opt/kafka/bin/kafka-topics.sh --create --topic aitu.research.jobs \
        --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
    pip install kafka-python pyspark psutil
    python consumer.py            # terminal 1 (group aitu-analytics)
    python producer.py            # terminal 2
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 spark_stream.py kafka localhost:9092

## Run without a broker (what produced logs/)
    python offline_run.py          # consumer logic on data/events.csv
    python spark_stream.py files   # Spark Structured Streaming, JSON file source
    python load_test.py; python spark_load.py; python spark_opt.py
