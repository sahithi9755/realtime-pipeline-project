#!/bin/bash

echo "Waiting for Kafka broker at kafka:9092..."

while ! nc -z kafka 9092; do
  echo "Kafka broker not reachable yet..."
  sleep 5
done

echo "Kafka broker is reachable. Starting Spark..."

exec spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
  main.py

