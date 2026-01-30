# Real-Time User Activity Data Pipeline

This project implements a real-time data pipeline using:

- Apache Kafka (event streaming)
- Apache Spark Structured Streaming (processing)
- PostgreSQL (analytics storage)
- Parquet Data Lake (raw storage)
- Docker Compose (container orchestration)

## Pipeline Flow
Producer → Kafka → Spark Streaming →  
• Postgres Aggregations  
• Parquet Data Lake  
• Enriched Kafka Topic  

## Features
- Windowed page view counts
- Active user tracking
- Sessionization with stateful streaming
- Watermark handling for late data
- Exactly-once processing via checkpoints

## How to Run
1. docker-compose up --build
2. python producer.py
3. Query Postgres tables
4. Consume enriched Kafka topic
