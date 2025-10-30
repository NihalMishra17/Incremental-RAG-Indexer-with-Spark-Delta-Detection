#!/bin/bash

# Get master's private IP
MASTER_IP=$(hostname -I | awk '{print $1}')
echo "==========================================
Running Spark Job on EMR (Client Mode)
==========================================
Master IP: $MASTER_IP
Bucket: rag-indexer
"

# Run spark-submit in client mode (logs appear in terminal)
spark-submit \
  --class com.cs441.hw2.SparkDeltaIndexer \
  --master yarn \
  --deploy-mode client \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf "spark.driver.extraJavaOptions=-DINPUT_DIR=s3a://rag-indexer/input-pdfs -DOUTPUT_DIR=s3a://rag-indexer/delta-output -DSPARK_MASTER=yarn -DCHECKPOINT_DIR=s3a://rag-indexer/checkpoints -DOLLAMA_HOST=http://$MASTER_IP:11434" \
  hw2-delta-indexer.jar 2>&1 | tee spark-job-output.log

echo "
==========================================
Job Complete!
==========================================
Check S3 output: aws s3 ls s3://rag-indexer/delta-output/ --recursive
Full logs saved to: spark-job-output.log
"