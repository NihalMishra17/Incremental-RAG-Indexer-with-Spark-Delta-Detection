#!/bin/bash

BUCKET_NAME="rag-indexer"

echo "=========================================="
echo "Submitting Spark Job to YARN"
echo "=========================================="
echo "Bucket: $BUCKET_NAME"
echo ""

spark-submit \
  --class com.cs441.hw2.SparkDeltaIndexer \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.executor.memory=4g \
  --conf spark.driver.memory=4g \
  --conf spark.executor.instances=2 \
  --driver-java-options "-DINPUT_DIR=s3a://${BUCKET_NAME}/input-pdfs -DOUTPUT_DIR=s3a://${BUCKET_NAME}/delta-output -DSPARK_MASTER=yarn -DOLLAMA_HOST=http://localhost:11434 -DCHECKPOINT_DIR=s3a://${BUCKET_NAME}/checkpoints" \
  s3://${BUCKET_NAME}/jars/hw2-delta-indexer.jar

echo ""
echo "=========================================="
echo "Job submitted to YARN!"
echo "=========================================="
echo ""
echo "Monitor with:"
echo "  yarn application -list"
echo "  yarn application -status <application_id>"
