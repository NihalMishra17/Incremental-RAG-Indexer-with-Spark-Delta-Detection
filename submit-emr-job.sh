#!/bin/bash

# Read saved values
BUCKET_NAME=$(cat bucket-name.txt)
MASTER_DNS=$(cat master-dns.txt)
KEY_FILE="$HOME/.ssh/emr-rag-keypair.pem"

echo "=========================================="
echo "Submitting Spark Job to EMR"
echo "=========================================="
echo "Bucket: $BUCKET_NAME"
echo "Master: $MASTER_DNS"
echo ""

# Create remote script
cat > /tmp/run-spark-job.sh << REMOTESCRIPT
#!/bin/bash

BUCKET_NAME="$BUCKET_NAME"

echo "Starting Spark job submission..."
echo "Bucket: \$BUCKET_NAME"
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
  --driver-java-options "-DINPUT_DIR=s3a://\${BUCKET_NAME}/input-pdfs -DOUTPUT_DIR=s3a://\${BUCKET_NAME}/delta-output -DSPARK_MASTER=yarn -DOLLAMA_HOST=http://localhost:11434 -DCHECKPOINT_DIR=s3a://\${BUCKET_NAME}/checkpoints" \
  s3://\${BUCKET_NAME}/jars/hw2-delta-indexer.jar

echo ""
echo "=========================================="
echo "Job submitted to YARN!"
echo "=========================================="
echo "Check YARN UI for application ID and logs"
REMOTESCRIPT

# Copy script to master
echo "Copying script to master node..."
scp -i $KEY_FILE /tmp/run-spark-job.sh hadoop@$MASTER_DNS:/home/hadoop/

# Execute on master
echo "Executing job..."
ssh -i $KEY_FILE hadoop@$MASTER_DNS "chmod +x /home/hadoop/run-spark-job.sh && /home/hadoop/run-spark-job.sh"

echo ""
echo "=========================================="
echo "Job submitted! Monitor progress:"
echo "  ssh -i $KEY_FILE hadoop@$MASTER_DNS"
echo "  yarn application -list"
echo "=========================================="
