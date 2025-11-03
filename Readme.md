# CS441 Homework 2: Incremental RAG Indexer with Spark and Delta Lake

**Author:** Nihal Niraj Mishra  
**Email:** nmish@uic.edu

## Video Demonstration
[Link to YouTube video demonstration]

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture & Design](#architecture--design)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Running Locally](#running-locally)
7. [AWS EMR Deployment](#aws-emr-deployment)
8. [Testing](#testing)
9. [Results & Metrics](#results--metrics)
10. [Design Rationale](#design-rationale)
11. [Limitations](#limitations)

---

## Project Overview

This project implements an **incremental delta indexer** for Retrieval Augmented Generation (RAG) systems using Apache Spark and Delta Lake. Unlike traditional batch indexers that reprocess entire corpora, this system intelligently detects and processes only changed documents, significantly reducing computation time and cost.

### Key Features

**Delta Detection** - Only processes new/changed documents using content hash comparison  
**Deterministic Chunking** - Stable chunk IDs based on document ID, position, and content  
**Incremental Embeddings** - Generates vectors only for new chunks  
**ACID Transactions** - Delta Lake ensures atomic updates and consistency  
**Versioned Storage** - Separate tables for documents, chunks, and embeddings  
**Idempotent Operations** - Safe retries without data duplication  
**Scalable Deployment** - Runs on AWS EMR with YARN  
**Comprehensive Logging** - LazyLogging throughout for observability  
**Configuration Management** - Typesafe Config for all parameters

### Technology Stack

- **Language:** Scala 2.12
- **Framework:** Apache Spark 3.5.0
- **Storage:** Delta Lake 3.2.0
- **Embedding Model:** Ollama mxbai-embed-large
- **Cloud Platform:** AWS EMR
- **Build Tool:** SBT 1.9.7
- **Testing:** ScalaTest

---

## Architecture & Design

### System Architecture

```
┌─────────────────┐
│  Input PDFs     │
│  (MSR Corpus)   │
└────────┬────────┘
         │
         v
┌─────────────────────────────────────────────────────────┐
│               Document Scanner                          │
│  - Reads PDF files from S3/local                        │
│  - Extracts text with Apache PDFBox                     │
│  - Generates stable document IDs (hash of filepath)     │
└────────┬────────────────────────────────────────────────┘
         │
         v
┌─────────────────────────────────────────────────────────┐
│               Delta Detector                            │
│  - Computes content hash (MD5) of normalized text       │
│  - Anti-join with existing documents table              │
│  - Identifies new/changed/unchanged/deleted docs        │
└────────┬────────────────────────────────────────────────┘
         │
         v
┌─────────────────────────────────────────────────────────┐
│            Incremental Chunker                          │
│  - Chunks only changed documents                        │
│  - Fixed-size chunks with configurable overlap          │
│  - Generates deterministic chunk IDs                    │
└────────┬────────────────────────────────────────────────┘
         │
         v
┌─────────────────────────────────────────────────────────┐
│          Incremental Embedder                           │
│  - Identifies chunks without embeddings                 │
│  - Calls Ollama API on driver (driver-only pattern)     │
│  - Generates 1024-dim vectors with mxbai-embed-large    │
└────────┬────────────────────────────────────────────────┘
         │
         v
┌─────────────────────────────────────────────────────────┐
│              Storage Layer (Delta Lake)                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  Documents   │  │    Chunks    │  │  Embeddings  │   │
│  │   Table      │  │    Table     │  │    Table     │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│  - ACID transactions                                    │
│  - Time travel & versioning                             │
│  - Schema evolution                                     │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

```
First Run (Cold Start):
Documents → Scan → Chunk → Embed → Store All
- Processes entire corpus from scratch
- Generates all chunks and embeddings
- Establishes baseline state in Delta Lake

Incremental Run (No Changes):
Documents → Delta Detection → Skip All → Zero Work
- Content hashes match existing state
- No chunking or embedding performed
- Near-instant completion with minimal resource usage

Incremental Run (With Changes):
Documents → Delta Detection → Process Changed Only → Merge
- Only new/modified documents are chunked
- Only new chunks receive embeddings
- Existing data preserved, new data merged atomically
- Processing time proportional to change volume
```

### Delta Lake Schema

**documents table:**
```
documentId: String (PK)
filepath: String
filename: String
documentText: String
contentHash: String
documentTimestamp: Timestamp
```

**chunks table:**
```
chunkId: String (PK)
documentId: String (FK)
chunkIndex: Int
chunkText: String
startPos: Int
endPos: Int
contentHash: String
chunkTimestamp: Timestamp
```

**embeddings table:**
```
embeddingId: String (PK)
chunkId: String (FK)
documentId: String
embeddingVector: Array[Double]
embeddingModel: String
embeddingHash: String
embeddingTimestamp: Timestamp
```

---

## Prerequisites

### Local Development

- **JDK 11** or higher
- **Scala 2.12.x**
- **SBT 1.9.7+**
- **Ollama** (for embedding generation)
- **Git**

### AWS EMR Deployment

- **AWS Account** with EMR access
- **AWS CLI** configured with credentials
- **S3 Bucket** for data and artifacts
- **EC2 Key Pair** for SSH access
- **EMR 7.10.0** with Spark 3.5.6

---

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/NihalMishra17/Incremental-RAG-Indexer-with-Spark-Delta-Detection
cd hw2-delta-indexer
```

### 2. Install Dependencies

```bash
sbt compile
```

### 3. Install Ollama (Local Development)

```bash
# macOS/Linux
curl -fsSL https://ollama.com/install.sh | sh

# Pull embedding model
ollama pull mxbai-embed-large

# Verify installation
ollama list
```

---

## Configuration

All configuration is managed through `src/main/resources/application.conf` using Typesafe Config.

### Key Configuration Parameters

```hocon
delta-indexer {
  # Input/Output paths (overridable via environment variables)
  input-dir = "test-corpus"
  input-dir = ${?INPUT_DIR}
  
  output-dir = "delta-output"
  output-dir = ${?OUTPUT_DIR}

  # Chunking configuration
  chunking {
    chunk-size = 1000      # Characters per chunk
    overlap = 100          # Overlap between chunks
    version = "v1"
  }

  # Embedding configuration
  embedding {
    model = "mxbai-embed-large"
    version = "v1"
    dimension = 1024
    ollama-host = "http://localhost:11434"
    ollama-host = ${?OLLAMA_HOST}
  }

  # Spark configuration
  spark {
    app-name = "HW2-Delta-Indexer"
    master = "local[*]"
    master = ${?SPARK_MASTER}
  }
}
```

### Environment Variables (EMR Override)

```bash
export INPUT_DIR="s3a://rag-indexer/input-pdfs"
export OUTPUT_DIR="s3a://rag-indexer/delta-output"
export SPARK_MASTER="yarn"
export OLLAMA_HOST="http://$(hostname -I | awk '{print $1}'):11434"
```

---

## Running Locally

### 1. Prepare Test Corpus

```bash
# Create test directory
mkdir -p test-corpus

# Copy PDF files
cp ~/path/to/MSRCorpus/*.pdf test-corpus/
```

### 2. Start Ollama Service

```bash
# Start Ollama
ollama serve &

# Verify it's running
curl http://localhost:11434/api/tags
```

### 3. Run the Indexer

```bash
# First run (processes all documents)
sbt run

# Subsequent run (incremental - skips unchanged)
sbt run
```

### 4. Check Results

```bash
# View Delta tables
ls -la delta-output/

# Check statistics CSV
cat delta-output/run-stats-*.csv
```

---

## AWS EMR Deployment

### Step 1: Prepare S3 Bucket

```bash
# Create bucket
aws s3 mb s3://rag-indexer

# Upload PDF corpus
aws s3 cp test-corpus/ s3://rag-indexer/input-pdfs/ --recursive

# Create directories
aws s3api put-object --bucket rag-indexer --key delta-output/
aws s3api put-object --bucket rag-indexer --key jars/
aws s3api put-object --bucket rag-indexer --key scripts/
```

### Step 2: Build and Upload JAR

```bash
# Build assembly JAR
sbt clean assembly

# Upload to S3
aws s3 cp target/scala-2.12/hw2-delta-indexer.jar s3://rag-indexer/jars/
```

### Step 3: Upload Bootstrap Script

```bash
aws s3 cp bootstrap-ollama.sh s3://rag-indexer/scripts/
```

### Step 4: Create EMR Cluster

```bash
aws emr create-cluster \
  --name "RAG-Indexer-Cluster" \
  --release-label emr-7.10.0 \
  --applications Name=Spark Name=Hadoop \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --bootstrap-actions \
    Path=s3://rag-indexer/scripts/bootstrap-ollama.sh \
  --ec2-attributes KeyName=your-key-pair \
  --use-default-roles \
  --region us-east-2 \
  --log-uri s3://rag-indexer/logs/
```

### Step 5: Upload Run Script

```bash
aws s3 cp run-spark-emr.sh s3://rag-indexer/scripts/
```

### Step 6: SSH to Master and Run

```bash
# Get cluster ID
aws emr list-clusters --active

# Get master public DNS
aws emr describe-cluster --cluster-id j-XXXXX --query 'Cluster.MasterPublicDnsName'

# SSH to master
ssh -i your-key-pair.pem hadoop@ec2-xxx.compute.amazonaws.com

# On master node:
aws s3 cp s3://rag-indexer/jars/hw2-delta-indexer.jar .
aws s3 cp s3://rag-indexer/scripts/run-spark-emr.sh .
chmod +x run-spark-emr.sh
./run-spark-emr.sh
```

### Step 7: Retrieve Results

```bash
# Download output
aws s3 cp s3://rag-indexer/delta-output/ ./results/ --recursive

# View statistics
cat results/run-stats-*.csv

# Check logs
aws s3 cp s3://rag-indexer/logs/ ./logs/ --recursive
```

---

## Testing

### Run All Tests

```bash
sbt test
```

### Test Coverage

The project includes **11 unit tests** covering:

**TextChunkerTest (6 tests):**
- Empty text handling
- Text smaller than chunk size
- Overlapping chunk creation
- Deterministic chunking
- Invalid parameter handling
- Indexed chunk generation

**HashUtilsTest (5 tests):**
- SHA-256 consistency
- Hash uniqueness
- Deterministic chunk ID generation
- Document ID normalization
- MD5 hash format

### Sample Test Output

```
[info] TextChunkerTest:
[info] - chunk should handle empty text
[info] - chunk should handle text smaller than chunk size
[info] - chunk should create overlapping chunks
[info] - chunk should be deterministic
[info] - chunk should handle invalid parameters gracefully
[info] - chunkWithIndex should return indexed chunks
[info] HashUtilsTest:
[info] - sha256 should produce consistent hashes
[info] - sha256 should produce different hashes for different content
[info] - generateChunkId should be deterministic
[info] - generateDocumentId should normalize path separators
[info] - md5 should produce 32 character hash
[info] Run completed in 707 milliseconds.
[info] Total number of tests run: 11
[info] Suites: completed 2, aborted 0
[info] Tests: succeeded 11, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

---



### Sample Statistics CSV

```csv
metric,value
timestamp,2025-11-02T05-51-48
run_type,first_run
environment,yarn
total_documents,5
total_chunks,167
total_embeddings,167
new_documents,5
efficiency_pct,0.00
duration_seconds,180
```


---

## Design Rationale

### 1. Why Delta Lake?

**ACID Transactions:** Ensures consistency during concurrent writes and failures. If a Spark job fails mid-way, Delta Lake prevents partial writes.

**Time Travel:** Can roll back to previous versions if needed:
```scala
spark.read.format("delta")
  .option("versionAsOf", 0)
  .load("delta-output/documents")
```

**Schema Evolution:** Allows adding new columns without breaking existing code:
```scala
spark.sql("ALTER TABLE documents ADD COLUMN author STRING")
```

**Performance:** Optimized file layout and Z-ordering for fast queries:
```scala
deltaTable.optimize().executeZOrderBy("documentId")
```

### 2. Why Driver-Only Embeddings?

**Network Isolation:** Worker nodes can't access master's Ollama service by default due to security groups and localhost binding.

**Simplicity:** Avoids complex networking setup and ensures deterministic execution.

**Acceptable for Homework Scale:** With 167 chunks, serial processing on driver completes in ~3 minutes.

**Production Alternative:** Would use distributed embedding service (SageMaker, Bedrock) or install Ollama on all nodes.

### 3. Why Content Hashing?

**Change Detection:** MD5 hash of normalized text detects actual content changes, not just metadata updates.

**Idempotency:** Same content → same hash → skip processing. Retries don't duplicate work.

**Deterministic IDs:** Chunk IDs derived from document ID + position + content hash ensure stability across runs.

### 4. Why Typesafe Config?

**Environment Flexibility:** Same code runs locally (local[*]) and on EMR (yarn) by changing config.

**Security:** Credentials and paths stay out of source code.

**Testability:** Easy to swap configs for unit tests vs integration tests.

### 5. Functional Programming Choices

**No Mutable State:** Uses `val`, functional transformations (map, filter, flatMap) instead of `var` and loops.

**Rationale:** Easier reasoning about parallel Spark operations, safer for distributed execution.

**Example:**
```scala
// Functional approach (used)
val startPositions = (0 until text.length by step).toSeq
startPositions.map { start => text.substring(start, end) }

// Imperative approach (avoided)
var i = 0
while (i < text.length) {
  chunks += text.substring(i, end)
  i += step
}
```

### 6. Logging Strategy

**LazyLogging:** Doesn't evaluate log messages unless level is enabled (performance).

**Structured Levels:**
- **INFO:** Pipeline progress, statistics
- **WARN:** Skipped/empty data
- **ERROR:** Failures that don't crash job
- **DEBUG:** Detailed chunk/embedding info

**Observable:** Logs capture every decision for debugging and audit.

---

## Limitations

### 1. Scalability Constraints

**Driver-Only Embeddings:** Current implementation generates embeddings serially on driver. For large corpora (10K+ documents), this becomes a bottleneck.

**Solution:** Implement distributed embedding with HTTP-based service (SageMaker, Bedrock) or install Ollama on all workers.

### 2. Chunking Strategy

**Fixed-Size Chunks:** Uses simple character-based chunking with overlap. Doesn't respect semantic boundaries (sentences, paragraphs).

**Better Approach:** Semantic chunking using sentence boundaries, section headers, or sliding window with sentence tokenization.

### 3. Embedding Model

**Single Model:** Only supports mxbai-embed-large. No multi-model versioning.

**Enhancement:** Add model version management, support for multiple embedders, A/B testing framework.

### 4. Error Handling

**Fail-Fast:** If Ollama crashes or model unavailable, entire job fails.

**Improvement:** Implement retry logic, circuit breakers, fallback to cached embeddings.

### 5. Query Interface

**No Retrieval API:** Builds index but doesn't provide query endpoint.

**Next Step:** Add FAISS/Annoy for vector similarity search, REST API for queries.

### 6. Cost Optimization

**Always-On Ollama:** Runs Ollama even when no work needed.

**Optimization:** Use AWS Lambda for on-demand embedding, or ECS Fargate with auto-scaling.

### 7. Schema Evolution

**No Version Migration:** Changing schema requires manual data migration.

**Solution:** Implement Delta Lake schema evolution with backward compatibility checks.


