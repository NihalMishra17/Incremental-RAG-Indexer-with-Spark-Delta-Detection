# HW2: Incremental RAG Indexer with Delta Lake

## Overview

This project implements a production-grade incremental indexing system for Retrieval-Augmented Generation (RAG) using Apache Spark and Delta Lake. The system efficiently processes PDF documents by detecting changes and only reprocessing modified content, achieving up to 80-100% deduplication efficiency.

## Key Features

### Core Functionality
- **Incremental Processing**: Only processes new or changed documents using anti-join operations
- **Delta Lake Integration**: ACID transactions with versioned transaction logs
- **Change Detection**: Identifies 4 types of changes (new, modified, unchanged, deleted)
- **Deduplication**: 80-100% efficiency by skipping unchanged documents
- **PDF Processing**: Extracts and chunks text from PDF documents
- **Vector Embeddings**: Generates embeddings using Ollama's mxbai-embed-large model
- **Distributed Computing**: Leverages Apache Spark for scalable processing

### Technical Stack
- **Apache Spark 3.3.2**: Distributed data processing
- **Delta Lake 2.3.0**: ACID transactions and versioning
- **Scala 2.13.8**: Primary programming language
- **Apache PDFBox**: PDF text extraction
- **Ollama**: Local embedding generation (mxbai-embed-large)
- **Lucene 9.7.0**: Vector indexing and retrieval

## Architecture

### Component Overview
```
┌─────────────────────────────────────────────────────────────┐
│                    SparkDeltaIndexer                        │
│                   (Main Orchestrator)                       │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
│ DocumentScanner  │ │DeltaDetector │ │  StorageLayer    │
│  (PDF Reading)   │ │(Anti-Joins)  │ │  (Delta Lake)    │
└──────────────────┘ └──────────────┘ └──────────────────┘
          │                   │                   │
          ▼                   ▼                   ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
│IncrementalChunker│ │   Embedder   │ │ Vector Indexer   │
│ (Text Splitting) │ │   (Ollama)   │ │    (Lucene)      │
└──────────────────┘ └──────────────┘ └──────────────────┘
```

### Delta Detection Algorithm

The system uses Spark anti-joins to efficiently detect four types of document changes:

1. **New Documents**: `current.anti_join(previous, "documentId")`
2. **Changed Documents**: `current.join(previous, "documentId").filter(contentHash differs)`
3. **Unchanged Documents**: `current.join(previous, ["documentId", "contentHash"])`
4. **Deleted Documents**: `previous.anti_join(current, "documentId")`

### Data Flow
```
PDF Files → Document Scanning → Delta Detection → Incremental Chunking
                                       ↓
                              Only Changed Docs
                                       ↓
                    ┌──────────────────┴──────────────────┐
                    ↓                                      ↓
          Embedding Generation                    Vector Indexing
                    ↓                                      ↓
              Delta Lake Storage                   Lucene Index
         (ACID Transactions + Versioning)
```

## Prerequisites

### Required Software
- **Java 11** (required for module compatibility)
- **Scala 2.13.8**
- **SBT 1.9.7+**
- **Apache Spark 3.3.2**
- **Ollama** with mxbai-embed-large model

### Install Ollama and Model
```bash
# Install Ollama
brew install ollama

# Start Ollama service
ollama serve

# Pull the embedding model (in another terminal)
ollama pull mxbai-embed-large
```

### Set Java 11
```bash
# Check available Java versions
/usr/libexec/java_home -V

# Set Java 11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Verify
java -version
# Should show: openjdk version "11.x.x"

# Make permanent (add to ~/.zshrc)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
```

## Project Structure
```
hw2-delta-indexer/
├── src/
│   ├── main/
│   │   ├── scala/com/cs441/hw2/
│   │   │   ├── SparkDeltaIndexer.scala    # Main orchestrator
│   │   │   ├── DocumentScanner.scala       # PDF processing
│   │   │   ├── DeltaDetector.scala         # Change detection
│   │   │   ├── IncrementalChunker.scala    # Text chunking
│   │   │   ├── IncrementalEmbedder.scala   # Embedding generation
│   │   │   ├── StorageLayer.scala          # Delta Lake interface
│   │   │   ├── VectorIndexer.scala         # Lucene indexing
│   │   │   └── OllamaClient.scala          # Ollama API client
│   │   └── resources/
│   │       ├── application.conf             # Configuration
│   │       ├── logback.xml                  # Logging config (sbt)
│   │       └── log4j2.properties            # Logging config (spark)
│   └── test/
│       └── scala/com/cs441/hw2/
│           └── [Unit Tests]
├── test-corpus/                             # Input PDF directory
├── delta-output/                            # Delta Lake tables
│   ├── documents/
│   │   └── _delta_log/                     # Transaction logs
│   ├── chunks/
│   │   └── _delta_log/
│   └── embeddings/
│       └── _delta_log/
├── build.sbt                                # Build configuration
└── README.md
```

## Configuration

### application.conf
```hocon
app {
  corpus-dir = "test-corpus"
  output-dir = "delta-output"
  
  spark {
    app-name = "DeltaIndexer"
    master = "local[*]"
  }
  
  chunking {
    chunk-size = 512
    chunk-overlap = 50
  }
  
  embedding {
    model = "mxbai-embed-large"
    ollama-url = "http://localhost:11434"
    batch-size = 32
  }
  
  retrieval {
    top-k = 5
  }
}
```

## Building the Project
```bash
# Clone the repository
git clone <your-repo-url>
cd hw2-delta-indexer

# Clean build
sbt clean

# Compile
sbt compile

# Run tests
sbt test

# Build fat JAR
sbt assembly
```

## Running the Application

### Development Mode (SBT)
```bash
# Ensure Ollama is running
ollama serve

# Run with SBT (shows detailed INFO logs)
sbt run
```

### Production Mode (spark-submit)
```bash
# Build the JAR
sbt assembly

# Run with spark-submit (with detailed logging)
spark-submit \
  --class com.cs441.hw2.SparkDeltaIndexer \
  --master "local[*]" \
  --driver-java-options "-Dlog4j.configurationFile=file:src/main/resources/log4j2.properties" \
  target/scala-2.13/hw2-delta-indexer.jar
```

**Note:** The `--driver-java-options` flag enables detailed INFO logging. Without it, only WARN/ERROR messages are shown.

### Alternative: Run without detailed logs
```bash
spark-submit \
  --class com.cs441.hw2.SparkDeltaIndexer \
  --master "local[*]" \
  target/scala-2.13/hw2-delta-indexer.jar
```

## Usage Examples

### First Run (Full Indexing)
```bash
# Place PDF files in test-corpus/
cp /path/to/pdfs/*.pdf test-corpus/

# Run the indexer
sbt run

# Expected output:
# No previous state found. Running first-time indexing...
# ================================================================================
# FIRST RUN: Processing entire corpus
# ================================================================================
# Documents: 5 | Chunks: 145 | Embeddings: 145
```

### Incremental Run (After Adding Files)
```bash
# Add new PDF
cp /path/to/new.pdf test-corpus/

# Run again
sbt run

# Expected output:
# Previous state found. Running incremental indexing...
# ================================================================================
# INCREMENTAL RUN: Processing changes only
# ================================================================================
# Previous state: 5 documents
# Current corpus: 6 documents
# Delta stats: new -> 1, changed -> 0, unchanged -> 5
# Deduplication ratio: 83.33%
# Processed: 1 docs | New chunks: 31 | New embeddings: 31
# Efficiency: Skipped 83.33% of corpus
```

### No Changes Run
```bash
# Run without changes
sbt run

# Expected output:
# Deduplication ratio: 100.00%
# No changes detected. Nothing to process.
```

## Performance Results

### Test Corpus: 5 PDF Documents

#### First Run (Baseline)
- **Documents processed**: 5
- **Chunks created**: 145
- **Embeddings generated**: 145
- **Processing mode**: Full corpus
- **Execution time**: ~76 seconds

#### Incremental Run (1 New Document)
- **Previous documents**: 5
- **Current documents**: 6
- **New documents**: 1
- **Changed documents**: 0
- **Unchanged documents**: 5
- **Deduplication ratio**: 83.33%
- **New chunks**: 31
- **Total chunks**: 176
- **Efficiency gain**: Skipped 83.33% of corpus
- **Execution time**: ~20 seconds

#### No Changes Run
- **Deduplication ratio**: 100.00%
- **Documents processed**: 0
- **Processing time**: ~15 seconds (detection only)

## Delta Lake Verification

### Check Transaction Logs
```bash
# View all transaction log files
find delta-output -name "*.json" -type f

# Expected output:
# delta-output/documents/_delta_log/00000000000000000000.json  (version 0)
# delta-output/documents/_delta_log/00000000000000000001.json  (version 1)
# delta-output/chunks/_delta_log/00000000000000000000.json
# delta-output/chunks/_delta_log/00000000000000000001.json
# delta-output/embeddings/_delta_log/00000000000000000000.json
# delta-output/embeddings/_delta_log/00000000000000000001.json
```

### Inspect Transaction History
```bash
# View transaction metadata for documents table
cat delta-output/documents/_delta_log/00000000000000000000.json | jq .

# Check version count
ls delta-output/documents/_delta_log/*.json | wc -l
```

## Testing

### Run All Tests
```bash
sbt test
```

### Test Coverage

The project includes comprehensive unit tests:

- **DocumentScannerTest**: PDF scanning and hashing
- **DeltaDetectorTest**: Change detection with anti-joins
- **IncrementalChunkerTest**: Text chunking logic
- **IncrementalEmbedderTest**: Embedding generation (mocked)
- **StorageLayerTest**: Delta Lake operations
- **VectorIndexerTest**: Lucene indexing

### Test Output Example
```
[info] DocumentScannerTest:
[info] - should scan and hash PDF documents
[info] - should handle empty directories
[info] DeltaDetectorTest:
[info] - should detect new documents
[info] - should detect changed documents
[info] - should detect unchanged documents
[info] - should calculate deduplication ratio
[info] All tests passed
```

## Troubleshooting

### Common Issues

#### 1. Java Version Mismatch
```bash
# Error: Illegal reflective access operation
# Solution: Switch to Java 11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

#### 2. Ollama Connection Failed
```bash
# Error: Connection refused to localhost:11434
# Solution: Start Ollama service
ollama serve

# Verify it's running
curl http://localhost:11434/api/tags
```

#### 3. No INFO Logs in spark-submit
```bash
# Use the --driver-java-options flag:
spark-submit \
  --class com.cs441.hw2.SparkDeltaIndexer \
  --master "local[*]" \
  --driver-java-options "-Dlog4j.configurationFile=file:src/main/resources/log4j2.properties" \
  target/scala-2.13/hw2-delta-indexer.jar
```

#### 4. SBT Cleanup Error
```bash
# Harmless error at end of sbt run about hadoop-client-api
# This occurs AFTER successful completion
# All data is saved before this message appears
# Can be ignored or suppressed with fork := true in build.sbt
```

#### 5. Delta Lake Table Not Found
```bash
# First run will create new tables
# Subsequent runs will load existing tables
# To reset: rm -rf delta-output/
```

## Technical Details

### Content-Based Hashing

Documents are identified by SHA-256 hash of their content:
```scala
def computeHash(content: String): String = {
  MessageDigest.getInstance("SHA-256")
    .digest(content.getBytes)
    .map("%02x".format(_))
    .mkString
}
```

This ensures:
- Identical files are deduplicated
- Content changes are detected
- File renames don't trigger reprocessing

### Anti-Join Operations

Change detection uses Spark's optimized anti-joins:
```scala
// New documents
val newDocs = currentDocs.join(
  previousDocs,
  currentDocs("documentId") === previousDocs("documentId"),
  "left_anti"
)

// Changed documents
val changedDocs = currentDocs.join(
  previousDocs,
  currentDocs("documentId") === previousDocs("documentId")
).filter(
  currentDocs("contentHash") =!= previousDocs("contentHash")
)
```

### Delta Lake ACID Guarantees

Each write creates a new transaction log entry:
```scala
documents.write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save(documentsPath)
```

Transaction logs provide:
- **Atomicity**: All-or-nothing commits
- **Consistency**: Schema enforcement
- **Isolation**: Concurrent read/write safety
- **Durability**: Crash recovery
- **Time Travel**: Query historical versions

## Known Behavior

### Harmless Warnings

1. **Hostname Resolution Warning**:
```
   WARN Utils: Your hostname resolves to a loopback address
```
- Normal for local development
- Does not affect functionality

2. **Native Hadoop Library Warning**:
```
   WARN NativeCodeLoader: Unable to load native-hadoop library
```
- Falls back to Java implementation
- No performance impact for local mode

3. **PDF Font Warnings**:
```
   WARN PDSimpleFont: No Unicode mapping for circlecopyrt
```
- Missing font mappings in PDFs
- Text extraction still works

4. **SBT Cleanup Error** (only with `sbt run`):
```
   NoSuchFileException: hadoop-client-api-3.3.2.jar
```
- Occurs AFTER successful completion
- All data is saved before this message
- Fixed by using `fork := true` in build.sbt

## Dependencies

### Core Dependencies
```scala
// Spark & Delta Lake
"org.apache.spark" %% "spark-core" % "3.3.2"
"org.apache.spark" %% "spark-sql" % "3.3.2"
"io.delta" %% "delta-core" % "2.3.0"

// PDF Processing
"org.apache.pdfbox" % "pdfbox" % "2.0.31"

// HTTP Client for Ollama
"com.softwaremill.sttp.client3" %% "core" % "3.8.15"
"com.softwaremill.sttp.client3" %% "circe" % "3.8.15"

// JSON Processing
"io.circe" %% "circe-core" % "0.14.5"
"io.circe" %% "circe-generic" % "0.14.5"
"io.circe" %% "circe-parser" % "0.14.5"

// Lucene
"org.apache.lucene" % "lucene-core" % "9.7.0"
"org.apache.lucene" % "lucene-queryparser" % "9.7.0"

// Logging & Config
"ch.qos.logback" % "logback-classic" % "1.4.11"
"com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
"com.github.pureconfig" %% "pureconfig" % "0.17.4"
```


## Future Enhancements

1. **Distributed Ollama**: Support for distributed embedding generation
2. **S3 Integration**: Store Delta Lake tables on S3
3. **Query Interface**: REST API for semantic search
4. **Web UI**: Dashboard for monitoring and visualization
5. **Multiple Models**: Support for different embedding models
6. **Batch Scheduling**: Automated incremental updates
7. **Performance Metrics**: Detailed profiling and optimization
8. **Multi-format Support**: Word, HTML, Markdown processing


## Author

**Nihal** - University of Illinois Chicago

