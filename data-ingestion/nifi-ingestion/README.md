# MyTemplate - Apache NiFi Data Flow

## Overview

`MyTemplate (2).xml` is an **Apache NiFi template** that defines a complete flow for generating flow files and writing them into **Hadoop Distributed File System (HDFS)**. It includes failure handling and logging mechanisms to help track and manage data processing in real-time.

---

##  Components Breakdown

###  Flow Summary

1. **GenerateFlowFile**: Creates flow files with dummy text content.
2. **PutHDFS (Success)**: Sends successfully processed files to a configured HDFS directory.
3. **PutHDFS (Failure)**: Stores failed files in a separate HDFS failure directory.
4. **LogAttribute**: Logs the attributes of FlowFiles for monitoring and debugging.

---

###  Processors

#### 1. `GenerateFlowFile`
- **Type**: `org.apache.nifi.processors.standard.GenerateFlowFile`
- **Function**: Generates test data.
- **Properties**:
  - `Data Format`: `Text`
  - `Character Set`: `UTF-8`
  - `Batch Size`: `1`
  - `Scheduling`: Every `1 minute`

#### 2. `PutHDFS (Success)`
- **Type**: `org.apache.nifi.processors.hadoop.PutHDFS`
- **Function**: Writes flow files to HDFS on **success** path.
- **HDFS Directory**: `/user/latifa/data/success`
- **Configuration Files**: `/home/latifa/nifi/conf/core-site.xml, hdfs-site.xml`
- **Strategy**: `writeAndRename`, `fail` on conflict
- **Compression**: `NONE`

#### 3. `PutHDFS (Failure)`
- **Type**: `org.apache.nifi.processors.hadoop.PutHDFS`
- **Function**: Handles failed files and writes them to a failure path.
- **HDFS Directory**: `/user/latifa/data/failure`
- **Similar configs as success processor**

#### 4. `LogAttribute`
- **Type**: `org.apache.nifi.processors.standard.LogAttribute`
- **Function**: Logs attributes and properties of FlowFiles for debugging.
- **Log Level**: `INFO`
- **Logs**:
  - Payload: `false`
  - FlowFile Properties: `true`
  - Output Format: `Line per Attribute`

---

###  Connections

- The processors are connected with relationships:
  - `success` path routes to either `PutHDFS (Success)` or `LogAttribute`
  - `failure` path routes to `PutHDFS (Failure)`

- **Backpressure Thresholds**:
  - Data Size: `1 GB`
  - Object Count: `10,000`

- **No Load Balancing**, no flow file expiration.

---

##  Process Group: `Latifa`

Encapsulates part of the flow:
- Includes its own internal `GenerateFlowFile` and `PutHDFS` processors.
- Funnels are used to collect flows based on `success` and `failure`.

---

##  Requirements

Before using this template, ensure:

- **Apache NiFi 1.28.1** or later is installed.
- **HDFS is set up** and accessible.
- The following files exist:
  - `/home/latifa/nifi/conf/core-site.xml`
  - `/home/latifa/nifi/conf/hdfs-site.xml`
- HDFS directories are created:
  - `/user/latifa/data/success`
  - `/user/latifa/data/failure`
- Optional: Kerberos settings can be configured but are empty by default.

---

##  Usage

1. **Import the template** into Apache NiFi:
   - Go to the NiFi UI > Menu > Templates > Import Template

2. **Drag the template** onto the canvas.

3. **Configure credentials** or paths if needed.

4. **Start the processors**:
   - Ensure HDFS is reachable
   - Enable required controller services (if any)

5. **Monitor Flow**:
   - Use `LogAttribute` and NiFi UI for tracking

---

##  Notes

- This is ideal for **testing HDFS integration** or **setting up basic flow pipelines**.
- Can be extended with more processing logic or real data sources.
- Kerberos integration is included but not configured — useful for secure clusters.

---


##  Author

Latifa2OUISSADAN — Project: NiFi + HDFS integration

