# **Architecture Specification: High-Throughput Clinical Data Ingest & Anonymization**

This document outlines the architectural requirements, production design, and prototype specifications for a clinical data system designed to handle massive ingestion volumes while maintaining strict data privacy and historical integrity.

---

## **1\. System Requirements**

The primary objective is to build a highly resilient pipeline capable of processing clinical attributes (specifically a patient’s "favorite color") while ensuring deduplication and generating unique identifiers.

### **Performance & Scale**

* **Throughput:** The system must handle **1,000,000 writes per minute**. Given a request size of **20KB**, the aggregate data volume is approximately **20GB per minute**.  
* **Latency:** Data must be post-processed and available for query within a **2-second** window from the time of ingestion.  
* **Validation:** A successful functional test requires 1 million records to be written within 60 seconds, with query availability verified within 5 seconds of the write completion.

### **Data Logic & Privacy**

* **Identity Resolution:** Patients are uniquely identified by the combination of **Name** and **Date of Birth (DOB)**.  
* **Deduplication:** Every incoming record must be checked against existing state.  
* **Identifier Management:** Each unique person must be assigned a persistent **Unique Patient ID**. If a person exists, their existing ID is paired with the incoming data; otherwise, a new ID is generated.  
* **Storage Requirements:**  
  1. **Auditable Record:** A master store containing all data (PHI \+ Patient ID \+ Favorite Color).  
  2. **Public Record:** An anonymized store containing only the Patient ID and Favorite Color (PII/PHI removed).  
* **Historical Integrity (to anticipate Option B):** The system must support **Point-in-Time snapshots**, allowing analysts to recreate the state of the data as it existed at any specific moment. (This requirement anticipates the work that would be required for option B in the take-home assignment.)

### **Operational Constraints**

* **Race Conditions:** It is guaranteed that no race conditions will occur where duplicate patients are written to the database simultaneously.

---

## **2\. Production Design**

The production solution leverages managed cloud services to provide the necessary scale, security, and "exactly-once" delivery guarantees without vendor lock-in.

### **Ingestion Layer: Amazon MSK Express**

To handle 20GB/min efficiently, **Amazon MSK Express** is selected.

* **Kafka Advantage:** Provides high-speed, append-only logging. MSK Express is utilized for its 3x throughput improvement over standard brokers and its ability to scale up to 20x faster during peak bursts.  
* **Security:** The production cluster will implement IAM-based Authentication, TLS Encryption in transit, and granular Kafka ACLs for Authorization.  
* **Topics:** A single, multi-partitioned topic will be defined to ensure maximum parallelism.

### **Compute Layer: Amazon EMR (Apache Spark)**

A Spark cluster hosted on **Amazon EMR** serves as the processing engine.

* **Streaming Logic:** Spark Structured Streaming reads micro-batches from the Kafka topic.  
* **Efficiency:** Instead of row-by-row lookups, Spark performs **bulk checks** against a **Redis** cache to resolve Patient IDs, drastically reducing latency.  
* **Security:** EMR will be secured via Kerberos authentication, encryption at rest (S3/EBS), and Web UI security for monitoring.

### **Storage & Persistence Layer**

Data is fanned out to three distinct destinations simultaneously:

| Destination | Purpose | Technology |
| :---- | :---- | :---- |
| **Master Store** | PHI \+ Clinical Data (Full) | **PostgreSQL/MariaDB** (with Temporal Tables) |
| **Anonymized Store** | Public/Analyst Access | **Amazon DynamoDB** |
| **Lookup Cache** | Fast Identity Resolution | **Amazon ElastiCache (Redis)** |

*   
  **Temporal Tables:** To satisfy the snapshot requirement, the relational database will utilize **System-Period Data Versioning**. This maintains a history table automatically, enabling AS OF syntax for point-in-time queries.  
* **High Performance Querying:** DynamoDB provides the sub-second response times needed to meet the 2-second end-to-end availability target for anonymized data.

---

## **3\. Prototype Implementation (Local Docker)**

The prototype is a cost-effective ($0 budget) representation of the production system, utilizing Docker containers to simulate the cloud-based workflow.

### **Architecture Mapping**

| Prototype Component | Production Equivalent | Notes |
| :---- | :---- | :---- |
| **Kafka Container** | Amazon MSK Express | Single topic, no security, handles ingestion. |
| **Spark Container** | Amazon EMR | Standard PySpark image; no performance tuning. |
| **Postgres Container** | RDS (Postgres) \+ DynamoDB \+ Redis | Multi-purpose relational storage. |

### **Prototype Data Strategy**

In the Postgres container, two tables are created to simulate the multi-database production design:

1. **Index Table (The Redis Proxy):** A simple table with a composite index on (Name, DOB) storing the Unique\_ID. Spark checks this table to perform deduplication logic.  
2. **Anonymized Table (The DynamoDB Proxy):** Stores only the Unique\_ID and the Favorite\_Color field.

### **Testing & Validation**

The prototype's validity is verified via a **Python End-to-End Test Script**:

* The script generates and pushes 1,000,000 mock records to the Kafka container.  
* It utilizes the **Python-Faker** library to generate unique identities.  
* Following a 2-second sleep (simulating the latency requirement), it executes a count and data-consistency query against the Postgres anonymized table.  
* This test serves as the baseline proof-of-concept for the 5-second availability window required in the production specifications.