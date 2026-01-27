# **Prototype For A High-Throughput Clinical Data Ingest**

This prototype demonstrates a high-throughput data pipeline designed to handle 1,000,000 records per minute. It performs deduplication, identity resolution, and PII anonymization using a distributed architecture.

## **📚 Documentation & Design**

Detailed documentation regarding the production-ready cloud architecture is available in the following files:

* **Documentation.md**: Contains the full system requirements and a detailed mapping between these prototype containers and the final AWS cloud components (MSK, EMR, DynamoDB, etc.).  
* **kontaktDiagram.drawio.svg**: A high-level architectural diagram of the cloud-based solution.  
* **AI Provenance**: The prompts used to generate this documentation and architectural strategy can be found in CopilotPrompts.txt, GeminiPrompts.txt, and within the source code comments.

---

## **🛠 Prerequisites**

### **1\. Docker**

This prototype runs in a containerized environment. Ensure you have Docker and the Docker Compose plugin installed.

**Debian/Ubuntu Linux:**

Bash  
sudo apt update  
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin postgresql-client libpq-dev python3-dev

*For Windows or macOS, please install **Docker Desktop**.*

### **2\. Python Environment**

The end-to-end (e2e) test script requires **Python 3.13.11**. We recommend using pyenv to manage your virtual environment:

Bash  
\# Install the specific Python version  
pyenv install 3.13.11

\# Create and activate a virtual environment  
pyenv virtualenv 3.13.11 kontaktenv  
pyenv activate kontaktenv

\# Install dependencies  
pip install \-r requirements.txt

---

## **🚀 Running the Prototype**

### **1\. Start the Infrastructure**

Pull the code via Git, navigate to the project directory, and launch the containers (Kafka, Spark, and Postgres):

Bash  
docker compose up

### **2\. Submit the Spark Job**

Currently, the Spark processing engine requires manual submission to the cluster. In a new terminal window, run:

Bash  
docker exec -it kontakttakehome-spark-master-1 /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 
  --executor-memory 1G \
  --driver-memory 512M \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2 \
  ./app/app.py

NOTE: The executor-memory and driver-memory parameters may not be necessary on your machine so if you want to test larger throughput then you would set those much higher

### **3\. Run the End-to-End Test**

Once the Spark job is running and the infrastructure is healthy, execute the test script to simulate the ingestion of 1M records and verify database persistence:

Bash  
python e2e.py


The test checks the database, if you wish to inspect the database yourself connect to database kontakt_database port 5432 with user kontakt and the password k0ntakt 

---

## **🏗 Component Mapping (Summary)**

| Prototype Component | Production Component (AWS) |
| :---- | :---- |
| Kafka (KRaft Mode) | Amazon MSK Express |
| Spark Standalone | Amazon EMR (Serverless/EC2) |
| Postgres (Main Table) | Amazon Aurora (Temporal Tables) |
| Postgres (Anonymized) | Amazon DynamoDB |
| Postgres (Index) | Amazon ElastiCache (Redis) |

