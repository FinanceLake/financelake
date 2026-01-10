
## Objective

To showcase how Apache Kafka can be used to ingest real-time financial data (CSV-based) and stream it into FinanceLake.



##  Architecture

![Kafka Architecture](../resources/img/kafka-archi.png)

- Kafka is **scalable**, **reactive**, and **event-driven**.
- Kafka integrates easily with tools like **Apache Spark**, **NiFi**, or **HDFS**.
- It is more modern and actively maintained compared to **Sqoop**.


## Technologies Used

- Apache Kafka 4.0.0
- Python 3.12.3
- `kafka-python` library
-`python-dotenv` library

##  Sample Dataset


The sample CSV dataset (`creditcard.csv`) is excluded from version control for simplicity.

To test the pipeline, you can download the dataset from Kaggle:

📥 [Download on Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)

Or via command line (requires [Kaggle CLI](https://github.com/Kaggle/kaggle-api)):


`kaggle datasets download -d mlg-ulb/creditcardfraud`
`unzip creditcardfraud.zip -d data/`


## Environment Variables

Create a .env file in the project root based on .env.example using this commande:

`cp .env.example .env`


##  How to Run

1- Create and activate a virtual environment:

Before installing dependencies, it's recommended to create a Python virtual environment to keep your project isolated:

`python3 -m venv .venv`
`source .venv/bin/activate`

All reuired  Python packages (like kafka-python and python-dotenv) will be installed inside this virtual environment.


2- Install Requirements

Make sure Kafka and Python are installed. Install the `kafka-python` and `python-dotenv` library:

`pip install kafka-python  python-dotenv`


3- Format the KRaft storage (only once)

`bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties`


4- start kafka-server

-terminale  1 

`bin/kafka-server-start.sh config/kraft/server.properties`

5- Create the Kafka topic


`bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`


6-Run your Python Kafka producer:
-terminale  2
`python3 producer.py`



7- Run your python Kafka Consumer:
-terminale  3
`python3 consumer.py`

8- start hdfs services

`start-dfs.sh`

9- create directory 

hdfs dfs -mkdir -p /user/your-username/finance-data
hdfs dfs -put /tmp/transactions.log /user/your-username/finance-data/


Notes

    Ensure Hadoop is correctly configured and start-dfs.sh runs without issues.

    Replace your-username with your actual HDFS username.