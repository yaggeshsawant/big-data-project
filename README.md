# 🛒 FMCG Global Sales Data Pipeline

> **An automated, end-to-end multi-source data pipeline processing 1.1 Million+ sales transactions across 7 countries with zero manual intervention.**

---

## 📌 Project Overview

The **FMCG Global Sales Data Pipeline** ingests, cleans, transforms, and stores high-volume sales transaction data from distributed global sources into cloud platforms ready for analytics. 

By unifying data streams across multiple cloud providers and on-premise infrastructure, this project provides automated real-time and batch-processing capabilities to generate actionable business intelligence for fast-moving consumer goods.

---

## 🚀 Key Features

* **⚡ Real-Time & Batch Streaming:** Seamless ingestion of 1.1M+ sales records across 7 international markets.
* **🌐 Multi-Source Integration:** Combines raw store, product, and sales data from **AWS**, **Snowflake**, and **Google Drive**.
* **📊 Large-Scale Distributed Processing:** PySpark transformations running over HDFS Parquet storage for high performance.
* **🤖 Fully Automated Orchestration:** End-to-end pipeline management and monitoring using **Apache Airflow**.
* **📈 Interactive Dashboards:** Final analytical datasets surfaced via **Tableau** for business consumption.

---

## 🛠️ Tech Stack & Architecture

| Layer | Technology | Usage / Function |
| :--- | :--- | :--- |
| **Ingestion** | ![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white) | Real-time & batch streaming of transaction data |
| **Storage** | ![HDFS](https://img.shields.io/badge/HDFS-FFE01B?style=for-the-badge&logo=apache-hadoop&logoColor=black) | Distributed storage of streamed data in Parquet format |
| **Sources** | ![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white) ![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white) ![Google Drive](https://img.shields.io/badge/Google_Drive-4285F4?style=for-the-badge&logo=googledrive&logoColor=white) | Multi-source data input feeds |
| **Data Warehouse** | ![Hive](https://img.shields.io/badge/Apache_Hive-FDEE21?style=for-the-badge&logo=apache-hive&logoColor=black) | Structured cataloging for raw and processed datasets |
| **Processing** | ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white) | Large-scale data cleaning & ETL transformations |
| **Orchestration**| ![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white) | Pipeline workflow scheduling and DAG management |
| **Cloud Storage** | ![S3](https://img.shields.io/badge/AWS_S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white) | Final staging storage for analytics consumption |
| **Visualization**| ![Tableau](https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white) | Interactive executive dashboards |

---

## ⚙️ Data Pipeline Workflow

```text
[ AWS / Snowflake / GDrive ] ──┐
                               ├──► [ Apache Kafka ] ──► [ HDFS (Parquet) ]
[ Sales Transactions (1.1M) ] ──┘                                 │
                                                                 ▼
[ Tableau Analytics ] ◄── [ AWS S3 Cloud ] ◄── [ PySpark ETL ] ◄─┘
         ▲                                            │
         └────────────────── [ Apache Airflow DAGs ] ─┘
