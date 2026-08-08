# Big Data Analytics - Hive Exercises (Assignment 3)

**Course:** Big Data Analytics (23CSE352)  
**Institution:** Amrita Vishwa Vidyapeetham  
**Student Name:** Sheela Akshar Sakhi  
**Roll Number:** CB.SC.U4CSE23547  
**Class / Section:** CSE - F  

---

## 📌 Repository Overview

This repository contains Hive Queries and Java MapReduce implementations for **Assignment 3 (Hive Exercises)** executed on Apache Hadoop/Hive. It includes complete source codes, dummy datasets, execution helper scripts, and documentation placeholders for formal submission.

---

## 📁 Repository Structure

```text
hive_exercises/
├── src/                        # HQL and Java Source Code Files
│   ├── Q1_RetailStore.hql
│   ├── Q2_PageView.hql
│   ├── Q3_Books.hql
│   ├── Q4_MovieLens.hql
│   ├── Q5_OnlineRetail.hql
│   ├── CountryTransactions.java
│   ├── CountrySales.java
│   ├── TopProducts.java
│   ├── CancelledTransactions.java
│   └── CustomerOrders.java
├── inputs/                     # Datasets
│   ├── retail_data.csv         # Generated dummy data for Q1
│   ├── pageview_data.txt       # Generated dummy data for Q2
│   ├── books.txt               # Generated dummy data for Q3
│   ├── purchases.txt           # Generated dummy data for Q3
│   └── download_datasets.sh    # Script to download Q4/Q5 datasets
├── docs/                       # Final Assignment Documentation
│   └── Assignment3_HiveQueries.md # Markdown file to export to Word/PDF
├── images/                     # Screenshots (To be filled by you)
├── run.sh                      # Automated Bash script to run MapReduce programs
└── README.md                   # Project documentation
```

---

## 📋 List of Questions

| Question | Topic | File Name | Key Concept |
|---|---|---|---|
| Q1 | Retail Store Database | `Q1_RetailStore.hql` | Create db, load, partition, cluster, group by |
| Q2 | PageView Database | `Q2_PageView.hql` | Map data types, partitioned tables |
| Q3 | Books Database | `Q3_Books.hql` | Join tables, sorting, complex partitioning |
| Q4 | MovieLens Analysis | `Q4_MovieLens.hql` | Aggregations, analytic functions, Lateral View explode |
| Q5 MR | Online Retail MapReduce | `.java` files | MR programs for transaction counts, top products, etc |
| Q5 HQL | Online Retail Hive | `Q5_OnlineRetail.hql` | Comprehensive analytics, grouping, partitioning, joining |

---

## ⚙️ Prerequisites & Setup

- **Operating System:** Ubuntu Linux / macOS
- **Apache Hadoop:** v3.x 
- **Apache Hive:** Installed and configured with metastore
- **Java Development Kit (JDK):** Java 8 / 11 / 17

---

## 🚀 Quick Execution Guide

### 1. Download Q4 & Q5 Datasets
```bash
cd inputs
./download_datasets.sh
cd ..
```

### 2. Run MapReduce Programs (For Q5 parts 1-5)
To execute **Program 1** (CountryTransactions):
```bash
./run.sh 1
```
(Replace 1 with 2, 3, 4, or 5 to run the other MR programs).

### 3. Run Hive Queries
Open a Hive CLI or Beeline shell and execute the `.hql` files using the `source` command, or from terminal:
```bash
hive -f src/Q1_RetailStore.hql
```

---

## 📄 Submission Documentation
The document ready to be exported to Microsoft Word or PDF is available under the `docs/` folder:
- **[docs/Assignment3_HiveQueries.md](docs/Assignment3_HiveQueries.md)** (Contains all questions, queries, and placeholders for your terminal screenshots). 
**Important**: You must run the queries on your Hive shell, take screenshots of the outputs, and paste them into this document before submission.
