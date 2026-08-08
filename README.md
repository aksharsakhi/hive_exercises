# Big Data Analytics - MapReduce & Hive Assignment

**Course:** Big Data Analytics (23CSE352)  
**Institution:** Amrita Vishwa Vidyapeetham  
**Student Name:** Sheela Akshar Sakhi  
**Roll Number:** CB.SC.U4CSE23547  
**Class / Section:** CSE - F  

---

## 📌 Repository Overview

This repository contains the complete, perfectly executing source code for **Assignment 3 (Hive and MapReduce Exercises)**. It includes all Java MapReduce programs, Hive `.hql` scripts, datasets, execution shell scripts, and the final compiled assignment document. 

Everything in this repository has been designed to be **100% reproducible**. Anyone can easily clone this repo and run these assignments from scratch.

---

## 🚀 How to Execute This Assignment

If you want to run these programs on your own Hadoop/Hive cluster, follow these exact steps:

### Step 1: Clone the Repository
Open your terminal and clone this repository into your workspace:
```bash
git clone https://github.com/aksharsakhi/hive_exercises.git
cd hive_exercises
```

### Step 2: Start Your Hadoop Cluster
Make sure your Hadoop daemons (HDFS and YARN) are running:
```bash
start-all.sh
```

*(Note: Ensure you are running Java 8 as Apache Hive 3.x is not fully compatible with Java 11+)*

### Step 3: Run the MapReduce Programs (Java)
There are 5 MapReduce programs written for the Online Retail dataset. We have provided an automated bash script (`run.sh`) that instantly compiles the Java code, builds the `.jar`, uploads the input files to HDFS, and executes the job.

Run them one by one (1 through 5):
```bash
./run.sh 1
./run.sh 2
./run.sh 3
./run.sh 4
./run.sh 5
```
*The output will print directly to your terminal.*

### Step 4: Run the Hive Analytics Queries (.hql)
There are 5 massive Hive query scripts that solve advanced analytical problems (including joins, partitions, clustering, MAP data types, and Window Functions). 

To execute them, run:
```bash
cd src
hive -f Q1_RetailStore.hql
hive -f Q2_PageView.hql
hive -f Q3_Books.hql
hive -f Q4_MovieLens.hql
hive -f Q5_OnlineRetail.hql
```
*The Hive shell will automatically execute all sub-queries and print the formatted tables directly to your terminal.*

---

## 📁 Repository Structure

```text
hive_exercises/
├── src/                        # HQL and Java Source Code Files
│   ├── Q1_RetailStore.hql      # Retail Store Database (DDL, Clusters)
│   ├── Q2_PageView.hql         # PageView Database (MAP types)
│   ├── Q3_Books.hql            # Library Database (Joins, Partitions)
│   ├── Q4_MovieLens.hql        # MovieLens Analysis (Lateral Views)
│   ├── Q5_OnlineRetail.hql     # Comprehensive Retail Analytics
│   ├── CountryTransactions.java# MR 1
│   ├── CountrySales.java       # MR 2
│   ├── TopProducts.java        # MR 3
│   ├── CancelledTransactions.java # MR 4
│   └── CustomerOrders.java     # MR 5
├── inputs/                     # Local datasets automatically uploaded to HDFS
├── images/                     # Screenshots of execution output
├── docs/                       # Final Assignment Documentation
│   ├── Assignment3_HiveExercises_23547.md
│   └── Assignment3_HiveExercises_23547.docx
├── run.sh                      # Automated Bash script to run MapReduce programs
└── README.md                   # This instruction file
```

---

## 📄 Final Assignment Documentation

The completely finished assignment containing all the Logic Descriptions, Source Code, and Embedded Screenshots is located in the `docs/` folder:

- **[docs/Assignment3_HiveExercises_23547.docx](docs/Assignment3_HiveExercises_23547.docx)**

This `.docx` file was automatically compiled and rendered from the Markdown source using `pandoc`. It is formatted in standard Amrita format and is ready for direct submission.

---

## 🔗 Dataset References
- [UCI Machine Learning Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)
- [GroupLens - MovieLens Dataset](https://grouplens.org/datasets/movielens/)
