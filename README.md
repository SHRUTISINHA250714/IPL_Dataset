# Building an End-to-End IPL Analytics Pipeline Using Databricks Lakehouse Architecture

## Introduction

Modern data engineering systems require scalable pipelines capable of ingesting raw data, transforming it into structured datasets, and delivering insights through analytics dashboards. **Databricks** provides a unified platform that integrates **data engineering, analytics, and visualization**.

This project implements a **complete end-to-end analytics pipeline** using Databricks to analyze **IPL (Indian Premier League) cricket data**.

The pipeline processes raw match datasets, transforms them through multiple layers using **Apache Spark**, and produces **analytics-ready datasets** that power an interactive dashboard.

### Objectives of the Project

- Implement **Databricks Lakehouse architecture**
- Design a **Medallion data pipeline**
- Perform **PySpark-based data transformations**
- Store data using **Delta Lake**
- Automate workflows using **Databricks Jobs**
- Build **interactive analytics dashboards**

---

# Databricks Lakehouse and Medallion Architecture
<img width="800" height="421" alt="image" src="https://gist.github.com/user-attachments/assets/556aa0a6-6bd9-4e81-9eca-add3d6c34326" />

The pipeline follows the **Medallion Architecture pattern**, widely used in Databricks environments.

This architecture organizes data into three layers:

1. **Bronze Layer**
2. **Silver Layer**
3. **Gold Layer**

Each layer progressively improves the **quality and usability of the data**.

---

# Bronze Layer

The **Bronze layer** stores raw data exactly as it arrives from the source systems.

### Characteristics

- Raw format storage
- Minimal transformation
- Maintains source schema
- Preserves data lineage

### Tables Created
bronze.matches
bronze.deliveries

These tables represent the **ingested raw IPL dataset**.

---

# Silver Layer

The **Silver layer** contains cleaned and structured data derived from the Bronze layer.

### Typical Transformations

- Schema normalization
- Data cleaning
- Standardization
- Duplicate removal
- Timestamp enrichment

### Tables Created
silver.matches
silver.deliveries

These tables provide **reliable datasets for downstream analytics**.

---

# Gold Layer

The **Gold layer** contains business-level analytical datasets optimized for reporting and dashboards.

These tables are generated through **aggregations and transformations on the Silver layer**.

### Gold Tables Created
gold.match_results
gold.match_scoreboard
gold.batter_statistics
gold.bowler_statistics
gold.points_table

The Gold layer serves as the **primary source for analytics dashboards**.

---

# Dataset Description

The project uses publicly available **IPL datasets** consisting of two primary files.

## matches.csv

This dataset contains **match-level metadata**.

### Important Fields

- match_id
- season
- city
- venue
- team1
- team2
- toss_winner
- winner
- player_of_match

This dataset provides **high-level information about each IPL match**.

---

## deliveries.csv

This dataset contains **ball-by-ball match data**.

### Key Columns

- match_id
- inning
- batting_team
- bowling_team
- over
- ball
- batter
- bowler
- batsman_runs
- extra_runs
- total_runs
- is_wicket

This dataset enables **detailed player and match analytics**.

---

# Data Pipeline Overview

The pipeline transforms raw IPL data into analytics datasets using **PySpark transformations**.

### Pipeline Workflow

<img width="1923" height="1023" alt="image" src="https://gist.github.com/user-attachments/assets/10976c2d-00ff-44be-991e-ae51670cf4fe" />


Each stage generates **intermediate datasets used in downstream processing**.

---

# Raw to Bronze Layer

The first stage ingests raw IPL datasets into Databricks.

### Steps Performed

- Read CSV datasets using Spark
- Preserve the original schema
- Store the data as **Delta tables**

### Tables Created
bronze.matches
bronze.deliveries

This layer ensures that the **original data remains available for auditing and reprocessing**.

---

# Bronze to Silver Layer

The second stage performs **data cleaning and schema standardization**.

### Operations Performed

- Converting column data types
- Removing duplicate records
- Adding ingestion timestamps
- Standardizing column names

### Tables Created
silver.matches
silver.deliveries

The Silver layer serves as the **trusted dataset for analytics computations**.

---

# Gold Layer Transformations

The Gold layer contains **business-level analytics tables** created using aggregations on Silver datasets.

These tables power the **analytics dashboard**.

---

## Match Results Table

The **match_results** table contains match-level metadata.

### Key Columns

- match_id
- season
- venue
- city
- team1
- team2
- winner
- player_of_match

**Purpose:**  
Provides **match summaries and contextual information** for analysis.

---

## Match Scoreboard Table

The **match_scoreboard** table aggregates ball-level data into innings-level statistics.

### Metrics Calculated

- total runs
- wickets
- balls faced
- overs
- run rate

This table enables **match performance analysis**.

---

## Batter Statistics Table

The **batter_statistics** table provides batting performance metrics.

### Metrics Included

- total runs
- number of fours
- number of sixes
- runs per season

This table enables analysis of **top run scorers and batting consistency**.

---

## Bowler Statistics Table

The **bowler_statistics** table captures bowling performance metrics.

### Metrics Included

- wickets taken
- runs conceded
- balls bowled
- economy rate

This table helps identify **top-performing bowlers**.

---

## Points Table

The **points_table** generates the IPL leaderboard for each season.

### Metrics Calculated

- matches played
- wins
- losses
- no-result matches
- points
- net run rate
- ranking

This table powers the **team standings dashboard**.

---

# Databricks Job Orchestration

The pipeline is automated using **Databricks Jobs**.

Each stage of the transformation process is configured as a **separate task** within the job pipeline.

### Execution Order
raw_to_bronze
→ bronze_to_silver
→ batter_statistics
→ bowler_statistics
→ match_results
→ match_scoreboard
→ points_table

### Benefits

- Automated pipeline execution
- Dependency management
- Monitoring job runs
- Failure detection and recovery

---

# Analytics Dashboard

The final stage of the project is an **interactive analytics dashboard** built using **Databricks Dashboards**.

### Dashboard Visualizations


<img width="1488" height="2048" alt="image" src="https://gist.github.com/user-attachments/assets/76a6bc9a-6b2d-4450-8363-17bbc26167e8" />

- **Team Standings**  
  Displays IPL leaderboard including rank, wins, losses, points, and net run rate.

- **Top Batters**  
  Displays players with the highest run totals.

- **Top Bowlers**  
  Displays bowlers with the most wickets.

- **Seasonal Team Performance**  
  Shows team points distribution across IPL seasons.

- **Run Rate Trend**  
  Tracks average run rate evolution across seasons.

- **All-Rounder Performance**  
  Compares players based on runs scored and wickets taken.

- **Highest Team Scores**  
  Displays the highest team totals per season.

---

# Key Insights from the Analysis

Several insights can be derived from the dataset:

- Run rates have increased significantly in recent IPL seasons, reflecting **more aggressive batting strategies**.
- Certain players dominate **batting metrics across multiple seasons**.
- Some bowlers consistently rank among the **highest wicket-takers**.
- Team performance varies significantly across seasons, with certain teams demonstrating **long-term dominance**.

---

# Technologies Used

- **Databricks**
- **Apache Spark**
- **PySpark**
- **Delta Lake**
- **SQL**
- **Databricks Dashboards**

---

# Data Engineering Concepts Demonstrated

This project demonstrates several important **data engineering practices**:

- Medallion Architecture
- Lakehouse data modeling
- Batch ETL pipelines
- Spark-based distributed transformations
- Analytical data modeling
- Pipeline orchestration
- Data visualization

---

# Future Improvements

Several enhancements can further extend this project:

- Real-time **streaming pipelines** for live match data ingestion
- **Machine learning models** for predicting match outcomes
- **Player performance forecasting** using historical statistics
- Integration with **Databricks Genie for natural language analytics**

---

# Conclusion

This project demonstrates how **Databricks can be used to build a scalable analytics pipeline using the Lakehouse architecture**.

By organizing the pipeline into **Bronze, Silver, and Gold layers**, raw IPL data is transformed into structured analytical datasets that power meaningful insights through dashboards.

The project highlights how modern data engineering platforms integrate **data ingestion, transformation, orchestration, and visualization within a single environment**.
