# AWS Serverless E-Commerce Orders ETL Pipeline

> A cloud-native, event-driven data engineering pipeline that automates ingestion, validation, transformation, security, and analytics for e-commerce order data — using fully managed AWS services.

![AWS](https://img.shields.io/badge/AWS-Serverless-orange?logo=amazonaws)
![Glue](https://img.shields.io/badge/AWS-Glue-blueviolet)
![Athena](https://img.shields.io/badge/AWS-Athena-yellow)
![Status](https://img.shields.io/badge/status-active-brightgreen)

---

## Table of Contents

- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Project Requirements](#project-requirements)
- [Architecture](#architecture)
- [AWS Services & Security Configuration](#aws-services--security-configuration)
- [Pipeline Walkthrough](#pipeline-walkthrough)
- [Datasets & Schema](#datasets--schema)
- [Analytics Query Outcomes](#analytics-query-outcomes)
- [Challenges & Solutions](#challenges--solutions)
- [Learning Outcomes](#learning-outcomes)

---

## Overview

This pipeline begins when an e-commerce orders dataset (CSV) is uploaded to an **Amazon S3 Landing Zone**. The file is validated using **AWS Lambda**, processed through multiple **AWS Glue** ETL jobs, secured through **PII masking**, cataloged using the **Glue Crawler** and **Data Catalog**, and finally made available for SQL-based analytics via **Amazon Athena**. Centralized monitoring, automated scheduling, and notification mechanisms ensure operational reliability throughout.

## Problem Statement

Traditional data pipelines often rely on brittle, expensive, schedule-based infrastructure (fixed cron jobs, always-on servers) that consume compute resources even when no new data has arrived. Processing raw, unvalidated CSV data also increases the risk of downstream failures from schema mismatches, duplicate rows, or missing values. On top of that, governing sensitive customer PII without sacrificing analytical usefulness is a constant challenge.

## Project Requirements

| Requirement | Description |
|---|---|
| **Serverless & event-driven** | Compute spins up only when new data lands, and scales back to zero on completion |
| **Layered data lake** | Data progresses through Landing → Raw → Processing → Secured → Curated zones |
| **Robust validation** | Row-level quality checks, schema conformance, duplicate removal, time-travel integrity |
| **State tracking** | AWS Glue Job Bookmarks maintain state across runs to avoid reprocessing old data |
| **Data optimization** | CSV → Apache Parquet (Snappy compression), partitioned by year/month |
| **Security** | Block Public Access, SSE-S3/SSE-KMS encryption at rest, S3 bucket versioning |
| **Regulatory compliance** | SHA-256 one-way hashing of PII (e.g. customer email) for GDPR/HIPAA alignment |

## Architecture

```
[ E-Commerce CSV ]
       │ (Upload)
       ▼
1. Ingestion Layer      ──► [ S3 Landing Zone ] ──► [ Amazon SQS ] ──► [ Lambda 1: Validation ]
                                                                              │
                                                                              ▼
2. Processing Layer     ──► [ S3 Raw Zone ] ◄── [ EventBridge (3 PM Daily) / Glue Workflow ]
                                  │ (Incremental tracking via Glue Job Bookmarks)
                                  ▼
                            [ Glue Job 1 ] ──► [ S3 Processing Zone ] ──► [ Lambda 2 ]
                                                                              │
                                                                              ▼
3. Transformation Layer ──►                                             [ Glue Job 2 ]
                                                                              │
                                                                              ▼
                            [ S3 Secured Zone ] ──────────────────────► [ Lambda 3 ]
                                                                              │
                                                                              ▼
4. Security Layer       ──►                                             [ Glue Job 3 ]
                                                                              │
                                                                              ▼
5. Analytics Layer      ──► [ S3 Curated Zone ] ◄── [ Glue Crawler & Data Catalog ]
                                  │                                          │
                                  ▼                                          ▼
                            [ Amazon Athena ] ◄───────────────────── [ CloudWatch Alarm ]
                             (SQL Queries)                          (Monitors Lambda 3 & Alerts)
```

**Layer breakdown:**

- **Ingestion** — Receives CSV uploads, buffers events via SQS, runs file-level structure checks in Lambda 1.
- **Processing** — Triggered by EventBridge at 3:00 PM daily. Glue Job 1 converts verified CSVs to Parquet, deduplicates, and checks schema, with Job Bookmarks tracking state.
- **Transformation** — Glue Job 2 appends business-logic columns.
- **Security** — Glue Job 3 applies cryptographic PII masking; Lambda 3 coordinates under CloudWatch Alarm monitoring.
- **Analytics** — Glue Crawler catalogs the curated partition for instant SQL access in Athena.
- **Monitoring & Notification** — Captures metrics, alarms on job/script failures, emails developers via SNS.

## AWS Services & Security Configuration

| Service | Role |
|---|---|
| **Amazon S3** | Storage backbone across five logical zones: `chinmayee-s3-landing-zone`, `chinmayee-s3-raw-zone`, `chinmayee-processed-zone`, `chinmayee-secured-zone`, `chinmayee-curated-zone` |
| **S3 Bucket Policies** | Enforce `aws:SecureTransport` (deny non-HTTPS traffic); restrict object access to designated IAM execution roles |
| **AWS IAM** | Governs access boundaries; Glue Crawler and Lambda functions provisioned with Administrator Access to avoid cross-resource blockages during schema evolution |
| **AWS Lambda** | Lightweight compute for validation, cross-stage triggering, and notification routing |
| **Amazon SQS** | Buffers S3 event metadata to decouple ingestion from execution |
| **AWS Glue Workflows & Jobs** | Orchestrates three PySpark ETL phases; Job Bookmarks enable delta-load processing |
| **AWS Glue Crawler & Data Catalog** | Crawls curated directories to maintain schemas and metadata definitions |
| **Amazon Athena** | Low-cost, interactive SQL queries on Parquet data lake objects |
| **Amazon EventBridge** | Time-based scheduling (3:00 PM workflow start) and pattern-matching status rules |
| **Amazon CloudWatch & Alarms** | Centralized observability; a dedicated alarm tracks Errors on Lambda 3 |
| **Amazon SNS** | Sends automated error alert emails on CloudWatch Alarm triggers |

## Pipeline Walkthrough

1. **Ingestion & buffering** — CSV lands in the S3 Landing Zone; the object-created event routes to an SQS queue.
2. **Basic file inspection** — Lambda 1 validates file extension/format from the SQS message; valid files are copied to the S3 Raw Zone.
3. **Core PySpark orchestration (Glue Job 1)** — EventBridge starts the Glue Workflow daily at 3:00 PM. With Job Bookmarks enabled, it filters out already-processed files, removes duplicates, checks schema, fills/filters missing values, and converts CSV → Parquet into the S3 Processing Zone.
4. **Business calculations (Glue Job 2)** — Triggered by Lambda 2 on Job 1's success. Adds two computed columns:
   - `fulfillment_urgency` — based on `datediff(ship_dt, order_dt)`: `CRITICAL DELAY` (>4 days), `DELAYED` (2–4 days), `ON-TIME` (≤2 days or pending)
   - `customer_segment` — based on `order_amount`: `VIP` (>$500), `Premium` ($200–$500), `Standard` (<$200)

   Output goes to the S3 Secured Zone.
5. **Security & cryptographic hashing (Glue Job 3)** — Lambda 3 (monitored by a CloudWatch Alarm) triggers Glue Job 3, which applies SHA-256 one-way masking to customer identifiers (e.g. email). Final output is written to the S3 Curated Zone in Snappy-compressed Parquet.
6. **Schema cataloging & analysis** — An EventBridge-scheduled Glue Crawler scans the Curated Zone and updates the Glue Data Catalog, making the data immediately queryable via Athena.

## Datasets & Schema

### Source data (CSV)

**File:** `ecommerce_orders_500.csv` — raw e-commerce transaction batch landed by front-end microservices.

```csv
order_id,customer_id,customer_email,customer_region,order_date,shipping_date,delivery_date,product_id,product_category,quantity,order_amount,order_status
ORD0001,CUST202,cust202@email.com,East,11-07-2026 07:09,,,PROD_MAT02,Fitness,4,370.29,Cancelled
ORD0002,CUST192,cust192@email.com,East,11-07-2026 14:11,12-07-2026 13:11,15-07-2026 03:11,PROD_MAT02,Fitness,3,388.11,Delivered
ORD0003,,cust114@email.com,West,10-07-2026 18:53,11-07-2026 23:53,14-07-2026 02:53,PROD_BFC02,Home & Kitchen,2,103.33,Delivered
ORD0492,CUST244,cust244@email.com,South,10-07-2026 11:27,11-07-2026 10:27,14-07-2026 01:27,PROD_BLN07,Home & Kitchen,2,1034.84,Delivered
```

### Curated output (Parquet)

**File:** `part-00000-6c1ec513-3e96-46b5-af5a-48728d74faae.c000.snappy.parquet` — final columnar dataset in the S3 Curated Zone, with PII masked and enrichment columns appended.

```json
{
  "type": "struct",
  "fields": [
    { "name": "order_id", "type": "string", "nullable": true },
    { "name": "customer_id", "type": "string", "nullable": true },
    { "name": "customer_email", "type": "string", "nullable": true },
    { "name": "customer_region", "type": "string", "nullable": true },
    { "name": "order_date", "type": "string", "nullable": true },
    { "name": "shipping_date", "type": "string", "nullable": true },
    { "name": "delivery_date", "type": "string", "nullable": true },
    { "name": "product_id", "type": "string", "nullable": true },
    { "name": "product_category", "type": "string", "nullable": true },
    { "name": "quantity", "type": "integer", "nullable": true },
    { "name": "order_amount", "type": "double", "nullable": true },
    { "name": "order_status", "type": "string", "nullable": true },
    { "name": "fulfillment_urgency", "type": "string", "nullable": true },
    { "name": "customer_segment", "type": "string", "nullable": true }
  ]
}
```

**Masking example:** raw email `cust202@email.com` → SHA-256 hash `75391a26998d84a700b0c23226e30677db3db4cec3617e82a024542bd9c35d7a`

**Enrichment example:** `fulfillment_urgency = ON-TIME`, `customer_segment = Premium`

## Analytics Query Outcomes

Validated end-to-end via Amazon Athena against the curated `chinmayee-db.chinmayee_curated_zone` table, analyzing regional logistical performance.

```sql
SELECT
    customer_region,
    fulfillment_urgency,
    COUNT(order_id) AS order_count,
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER(PARTITION BY customer_region), 2) AS regional_percentage
FROM "chinmayee-db"."chinmayee_curated_zone"
GROUP BY customer_region, fulfillment_urgency
ORDER BY customer_region, order_count DESC;
```

**Result** (`4d252ffe-60ce-43f3-b1e0-9693bead53a9.csv`):

| customer_region | fulfillment_urgency | order_count | regional_percentage |
|---|---|---|---|
| East | ON-TIME | 109 | 100.0% |
| North | ON-TIME | 84 | 100.0% |
| South | ON-TIME | 115 | 100.0% |
| West | ON-TIME | 109 | 100.0% |
| usa | ON-TIME | 18 | 100.0% |

## Challenges & Solutions

**1. Resource name mismatch on Lambda 3 init**
A spelling/regional deployment mismatch prevented Lambda 3 from resolving the Glue Job 3 resource identifier. Fixed by reconciling resource name strings across all deployment configs.

**2. Hardcoded filename causing `AnalysisException`**
The ETL script had a hardcoded input filename (`ecommerce_orders_10.csv`). New batches with different names caused `pyspark.sql.utils.AnalysisException: Path does not exist`. Fixed by pointing the PySpark reader at the base folder (`s3://chinmayee-s3-raw-zone/raw/`) instead of a specific file, so it dynamically picks up any new CSV.

**3. PII masking without breaking analytics**
Needed to secure customer emails without breaking groupings, schemas, or inflating cost. Solved with a one-way SHA-256 hashing function inside the final Glue transformation — unique signatures are preserved for downstream metrics while plaintext PII stays hidden.

## Learning Outcomes

- **Serverless scaling mechanics** — building reactive, event-driven flows with SQS, EventBridge, and Lambda instead of fixed cron architectures.
- **Stateful processing** — using Glue Job Bookmarks for incremental loads, cutting reprocessing overhead.
- **Cloud governance & IAM** — managing access boundaries for Glue Crawlers and Lambda across shifting schema/metadata needs.
- **Big data performance** — converting CSV to Parquet + Snappy compression to shrink storage footprint and speed up Athena scans.
- **Targeted alerting** — designing CloudWatch alarms on critical handoff points (Lambda 3) with real-time SNS notifications.
