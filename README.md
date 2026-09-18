# Azure-Databricks-Data-Quality-Framework
Metadata-Driven Data Quality Framework built using PySpark and Delta Lake inside Databricks to programmatically audit and isolate enterprise transactional anomalies.
# Metadata-Driven Cloud Data Quality Framework

## 📌 Project Overview
An enterprise-grade, metadata-driven data pipeline built natively within **Azure Databricks Serverless** using **PySpark** and **Delta Lake**. The framework eliminates hardcoded transformation logic by programmatically evaluating incoming records against an operational control schema, isolating anomalies into an audit-ready quarantine layer while driving clean data to downstream business intelligence engines.

## 🚀 Key Architectural Accomplishments
* **Scalable Data Profiling:** Engineered a self-contained in-memory generation schema to test pipeline volume limits against **100,000 production records**.
* **Automated Governance Checks:** Programmed data profiling parameters focusing on key constraint domains (Null tracking, string enforcement, range limits) executed in under **17 seconds**.
* **Exception Isolation (Quarantine Management):** Systematically filtered and quarantined **23,24% of bad data records** (12,201 missing primary customer keys and 8,056 negative transactional value anomalies) without stalling main execution workflows.

## 🛠️ Technology Stack
* **Compute Engine:** PySpark (Apache Spark Distributed Compute)
* **Storage Layer:** Delta Lake (ACID compliant storage pools)
* **Execution Environment:** Databricks Serverless Catalog Workspace
