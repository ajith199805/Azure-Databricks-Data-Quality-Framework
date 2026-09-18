# Databricks notebook source
from pyspark.sql.functions import expr, rand, when, col, lit, current_timestamp
import random

print("Generating 100,000+ row enterprise retail dataset completely in-memory...")

# 1. Create a base sequence dataframe of 100,000 rows
base_df = spark.range(1, 100001)

# 2. Programmatically generate realistic retail columns with intentional anomalies
df_real = base_df.withColumn("InvoiceNo", (100000 + col("id")).cast("string")) \
                 .withColumn("StockCode", when(rand() > 0.95, lit(None)).otherwise((20000 + (rand() * 1000).cast("int")).cast("string"))) \
                 .withColumn("Description", lit("Enterprise Retail Item Product")) \
                 .withColumn("Quantity", when(rand() > 0.92, (rand() * -100).cast("int")).otherwise((rand() * 50 + 1).cast("int"))) \
                 .withColumn("InvoiceDate", expr("date_add(current_date(), -cast(rand() * 365 as int))")) \
                 .withColumn("UnitPrice", (rand() * 100 + 0.5).cast("decimal(10,2)")) \
                 .withColumn("CustomerID", when(rand() > 0.88, lit(None)).otherwise((12000 + (rand() * 5000).cast("int")).cast("string"))) \
                 .withColumn("Country", lit("United Kingdom"))

# 3. Save it as your raw staging table (managed natively by serverless catalogs)
df_real.write.format("delta").mode("overwrite").saveAsTable("raw_online_retail")

print("----------------------------------------------------------------")
print("✅ Success! 100,000 Rows generated and loaded completely inside your cloud account!")
print(f"Total production records registered in 'raw_online_retail' table: {spark.table('raw_online_retail').count():,}")
print("----------------------------------------------------------------")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

print("Initializing Metadata-Driven Quality Assurance Processing...")

# 1. READ FROM RAW BRONZE LAYER
bronze_data = spark.table("raw_online_retail")

# 2. DEFINE THE METADATA CONTROL PARAMETERS (Bridges your data validation background)
# We choose required columns and map specific business criteria programmatically
required_columns = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID"]

selected_df = bronze_data.select(*required_columns)

# 3. ENTERPRISE DATA QUALITY CRITERIA RULES
# Rule A: CustomerID cannot be null/blank
# Rule B: StockCode cannot be null/blank
# Rule C: Quantity must be positive (> 0) to avoid transactional anomalies
valid_customer  = selected_df["CustomerID"].isNotNull()
valid_stockcode = selected_df["StockCode"].isNotNull()
valid_quantity  = selected_df["Quantity"] > 0

# Apply the conditions to filter out perfect vs corrupted records
clean_records_condition = (valid_customer & valid_stockcode & valid_quantity)

# 4. SPLIT DATA SYSTEMATICALLY INTO CLEAN VS QUARANTINED DATA
# Clean records get passed to Silver Layer
silver_clean_df = selected_df.filter(clean_records_condition) \
                             .withColumn("dq_processed_at", current_timestamp())

# Corrupted records get routed to an exception isolation layer for reporting
quarantine_bad_df = selected_df.filter(~clean_records_condition) \
                               .withColumn("dq_failed_at", current_timestamp())

# 5. WRITE AND LOCK RESULTS INTO SEPARATE MANAGED DELTA TABLES
silver_clean_df.write.format("delta").mode("overwrite").saveAsTable("silver_clean_retail")
quarantine_bad_df.write.format("delta").mode("overwrite").saveAsTable("quarantine_bad_retail")

# Calculate metrics for the run report
total_rows = bronze_data.count()
clean_rows = silver_clean_df.count()
bad_rows = quarantine_bad_df.count()

print("----------------------------------------------------------------")
print("✅ Data Quality Execution Framework Complete!")
print(f"Total Rows Processed: {total_rows:,}")
print(f"✔️ Clean Records Transferred to Silver Layer: {clean_rows:,} ({(clean_rows/total_rows)*100:.2f}%)")
print(f"❌ Anomalous Records Quarantined for Audit: {bad_rows:,} ({(bad_rows/total_rows)*100:.2f}%)")
print("----------------------------------------------------------------")


# COMMAND ----------

# Cell 3: Data Quality Audit Reporting using Spark SQL
print("Fetching sample audit logs from the Quarantine Isolation Layer...")

# Display a statistical preview of why records failed (e.g., Negative quantities or Null values)
quarantine_preview_df = spark.sql("""
    SELECT 
        'Negative Quantity Anomaly' AS Failure_Reason,
        COUNT(*) AS Total_Affected_Rows,
        MIN(Quantity) AS Worst_Value_Detected
    FROM quarantine_bad_retail 
    WHERE Quantity <= 0
    
    UNION ALL
    
    SELECT 
        'Missing / Null Customer Identifier' AS Failure_Reason,
        COUNT(*) AS Total_Affected_Rows,
        0 AS Worst_Value_Detected
    FROM quarantine_bad_retail 
    WHERE CustomerID IS NULL
""")

display(quarantine_preview_df)


# COMMAND ----------

