# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Read Stocks delta table using absolute OneLake ABFSS path and preview schema/sample
from pyspark.sql.utils import AnalysisException

# Absolute ABFSS base for the provided Lakehouse
abfss_base = "abfss://834c6a2d-e10f-42e2-8ac0-6605a5d3140f@onelake.dfs.fabric.microsoft.com/e3888087-2a93-42b2-8948-e5a053719d46"
stocks_path = f"{abfss_base}/Tables/Stocks"
print("Reading from:", stocks_path)

try:
    stocks_df = spark.read.format("delta").load(stocks_path)
    print("Loaded Stocks delta table successfully.")
    print("Row count (approx):", stocks_df.limit(1_000_000).count())  # bounded count sample to avoid scanning entire table
    print("Total columns:", len(stocks_df.columns))
    print("Columns:", stocks_df.columns)
    stocks_df.printSchema()
    display(stocks_df.limit(20))
except AnalysisException as e:
    print("Failed to read as delta; error:", str(e)[:1000])
    raise
except Exception as e:
    print("Unexpected error:", type(e).__name__, str(e)[:1000])
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify total rows, distinct tickers, and date range
from pyspark.sql import functions as F

row_cnt = stocks_df.count()
print(f"Total rows: {row_cnt}")

if row_cnt == 0:
    print("Table appears to be empty. Cannot proceed with analysis.")
else:
    tickers_df = stocks_df.select("Ticker").distinct().orderBy("Ticker")
    print("Distinct tickers:")
    display(tickers_df)

    range_df = stocks_df.select(F.min("Date").alias("min_date"), F.max("Date").alias("max_date"))
    display(range_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if row_cnt != 0:
    raise RuntimeError("Forced Failure")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
