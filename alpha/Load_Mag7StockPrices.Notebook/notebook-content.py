# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e10ec919-f879-4c67-b48c-10dd1cb610a6",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "834c6a2d-e10f-42e2-8ac0-6605a5d3140f",
# META       "known_lakehouses": [
# META         {
# META           "id": "e10ec919-f879-4c67-b48c-10dd1cb610a6"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3911f3b1-a7cd-461d-8da8-84b1ead9fe4e",
# META       "known_warehouses": [
# META         {
# META           "id": "3911f3b1-a7cd-461d-8da8-84b1ead9fe4e",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType, LongType
)

TICKERS = ["MSFT", "AAPL", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]
PERIOD_DAYS = 365

schema = StructType([
    StructField("Symbol", StringType(), False),
    StructField("Date",   DateType(),   False),
    StructField("Open",   DoubleType(), True),
    StructField("High",   DoubleType(), True),
    StructField("Low",    DoubleType(), True),
    StructField("Close",  DoubleType(), True),
    StructField("Volume", LongType(),   True),
])

def fetch_yahoo(symbol: str, days: int = 365) -> list:
    now = datetime.now(timezone.utc)
    end_ts = int(now.timestamp())
    start_ts = int((now - timedelta(days=days)).timestamp())

    url = (
        f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?period1={start_ts}&period2={end_ts}&interval=1d"
    )
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()

    chart = resp.json()["chart"]["result"][0]
    timestamps = chart["timestamp"]
    q = chart["indicators"]["quote"][0]

    rows = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        rows.append(Row(
            Symbol=symbol,
            Date=dt,
            Open=float(q["open"][i])   if q["open"][i]   is not None else None,
            High=float(q["high"][i])   if q["high"][i]   is not None else None,
            Low=float(q["low"][i])     if q["low"][i]    is not None else None,
            Close=float(q["close"][i]) if q["close"][i]  is not None else None,
            Volume=int(q["volume"][i]) if q["volume"][i] is not None else None,
        ))
    return rows

all_rows = []
for ticker in TICKERS:
    print(f"Fetching {ticker} …")
    ticker_rows = fetch_yahoo(ticker, PERIOD_DAYS)
    print(f"  → {len(ticker_rows)} rows")
    all_rows.extend(ticker_rows)

df = spark.createDataFrame(all_rows, schema=schema) \
            .orderBy("Symbol", "Date") \
            .cache()

print(f"DataFrame count: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "dbo.mag7_stock_prices"

(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"Wrote table: {table_name}")

# Read back and validate
loaded = spark.table(table_name)
row_count = loaded.count()
print(f"Table row count: {row_count}")

display(loaded.orderBy("Symbol", "Date").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
