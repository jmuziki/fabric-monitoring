# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4199e386-5245-42d4-a8d2-70bc1013dcca",
# META       "default_lakehouse_name": "Gold",
# META       "default_lakehouse_workspace_id": "834c6a2d-e10f-42e2-8ac0-6605a5d3140f",
# META       "known_lakehouses": [
# META         {
# META           "id": "4199e386-5245-42d4-a8d2-70bc1013dcca"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException


src_tbl = "dbo.mag7_stock_prices"
df_src = spark.read.table(src_tbl)

required_cols = ["Symbol","Date","Open","High","Low","Close","Volume"]
missing = [c for c in required_cols if c not in df_src.columns]
if missing:
    raise ValueError(f"Source table {src_tbl} is missing required columns: {missing}")

prices = (
    df_src.select(
        F.col("Symbol").cast("string").alias("Symbol"),
        F.col("Date").cast("date").alias("Date"),
        F.col("Open").cast("double").alias("Open"),
        F.col("High").cast("double").alias("High"),
        F.col("Low").cast("double").alias("Low"),
        F.col("Close").cast("double").alias("Close"),
        F.col("Volume").cast("long").alias("Volume"),
    )
    .dropna(subset=["Symbol","Date","Close"]) 
)

w = Window.partitionBy("Symbol").orderBy("Date")
w_unbounded = w.rowsBetween(Window.unboundedPreceding, 0)
w20 = w.rowsBetween(-19, 0)
w50 = w.rowsBetween(-49, 0)
w200 = w.rowsBetween(-199, 0)
w252 = w.rowsBetween(-251, 0)

# Daily returns
feat = prices.withColumn("PrevClose", F.lag("Close").over(w)) \
    .withColumn(
        "DailyReturn",
        F.when(F.col("PrevClose").isNull(), F.lit(0.0))
         .otherwise(F.col("Close")/F.col("PrevClose") - F.lit(1.0))
    ) \
    .withColumn("LogReturn", F.log1p(F.col("DailyReturn"))) \
    .drop("PrevClose")

# Cumulative and YTD returns
feat = feat.withColumn("CumReturn", F.exp(F.sum("LogReturn").over(w_unbounded)) - F.lit(1.0))
feat = feat.withColumn("Year", F.year("Date"))
wytd = Window.partitionBy("Symbol","Year").orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
feat = feat.withColumn("YTDReturn", F.exp(F.sum("LogReturn").over(wytd)) - F.lit(1.0))

# Calendar attributes
feat = (feat
    .withColumn("Month", F.month("Date"))
    .withColumn("MonthName", F.date_format("Date", "MMM"))
    .withColumn("Quarter", F.quarter("Date"))
    .withColumn("WeekOfYear", F.weekofyear("Date"))
    .withColumn("DayOfWeek", F.dayofweek("Date"))
    .withColumn("DayName", F.date_format("Date", "E"))
)

# Trend and momentum indicators
feat = (feat
    .withColumn("SMA20", F.avg("Close").over(w20))
    .withColumn("SMA50", F.avg("Close").over(w50))
    .withColumn("SMA200", F.avg("Close").over(w200))
    .withColumn("BBandStd20", F.stddev_samp("Close").over(w20))
    .withColumn("BBandUpper20", F.col("SMA20") + F.lit(2.0)*F.col("BBandStd20"))
    .withColumn("BBandLower20", F.col("SMA20") - F.lit(2.0)*F.col("BBandStd20"))
    .withColumn("PriceVsSMA50", F.col("Close")/F.col("SMA50") - F.lit(1.0))
    .withColumn("PriceVsSMA200", F.col("Close")/F.col("SMA200") - F.lit(1.0))
)

# Realized volatility (annualized 20d)
feat = feat.withColumn("Vol20D_Ann", F.stddev_samp("DailyReturn").over(w20) * F.sqrt(F.lit(252.0)))

# 52-week stats, drawdowns
feat = (feat
    .withColumn("RollMax252", F.max("Close").over(w252))
    .withColumn("RollMin252", F.min("Close").over(w252))
    .withColumn("Is52WHigh", F.when(F.col("Close") >= F.col("RollMax252"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("Is52WLow", F.when(F.col("Close") <= F.col("RollMin252"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("PctTo52WHigh", F.col("Close")/F.col("RollMax252") - F.lit(1.0))
    .withColumn("PctAbove52WLow", F.col("Close")/F.col("RollMin252") - F.lit(1.0))
)
feat = feat.withColumn("RunningMax", F.max("Close").over(w_unbounded)) \
    .withColumn("Drawdown", F.col("Close")/F.col("RunningMax") - F.lit(1.0)) \
    .withColumn("MaxDrawdownToDate", F.min("Drawdown").over(w_unbounded))

# Volume analytics
feat = (feat
    .withColumn("VolMA20", F.avg("Volume").over(w20))
    .withColumn("VolStd20", F.stddev_samp("Volume").over(w20))
    .withColumn("VolumeZ20", F.when((F.col("VolStd20").isNull()) | (F.col("VolStd20") == 0), F.lit(0.0))
                                 .otherwise((F.col("Volume")-F.col("VolMA20"))/F.col("VolStd20")))
)

# Equal-weight Magnificent 7 index (for context) and excess returns
idx = feat.select("Date","Symbol","DailyReturn").groupBy("Date").agg(F.avg("DailyReturn").alias("M7IndexReturn"))
w_date = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
idx = idx.withColumn("IndexLogReturn", F.log1p("M7IndexReturn")) \
         .withColumn("M7IndexCumReturn", F.exp(F.sum("IndexLogReturn").over(w_date)) - F.lit(1.0))
feat = feat.join(idx, on="Date", how="left") \
           .withColumn("ExcessReturn", F.col("DailyReturn") - F.col("M7IndexReturn")) \
           .withColumn("ExcessReturn20D", F.sum("ExcessReturn").over(w20))

# Symbol metadata for friendly labels
meta_rows = [
    ("AAPL","Apple Inc.","Information Technology"),
    ("MSFT","Microsoft Corporation","Information Technology"),
    ("NVDA","NVIDIA Corporation","Information Technology"),
    ("AMZN","Amazon.com, Inc.","Consumer Discretionary"),
    ("META","Meta Platforms, Inc.","Communication Services"),
    ("TSLA","Tesla, Inc.","Consumer Discretionary"),
    ("GOOGL","Alphabet Inc.","Communication Services"),
    ("GOOG","Alphabet Inc.","Communication Services"),
]
meta_df = spark.createDataFrame(meta_rows, ["Symbol","CompanyName","GICSSector"])
feat = feat.join(meta_df, on="Symbol", how="left")

# Latest date flag for slicers
max_date_by_sym = F.max("Date").over(Window.partitionBy("Symbol"))
feat = feat.withColumn("IsLatest", F.when(F.col("Date") == max_date_by_sym, F.lit(1)).otherwise(F.lit(0)))

select_cols = [
    "Symbol","CompanyName","GICSSector","Date","Year","Quarter","Month","MonthName","WeekOfYear","DayOfWeek","DayName",
    "Open","High","Low","Close","Volume","VolMA20","VolumeZ20",
    "DailyReturn","LogReturn","CumReturn","YTDReturn","Vol20D_Ann",
    "SMA20","SMA50","SMA200","BBandUpper20","BBandLower20","PriceVsSMA50","PriceVsSMA200",
    "RollMax252","RollMin252","Is52WHigh","Is52WLow","PctTo52WHigh","PctAbove52WLow",
    "RunningMax","Drawdown","MaxDrawdownToDate",
    "M7IndexReturn","M7IndexCumReturn","ExcessReturn","ExcessReturn20D","IsLatest"
]
result = feat.select(*[F.col(c) for c in select_cols])

result.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable("tabular.magnificent7")
display(result.orderBy(F.col("Date").desc(), F.col("Symbol")).limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
