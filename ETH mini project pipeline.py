# Databricks notebook source
# DBTITLE 1,impoet function , add Error Handling (try-except) Around the API Call
import requests
import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- IMPROVED: Wrap API call in try-except ---
URL = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart"
PARAMS = {"vs_currency": "usd", "days": 30, "interval": "daily"}

try:
    response = requests.get(URL, params=PARAMS, timeout=10)  # timeout prevents hanging forever
    response.raise_for_status()  # raises an error for 4xx/5xx HTTP codes
    data = response.json()
    logger.info("API call successful. Records received.")

except requests.exceptions.Timeout:
    logger.error("API request timed out. CoinGecko may be slow.")
    raise

except requests.exceptions.HTTPError as e:
    logger.error(f"HTTP error from API: {e.response.status_code} - {e}")
    raise

except requests.exceptions.RequestException as e:
    logger.error(f"Unexpected API error: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Use Spark Natively — Skip the Pandas-to-Spark Roundtrip
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

# Build rows directly from the raw API response
prices  = data['prices']   # [[timestamp, price], ...]
volumes = data['total_volumes']  # [[timestamp, volume], ...]

# Zip into a list of tuples: (timestamp, price, volume)
rows = [
    (int(p[0]), float(p[1]), float(v[1]))
    for p, v in zip(prices, volumes)
]

# Define schema explicitly — never let Spark infer schema in production
schema = StructType([
    StructField("timestamp_ms", LongType(),  nullable=False),
    StructField("price",        DoubleType(), nullable=False),
    StructField("volume",       DoubleType(), nullable=False),
])

# Create Spark DataFrame directly — no Pandas involved
df_spark = spark.createDataFrame(rows, schema=schema)

# Transform: convert timestamp and round values
df_clean = df_spark.withColumn(
    "date", F.to_date(F.from_unixtime(F.col("timestamp_ms") / 1000))
).select(
    F.col("date"),
    F.round("price",  2).alias("price"),
    F.round("volume", 2).alias("volume")
)

df_clean.createOrReplaceTempView("eth_daily")
logger.info(f"Temp view created. Row count: {df_clean.count()}")

# COMMAND ----------

# DBTITLE 1,Separate Config from Logic (Use Constants / A Config Block)
# ============================================================
# CONFIG BLOCK — change these without touching pipeline logic
# ============================================================
COIN_ID        = "ethereum"
VS_CURRENCY    = "usd"
DAYS_BACK      = 30
INTERVAL       = "daily"
DB_PATH        = "crypto_data.db"
TABLE_NAME     = "eth_daily"
VIEW_NAME      = "eth_daily"
API_TIMEOUT    = 10  # seconds

# Date format for display: วัน/เดือน/ปี
DATE_FORMAT    = "dd/MM/yyyy"

# Build URL from config — no hardcoded strings in logic
BASE_URL = f"https://api.coingecko.com/api/v3/coins/{COIN_ID}/market_chart"
PARAMS   = {"vs_currency": VS_CURRENCY, "days": DAYS_BACK, "interval": INTERVAL}

# COMMAND ----------

# DBTITLE 1,Display
display(df_clean)

# COMMAND ----------

# DBTITLE 1,Daily % Return + Cumulative Return
# MAGIC %sql
# MAGIC     
# MAGIC -- COMMAND ----------
# MAGIC -- DBTITLE 1, Q1: Daily Return & Cumulative Performance
# MAGIC
# MAGIC WITH daily_returns AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         price,
# MAGIC         LAG(price) OVER (ORDER BY date)     AS prev_price,
# MAGIC
# MAGIC         ROUND(
# MAGIC             (price - LAG(price) OVER (ORDER BY date))
# MAGIC             / LAG(price) OVER (ORDER BY date) * 100
# MAGIC         , 2)                                AS daily_return_pct
# MAGIC
# MAGIC     FROM eth_daily
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     date,
# MAGIC     ROUND(price, 2)         AS close_price,
# MAGIC     ROUND(prev_price, 2)    AS prev_close,
# MAGIC     daily_return_pct,
# MAGIC
# MAGIC     -- Cumulative return anchored to Day 1
# MAGIC     ROUND(
# MAGIC         (price - FIRST_VALUE(price) OVER (ORDER BY date))
# MAGIC         / FIRST_VALUE(price) OVER (ORDER BY date) * 100
# MAGIC     , 2)                    AS cumulative_return_pct,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN daily_return_pct >  3  THEN 'Strong Up'
# MAGIC         WHEN daily_return_pct >  0  THEN 'Up'
# MAGIC         WHEN daily_return_pct =  0  THEN 'Flat'
# MAGIC         WHEN daily_return_pct > -3  THEN 'Down'
# MAGIC         ELSE                             'Strong Down'
# MAGIC     END                     AS day_sentiment
# MAGIC
# MAGIC FROM daily_returns
# MAGIC ORDER BY date DESC;

# COMMAND ----------

# DBTITLE 1,Rolling 7-Day Volatility
# MAGIC %sql
# MAGIC     
# MAGIC -- COMMAND ----------
# MAGIC -- DBTITLE 1, Q2: Rolling Volatility & Risk Trend
# MAGIC
# MAGIC WITH returns AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         price,
# MAGIC         ROUND(
# MAGIC             (price - LAG(price) OVER (ORDER BY date))
# MAGIC             / LAG(price) OVER (ORDER BY date) * 100
# MAGIC         , 4)    AS daily_return_pct
# MAGIC     FROM eth_daily
# MAGIC ),
# MAGIC
# MAGIC volatility AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         price,
# MAGIC         daily_return_pct,
# MAGIC
# MAGIC         -- This week's volatility
# MAGIC         ROUND(
# MAGIC             STDDEV(daily_return_pct) OVER (
# MAGIC                 ORDER BY date
# MAGIC                 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC             )
# MAGIC         , 4)    AS volatility_7d,
# MAGIC
# MAGIC         -- Last week's volatility (for comparison)
# MAGIC         ROUND(
# MAGIC             STDDEV(daily_return_pct) OVER (
# MAGIC                 ORDER BY date
# MAGIC                 ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
# MAGIC             )
# MAGIC         , 4)    AS volatility_prev_7d
# MAGIC
# MAGIC     FROM returns
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     date,
# MAGIC     ROUND(price, 2)         AS close_price,
# MAGIC     daily_return_pct,
# MAGIC     volatility_7d,
# MAGIC     volatility_prev_7d,
# MAGIC
# MAGIC     -- Is the market getting riskier or calmer?
# MAGIC     CASE
# MAGIC         WHEN volatility_7d > volatility_prev_7d THEN 'Rising Risk'
# MAGIC         WHEN volatility_7d < volatility_prev_7d THEN 'Falling Risk'
# MAGIC         ELSE                                         'Stable'
# MAGIC     END                     AS risk_trend,
# MAGIC
# MAGIC     -- Where does today's volatility rank in the full 30-day window?
# MAGIC     ROUND(
# MAGIC         PERCENT_RANK() OVER (ORDER BY volatility_7d)
# MAGIC     , 2)                    AS volatility_percentile
# MAGIC
# MAGIC FROM volatility
# MAGIC WHERE volatility_7d IS NOT NULL
# MAGIC ORDER BY date DESC;

# COMMAND ----------

# DBTITLE 1,Anomaly Detection via Z-Score
# MAGIC %sql
# MAGIC     
# MAGIC -- COMMAND ----------
# MAGIC -- DBTITLE 1, Q3: Anomaly Detection (Z-Score Method)
# MAGIC
# MAGIC WITH stats AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         price,
# MAGIC         ROUND(
# MAGIC             (price - LAG(price) OVER (ORDER BY date))
# MAGIC             / LAG(price) OVER (ORDER BY date) * 100
# MAGIC         , 4)    AS daily_return_pct,
# MAGIC
# MAGIC         -- 30-day rolling benchmark
# MAGIC         AVG(price) OVER (
# MAGIC             ORDER BY date
# MAGIC             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
# MAGIC         )       AS rolling_mean_30d,
# MAGIC
# MAGIC         STDDEV(price) OVER (
# MAGIC             ORDER BY date
# MAGIC             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
# MAGIC         )       AS rolling_std_30d
# MAGIC
# MAGIC     FROM eth_daily
# MAGIC ),
# MAGIC
# MAGIC zscore_calc AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         -- Z-Score: how many std deviations from the 30-day mean?
# MAGIC         ROUND(
# MAGIC             (price - rolling_mean_30d) / NULLIF(rolling_std_30d, 0)
# MAGIC         , 2)    AS z_score
# MAGIC     FROM stats
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     date,
# MAGIC     ROUND(price, 2)             AS close_price,
# MAGIC     daily_return_pct,
# MAGIC     ROUND(rolling_mean_30d, 2)  AS benchmark_avg,
# MAGIC     ROUND(rolling_std_30d,  2)  AS benchmark_std,
# MAGIC     z_score,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN z_score >  2 THEN 'Price Spike — Investigate'
# MAGIC         WHEN z_score < -2 THEN 'Price Crash — Investigate'
# MAGIC         WHEN ABS(z_score) BETWEEN 1 AND 2 THEN 'Notable Move'
# MAGIC         ELSE                                    'Normal'
# MAGIC     END                         AS anomaly_flag
# MAGIC
# MAGIC FROM zscore_calc
# MAGIC WHERE z_score IS NOT NULL
# MAGIC ORDER BY ABS(z_score) DESC;   -- Most extreme events surface first