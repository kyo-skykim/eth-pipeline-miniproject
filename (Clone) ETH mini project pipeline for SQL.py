# Databricks notebook source
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