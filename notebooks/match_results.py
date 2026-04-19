# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

deliveries_df=spark.table("ipl_dataset.silver.deliveries")
matches_df=spark.table("ipl_dataset.silver.matches")
teams=spark.table("ipl_dataset.silver.teams")

# COMMAND ----------



match_results = (
    matches_df
    .dropDuplicates(["id"])   # safety check
    .select(
        col("id").alias("match_id"),
        col("season").alias("Year"),
        col("city"),
        col("date"),
        col("venue"),
        col("team1"),
        col("team2"),
        col("toss_winner"),
        col("toss_decision"),
        col("winner"),
        col("result"),
        col("result_margin"),
        col("player_of_match"),
        col("target_runs"),
        col("target_overs"),
        col("super_over"),
        col("method"),
        col("umpire1"),
        col("umpire2")
    )
)

# COMMAND ----------

display (match_results )

# COMMAND ----------

print(match_results.columns)

# COMMAND ----------

match_results.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_dataset.gold.match_results")