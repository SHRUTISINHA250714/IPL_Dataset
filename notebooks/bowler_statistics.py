# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

deliveries_df=spark.table("ipl_dataset.silver.deliveries")
matches_df=spark.table("ipl_dataset.silver.matches")
teams=spark.table("ipl_dataset.silver.teams")

# COMMAND ----------

joined_df=deliveries_df.join(matches_df,on=[deliveries_df.match_id==matches_df.id],how="left")

# COMMAND ----------

print(joined_df.columns)

# COMMAND ----------

bowler_stats_1 = (
    joined_df
    .groupBy(
        col("season").alias("Year"),
        col("bowling_team").alias("Team"),
        col("bowler")
    )
    .agg(
        sum(col("is_wicket")).alias("Wickets"),
        sum(col("total_runs")).alias("Total_runs"),
        sum(col("extra_runs")).alias("Extras"),
        count(col("ball")).alias("Balls_bowled"),
        sum(
            when(
                col("extras_type").isin("wides", "noballs"),
                1
            ).otherwise(0)
        ).alias("extra_deliveries")
    )
)

bowler_stats_1 = bowler_stats_1.withColumn(
    "Legal_deliveries",
    col("Balls_bowled") - col("extra_deliveries")
)

bowler_stats_1 = bowler_stats_1.withColumn(
    "Economy",
    round(
        col("Total_runs") / 
        when(
            floor(col("Legal_deliveries") / 6) == 0,
            None
        ).otherwise(floor(col("Legal_deliveries") / 6)),
        2
    )
)

bowler_stats_1 = bowler_stats_1.withColumn(
    "Overs",
    concat(
        floor(col("Legal_deliveries") / 6).cast("string"),
        lit("."),
        (col("Legal_deliveries") % 6).cast("string")
    )
).orderBy(
    col("Year"),
    col("Wickets").desc()
)

display(bowler_stats_1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ipl_dataset.gold

# COMMAND ----------

bowler_stats_1 .write.mode("overwrite").saveAsTable("ipl_dataset.gold.bowler_stats")