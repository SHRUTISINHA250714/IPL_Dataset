# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

deliveries_df=spark.table("ipl_dataset.silver.deliveries")
matches_df=spark.table("ipl_dataset.silver.matches")
teams=spark.table("ipl_dataset.silver.teams")

# COMMAND ----------

joined_df=deliveries_df.join(matches_df,on=[deliveries_df.match_id==matches_df.id],how="left")


# COMMAND ----------

match_scoreboard = (
    joined_df
    .groupBy(
        col("season").alias("Year"),
        col("match_id"),
        col("inning"),
        col("batting_team").alias("Team")
    )
    .agg(
        sum("total_runs").alias("runs"),
        sum("is_wicket").alias("wickets"),
        count("ball").alias("balls")
    )
)

match_scoreboard = match_scoreboard.withColumn(
    "overs",
    round(col("balls") / 6, 2)
)

match_scoreboard = match_scoreboard.withColumn(
    "run_rate",
    round(col("runs") / col("overs"), 2)
)
display(match_scoreboard)

# COMMAND ----------

print(match_scoreboard.columns)

# COMMAND ----------


match_scoreboard.write.mode("overwrite").saveAsTable("ipl_dataset.gold.match_scoreboard")