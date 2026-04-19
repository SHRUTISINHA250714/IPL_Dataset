# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

deliveries_df=spark.table("ipl_dataset.bronze.deliveries")
matches_df=spark.table("ipl_dataset.bronze.matches")

# COMMAND ----------

# display(deliveries_df.limit(100))

# COMMAND ----------

# deliveries_df = deliveries_df \
#     .withColumn("batting_team_1",
#         expr("concat_ws('', transform(split(batting_team, ' '), x -> substring(x, 1, 1)))")
#     ) \
#     .withColumn("bowling_team_1",
#         expr("concat_ws('', transform(split(bowling_team, ' '), x -> substring(x, 1, 1)))")
#     )
deliveries_df=deliveries_df.withColumn("ingested_at",current_timestamp())
display(deliveries_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ipl_dataset.silver

# COMMAND ----------

deliveries_df.write.mode("overwrite").saveAsTable("ipl_dataset.silver.deliveries")


# COMMAND ----------

# MAGIC %py
# MAGIC df1=matches_df.select(col("season")).distinct()
# MAGIC # display(df1)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when, current_timestamp

matches_df = (
    matches_df.withColumn(
        "season",
        when(col("season") == "2007/08", "2008")
        .when(col("season") == "2009/10", "2010")
        .when(col("season") == "2020/21", "2021")
        .otherwise(col("season"))
    )
)

matches_df = matches_df.withColumn("ingested_at", current_timestamp())

display(matches_df)

matches_df.write.mode("overwrite").saveAsTable("ipl_dataset.silver.matches")
    
    
    


# COMMAND ----------

teams_df=deliveries_df.select(col('batting_team').alias('Team_FullName')).distinct()
teams_df = teams_df \
    .withColumn("Team",
        expr("concat_ws('', transform(split(Team_FullName, ' '), x -> substring(x, 1, 1)))")
    )
display(teams_df)
teams_df.write.mode("overwrite").saveAsTable("ipl_dataset.silver.teams")