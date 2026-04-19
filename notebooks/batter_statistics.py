# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

deliveries_df=spark.table("ipl_dataset.silver.deliveries")
matches_df=spark.table("ipl_dataset.silver.matches")
teams=spark.table("ipl_dataset.silver.teams")

# COMMAND ----------

joined_df=deliveries_df.join(matches_df,on=[deliveries_df.match_id==matches_df.id],how="left")

# COMMAND ----------

display(joined_df)

# COMMAND ----------

print(joined_df.columns)

# COMMAND ----------

# groupBy(season, batter) → groups runs by batsman in each year

# sum(batsman_runs) → calculates total runs scored by that batsman in that season

# orderBy → shows highest scorers per year

# COMMAND ----------

# batter_stats_1 = (
#     joined_df
#     .groupBy(
#         col("season").alias("Year"),
#         col("batting_team").alias("Team"),
#         col("batter").alias("batsman")
#     )
#     .agg(
#         sum("batsman_runs").alias("Total_runs")
#     )
#     .orderBy("Year",col("Total_runs").desc())
# )

# display(batter_stats_1)

# COMMAND ----------

# fours_by_batsman = (
#     joined_df
#     .filter(col("batsman_runs") == 4)
#     .groupBy("batter")
#     .agg(count("*").alias("total_fours"))
#     .orderBy(col("total_fours").desc())
# )

# display(fours_by_batsman)

# COMMAND ----------

# sixes_by_batsman = (
#     joined_df
#     .filter(col("batsman_runs") == 6)
#     .groupBy("batter")
#     .agg(count("*").alias("total_sixes"))
#     .orderBy(col("total_sixes").desc())
# )

# display(sixes_by_batsman)

# COMMAND ----------

batter_stats_1 = (
    joined_df
    .groupBy(
        col("season").alias("Year"),
        col("batting_team").alias("Team"),
        col("batter").alias("batsman")
    )
    .agg(
        sum("batsman_runs").alias("Total_runs"),
        count("ball").alias("No_of_deliveries"),
        sum(when(col("batsman_runs") == 4, 1).otherwise(0)).alias("Fours"),
        sum(when(col("batsman_runs") == 6, 1).otherwise(0)).alias("Sixes")
    )
    .withColumn(
        "Strike_Rate",
        round((col("Total_runs") / col("No_of_deliveries")) * 100, 2))
    .orderBy("Year",col("Total_runs").desc())
)

display(batter_stats_1)

# COMMAND ----------

# No of runs a batter scores in each match 
match_score = (
    joined_df
    .groupBy(
        col("season").alias("Year"),
        col("match_id").alias("match_id"),
        col("batting_team").alias("Team"),
        col("batter").alias("batsman")
    )
    .agg(
        sum("batsman_runs").alias("Total_runs")
    )
)

display(match_score)

# COMMAND ----------

batter_stats_2 = (
    match_score
    .groupBy( 
       'Year',
       'Team',
       'batsman'
    )
    .agg(
        sum(when((col("Total_runs") >= 50) & (col("Total_runs") < 100), 1).otherwise(0)).alias("Fifties"),
        sum(when(col("Total_runs") >= 100, 1).otherwise(0)).alias("Hundreds")
    )
    .orderBy(col("Hundreds").desc(), col("Fifties").desc())
)
batter_stats_2 = batter_stats_2.filter((col('Fifties') > 0) | (col('Hundreds') > 0))
display(batter_stats_2)

# COMMAND ----------

batter_stats=(
    batter_stats_1.join(
        batter_stats_2,
        on =['Year','Team','batsman'],
        how='left'
    )
    
)
batter_stats=batter_stats.select(
    'Year',col('Team').alias("Team_FullName"),'batsman','Total_runs','Fours','Sixes','Fifties','Hundreds','Strike_Rate'
)
batter_stats=batter_stats.join(teams,on=['Team_FullName'],how='left').select('Year','Team','batsman','Total_runs','Strike_Rate','Fours','Sixes','Fifties','Hundreds').orderBy('Year',col("Total_runs").desc()).fillna(0)
display(batter_stats)

# COMMAND ----------

print(batter_stats.columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ipl_dataset.gold

# COMMAND ----------

batter_stats.write.mode("overwrite").saveAsTable("ipl_dataset.gold.batter_stats")