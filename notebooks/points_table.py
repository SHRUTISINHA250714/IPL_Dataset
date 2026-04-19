# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

match_results = spark.table("ipl_dataset.gold.match_results")
match_scoreboard = spark.table("ipl_dataset.gold.match_scoreboard")

# COMMAND ----------


from pyspark.sql.window import Window

# Load gold tables
match_results = spark.table("ipl_dataset.gold.match_results")
match_scoreboard = spark.table("ipl_dataset.gold.match_scoreboard")

# ---------------------------------------------------
# Step 1: Convert team1 and team2 into rows
# ---------------------------------------------------

team1 = match_results.select(
    col("Year"),
    col("match_id"),
    col("team1").alias("Team"),
    col("winner"),
    col("result")
)

team2 = match_results.select(
    col("Year"),
    col("match_id"),
    col("team2").alias("Team"),
    col("winner"),
    col("result")
)

teams_all = team1.union(team2)

# ---------------------------------------------------
# Step 2: Compute win / loss / no_result
# ---------------------------------------------------

points_base = (
    teams_all
    .withColumn("win", when(col("winner") == col("Team"), 1).otherwise(0))
    .withColumn("loss", when((col("winner") != col("Team")) & col("winner").isNotNull(), 1).otherwise(0))
    .withColumn("no_result", when(col("result") == "no result", 1).otherwise(0))
)

# ---------------------------------------------------
# Step 3: Aggregate match stats
# ---------------------------------------------------

points_agg = (
    points_base
    .groupBy("Year","Team")
    .agg(
        count("match_id").alias("matches_played"),
        sum("win").alias("wins"),
        sum("loss").alias("losses"),
        sum("no_result").alias("no_results")
    )
)

# ---------------------------------------------------
# Step 4: Compute points
# ---------------------------------------------------

points_agg = points_agg.withColumn(
    "points",
    col("wins")*2 + col("no_results")
)

# ---------------------------------------------------
# Step 5: Runs scored
# ---------------------------------------------------

runs_scored = (
    match_scoreboard
    .groupBy("Year","Team")
    .agg(
        sum("runs").alias("runs_scored")
    )
)

# ---------------------------------------------------
# Step 6: Overs faced
# ---------------------------------------------------

overs_faced = (
    match_scoreboard
    .groupBy("Year","Team")
    .agg(
        sum(
            split(col("overs"), r"\.")[0].cast("int") +
            split(col("overs"), r"\.")[1].cast("int")/6
        ).alias("overs_faced")
    )
)

# ---------------------------------------------------
# Step 7: Runs conceded
# ---------------------------------------------------

runs_conceded = (
    match_scoreboard
    .groupBy("Year","match_id")
    .agg(sum("runs").alias("total_match_runs"))
)

runs_conceded = (
    match_scoreboard
    .join(runs_conceded,["Year","match_id"])
    .withColumn("runs_conceded",col("total_match_runs")-col("runs"))
    .groupBy("Year","Team")
    .agg(sum("runs_conceded").alias("runs_conceded"))
)

# ---------------------------------------------------
# Step 8: Overs bowled
# ---------------------------------------------------

overs_bowled = overs_faced.withColumnRenamed("Team","Team") \
                          .withColumnRenamed("overs_faced","overs_bowled")

# ---------------------------------------------------
# Step 9: Combine run stats
# ---------------------------------------------------

run_stats = (
    runs_scored
    .join(runs_conceded,["Year","Team"])
    .join(overs_faced,["Year","Team"])
    .join(overs_bowled,["Year","Team"])
)

# ---------------------------------------------------
# Step 10: Net Run Rate
# ---------------------------------------------------

run_stats = run_stats.withColumn(
    "net_run_rate",
    round(
        (col("runs_scored")/col("overs_faced")) -
        (col("runs_conceded")/col("overs_bowled")),
        3
    )
)

# ---------------------------------------------------
# Step 11: Final points table
# ---------------------------------------------------

points_table = (
    points_agg
    .join(
        run_stats.select("Year","Team","net_run_rate"),
        ["Year","Team"],
        "left"
    )
)

# ---------------------------------------------------
# Step 12: Rank teams per season
# ---------------------------------------------------

window_spec = Window.partitionBy("Year").orderBy(
    col("points").desc(),
    col("net_run_rate").desc()
)

points_table = points_table.withColumn(
    "rank",
    rank().over(window_spec)
)

# ---------------------------------------------------
# Step 13: Sort standings
# ---------------------------------------------------

points_table = points_table.orderBy(
    col("Year"),
    col("rank")
)




display(points_table)

# COMMAND ----------

print(points_table.columns)

# COMMAND ----------

points_table.write.mode("overwrite").saveAsTable("ipl_dataset.gold.points_table")
