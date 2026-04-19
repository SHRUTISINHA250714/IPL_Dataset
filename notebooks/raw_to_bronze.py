# Databricks notebook source
# MAGIC  %sql
# MAGIC  create schema if not exists ipl_dataset.bronze

# COMMAND ----------

deliveries_df = (
    spark.read
    .format('csv')
    .option('header', 'true')
    .load('/Volumes/ipl_dataset/raw/raw/deliveries.csv')
    )

# COMMAND ----------

display(deliveries_df)

# COMMAND ----------

print(deliveries_df.columns)

# COMMAND ----------

deliveries_df.write.mode('overwrite').saveAsTable('ipl_dataset.bronze.deliveries')

# COMMAND ----------

matches_df = (
    spark.read
    .format('csv')
    .option('header', 'true')
    .load('/Volumes/ipl_dataset/raw/raw/matches.csv')
    )

# COMMAND ----------

display(matches_df)

# COMMAND ----------

print(matches_df.columns)

# COMMAND ----------

matches_df.write.mode('overwrite').saveAsTable('ipl_dataset.bronze.matches')