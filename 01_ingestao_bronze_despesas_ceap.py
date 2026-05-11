# Databricks notebook source
import zipfile
import os

zip_path = "/Volumes/workspace/politica_raw/landing/camara_ceap/despesas/ano=2024/Ano-2024.csv.zip"
extract_path = "/Volumes/workspace/politica_raw/landing/camara_ceap/despesas/ano=2024/extraido"

os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_path)

print("Arquivo extraído com sucesso!")

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/politica_raw/landing/camara_ceap/despesas/ano=2024/extraido")

# COMMAND ----------

csv_path = "/Volumes/workspace/politica_raw/landing/camara_ceap/despesas/ano=2024/extraido/*.csv"

df_bronze = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", True)
    .csv(csv_path)
)

display(df_bronze.limit(10))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_bronze = (
    df_bronze
    .withColumn("_data_ingestao", current_timestamp())
    .withColumn("_arquivo_origem", lit("Ano-2024.csv.zip"))
    .withColumn("_camada", lit("bronze"))
)

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_bronze.despesas_ceap")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.politica_bronze.despesas_ceap
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_linhas
# MAGIC FROM workspace.politica_bronze.despesas_ceap;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.politica_bronze.despesas_ceap;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   numAno,
# MAGIC   COUNT(*) AS total_linhas
# MAGIC FROM workspace.politica_bronze.despesas_ceap
# MAGIC GROUP BY numAno
# MAGIC ORDER BY numAno;