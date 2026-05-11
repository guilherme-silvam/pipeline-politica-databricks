# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.politica_raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.politica_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.politica_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.politica_gold;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.politica_raw.landing;

# COMMAND ----------

origem = "/Volumes/workspace/politica_raw/landing/Ano-2024.csv.zip"

destino_dir = "/Volumes/workspace/politica_raw/landing/camara_ceap/despesas/ano=2024/"
destino = destino_dir + "Ano-2024.csv.zip"

dbutils.fs.mkdirs(destino_dir)

dbutils.fs.mv(origem, destino)

print("Arquivo movido com sucesso para:")
print(destino)

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/politica_raw/landing/camara_ceap/despesas/ano=2024/")