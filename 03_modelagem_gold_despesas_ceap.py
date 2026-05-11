# Databricks notebook source
from pyspark.sql.functions import (
    col,
    sum,
    count,
    avg,
    round,
    desc,
    current_timestamp
)

df_silver = spark.table("workspace.politica_silver.despesas_ceap")


# COMMAND ----------

df_gold_parlamentar = (
    df_silver
    .groupBy(
        "ano",
        "nome_parlamentar",
        "partido",
        "uf"
    )
    .agg(
        round(sum("valor_liquido"), 2).alias("total_gasto"),
        count("*").alias("quantidade_despesas"),
        round(avg("valor_liquido"), 2).alias("ticket_medio")
    )
    .withColumn("_data_processamento", current_timestamp())
    .orderBy(desc("total_gasto"))
)

df_gold_parlamentar.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_gold.gastos_por_parlamentar")

display(df_gold_parlamentar.limit(20))

# COMMAND ----------

df_gold_partido = (
    df_silver
    .groupBy(
        "ano",
        "partido"
    )
    .agg(
        round(sum("valor_liquido"), 2).alias("total_gasto"),
        count("*").alias("quantidade_despesas"),
        round(avg("valor_liquido"), 2).alias("ticket_medio")
    )
    .withColumn("_data_processamento", current_timestamp())
    .orderBy(desc("total_gasto"))
)

df_gold_partido.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_gold.gastos_por_partido")

display(df_gold_partido)

# COMMAND ----------

df_gold_uf = (
    df_silver
    .groupBy(
        "ano",
        "uf"
    )
    .agg(
        round(sum("valor_liquido"), 2).alias("total_gasto"),
        count("*").alias("quantidade_despesas"),
        round(avg("valor_liquido"), 2).alias("ticket_medio")
    )
    .withColumn("_data_processamento", current_timestamp())
    .orderBy(desc("total_gasto"))
)

df_gold_uf.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_gold.gastos_por_uf")

display(df_gold_uf)

# COMMAND ----------

df_gold_tipo_despesa = (
    df_silver
    .groupBy(
        "ano",
        "tipo_despesa"
    )
    .agg(
        round(sum("valor_liquido"), 2).alias("total_gasto"),
        count("*").alias("quantidade_despesas"),
        round(avg("valor_liquido"), 2).alias("ticket_medio")
    )
    .withColumn("_data_processamento", current_timestamp())
    .orderBy(desc("total_gasto"))
)

df_gold_tipo_despesa.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_gold.gastos_por_tipo_despesa")

display(df_gold_tipo_despesa)

# COMMAND ----------

df_gold_evolucao_mensal = (
    df_silver
    .groupBy(
        "ano",
        "mes"
    )
    .agg(
        round(sum("valor_liquido"), 2).alias("total_gasto"),
        count("*").alias("quantidade_despesas")
    )
    .withColumn("_data_processamento", current_timestamp())
    .orderBy("ano", "mes")
)

df_gold_evolucao_mensal.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_gold.evolucao_mensal_gastos")

display(df_gold_evolucao_mensal)

# COMMAND ----------

df_gold_fornecedores = (
    df_silver
    .groupBy(
        "ano",
        "fornecedor",
        "cnpj_cpf_fornecedor"
    )
    .agg(
        round(sum("valor_liquido"), 2).alias("total_recebido"),
        count("*").alias("quantidade_despesas"),
        round(avg("valor_liquido"), 2).alias("ticket_medio")
    )
    .withColumn("_data_processamento", current_timestamp())
    .orderBy(desc("total_recebido"))
)

df_gold_fornecedores.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_gold.top_fornecedores")

display(df_gold_fornecedores.limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.politica_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.politica_gold.gastos_por_parlamentar
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   nome_parlamentar,
# MAGIC   partido,
# MAGIC   uf,
# MAGIC   total_gasto
# MAGIC FROM workspace.politica_gold.gastos_por_parlamentar
# MAGIC ORDER BY total_gasto DESC
# MAGIC LIMIT 10;