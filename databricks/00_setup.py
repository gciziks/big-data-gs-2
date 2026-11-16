# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparação (rode uma vez)
# MAGIC Cria o schema `gs`, o Volume `chegada` (a "pasta de pouso") e as subpastas das 3 fontes.
# MAGIC
# MAGIC Depois, pela interface: **Catalog > workspace > gs > chegada > clima / ibge / inscricoes > Upload to this volume**
# MAGIC e suba os arquivos `.jsonl` que o Airflow gravou na pasta `chegada/` do repositório (requisito 04).

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gs")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gs.chegada")

BASE = "/Volumes/workspace/gs/chegada"
for pasta in ["clima", "ibge", "inscricoes", "_controle", "_saida"]:
    dbutils.fs.mkdirs(f"{BASE}/{pasta}")

display(dbutils.fs.ls(BASE))
