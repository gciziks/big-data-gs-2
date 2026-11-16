# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Entregar os dados ao MVP (requisito 08)
# MAGIC O Databricks gratuito não deixa um site de fora ler as tabelas direto. O contrato com o MVP é este arquivo:
# MAGIC `painel_alerta.json`, gerado a partir da tabela **`workspace.gs.ouro_painel_alerta`** (sem nenhum dado pessoal).
# MAGIC
# MAGIC Depois de rodar: **Catalog > workspace > gs > chegada > _saida > painel_alerta.json > Download** e coloque o arquivo
# MAGIC na pasta `mvp_dados/` do repositório. Os campos estão descritos em `docs/contrato_mvp.md`.

# COMMAND ----------

import json
from pyspark.sql import functions as F

painel = (
    spark.table("workspace.gs.ouro_painel_alerta")
    .withColumn("data", F.date_format("data", "yyyy-MM-dd"))
    .orderBy("data", "nome_municipio")
)
linhas = [r.asDict() for r in painel.collect()]

saida = "/Volumes/workspace/gs/chegada/_saida/painel_alerta.json"
with open(saida, "w", encoding="utf-8") as f:
    json.dump({"gerado_de": "workspace.gs.ouro_painel_alerta", "registros": linhas}, f, ensure_ascii=False, default=float)

print(f"{len(linhas)} registros gravados em {saida}")
display(painel.limit(10))
