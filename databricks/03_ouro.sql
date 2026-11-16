-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Ouro: modelo dimensional + proteção de dados (requisitos 07 e 08)
-- MAGIC **Modelo estrela** (1 fato + 3 dimensões de análise + 1 dimensão de inscritos):
-- MAGIC
-- MAGIC - `fato_chuva_dia`: 1 linha por município e por dia (chuva total, chuva máxima em 1 hora, temperaturas, nível de alerta).
-- MAGIC - `dim_municipio`, `dim_data`, `dim_nivel_alerta`: descrevem o fato.
-- MAGIC - `dim_inscrito`: cidadãos inscritos para receber alerta, **com dados pessoais protegidos** (máscara e hash).
-- MAGIC - `ouro_painel_alerta`: tabela pronta que o **MVP consome** (sem nenhum dado pessoal).

-- COMMAND ----------

-- Níveis de alerta. Critério DIDÁTICO do projeto (não é o critério oficial da Defesa Civil).
-- min_mm é incluso e max_mm é exclusivo (chuva total do dia, em mm).
CREATE OR REPLACE TABLE workspace.gs.dim_nivel_alerta AS
SELECT * FROM VALUES
  (1, 'Baixo',       0.0,     5.0, 'Pouca ou nenhuma chuva'),
  (2, 'Moderado',    5.0,    20.0, 'Chuva moderada: atenção em pontos de alagamento'),
  (3, 'Forte',      20.0,    50.0, 'Chuva forte: risco de alagamentos'),
  (4, 'Muito forte',50.0, 10000.0, 'Chuva muito forte: risco alto, avisar a população')
AS t(id_nivel, nome_nivel, min_mm, max_mm, descricao);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_municipio AS
SELECT
  codigo_ibge,
  nome AS nome_municipio,
  uf,
  populacao,
  ano_referencia AS ano_populacao,
  CASE WHEN populacao >= 1000000 THEN 'Grande (1 milhão ou mais)'
       WHEN populacao >= 300000  THEN 'Média (300 mil a 1 milhão)'
       ELSE 'Pequena (menos de 300 mil)' END AS faixa_populacao
FROM workspace.gs.prata_ibge;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_data AS
SELECT
  d AS data,
  year(d) AS ano,
  month(d) AS mes,
  day(d) AS dia,
  dayofweek(d) AS dia_semana_num,
  ELT(dayofweek(d), 'Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado') AS dia_semana,
  dayofweek(d) IN (1, 7) AS fim_de_semana
FROM (SELECT DISTINCT CAST(data_hora AS DATE) AS d FROM workspace.gs.prata_clima);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.fato_chuva_dia AS
WITH diario AS (
  SELECT
    codigo_ibge,
    CAST(data_hora AS DATE) AS data,
    ROUND(SUM(precipitacao_mm), 1) AS chuva_total_mm,
    MAX(precipitacao_mm) AS chuva_max_hora_mm,
    SUM(CASE WHEN precipitacao_mm > 0 THEN 1 ELSE 0 END) AS horas_com_chuva,
    MAX(temperatura_c) AS temp_max_c,
    MIN(temperatura_c) AS temp_min_c,
    COUNT(*) AS horas_registradas
  FROM workspace.gs.prata_clima
  GROUP BY codigo_ibge, CAST(data_hora AS DATE)
)
SELECT d.codigo_ibge, d.data, n.id_nivel AS id_nivel_alerta,
       d.chuva_total_mm, d.chuva_max_hora_mm, d.horas_com_chuva,
       d.temp_max_c, d.temp_min_c, d.horas_registradas
FROM diario d
JOIN workspace.gs.dim_nivel_alerta n
  ON d.chuva_total_mm >= n.min_mm AND d.chuva_total_mm < n.max_mm;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## LGPD (requisito 08)
-- MAGIC Os dados pessoais (nome, e-mail, telefone) **não passam para a Ouro como vieram**:
-- MAGIC - nome: só o primeiro nome e a inicial do sobrenome (`Ana S.`)
-- MAGIC - e-mail: **hash** SHA-256 (não dá para ler, mas dá para saber se dois registros têm o mesmo e-mail)
-- MAGIC - telefone: **máscara** (`(11) *****-1234`)
-- MAGIC
-- MAGIC Todos os dados desta fonte são fictícios; a técnica é a que se usaria com dados reais.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_inscrito AS
SELECT
  id_inscricao,
  concat_ws(' ', split(nome, ' ')[0], nullif(concat(left(split(nome, ' ')[1], 1), '.'), '.')) AS nome_mascarado,
  sha2(lower(trim(email)), 256) AS email_hash,
  concat('(', left(telefone, 2), ') *****-', right(telefone, 4)) AS telefone_mascarado,
  codigo_ibge,
  canal,
  data_inscricao
FROM workspace.gs.prata_inscricoes;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Tabela que o MVP consome: `ouro_painel_alerta`
-- MAGIC Uma linha por município e por dia, já com o nível de alerta e quantas pessoas inscritas seriam avisadas.
-- MAGIC `inscritos_a_avisar` conta os inscritos até aquele dia, quando o nível é **Forte (3)** ou pior.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_painel_alerta AS
WITH inscritos AS (
  SELECT f.codigo_ibge, f.data, COUNT(i.id_inscricao) AS qtd_inscritos
  FROM workspace.gs.fato_chuva_dia f
  LEFT JOIN workspace.gs.dim_inscrito i
    ON i.codigo_ibge = f.codigo_ibge AND i.data_inscricao <= f.data
  GROUP BY f.codigo_ibge, f.data
)
SELECT
  f.data,
  m.codigo_ibge,
  m.nome_municipio,
  m.populacao,
  n.id_nivel AS id_nivel_alerta,
  n.nome_nivel AS nivel_alerta,
  f.chuva_total_mm,
  f.chuva_max_hora_mm,
  f.horas_com_chuva,
  f.temp_max_c,
  COALESCE(c.qtd_inscritos, 0) AS qtd_inscritos,
  CASE WHEN n.id_nivel >= 3 THEN COALESCE(c.qtd_inscritos, 0) ELSE 0 END AS inscritos_a_avisar
FROM workspace.gs.fato_chuva_dia f
JOIN workspace.gs.dim_municipio m ON m.codigo_ibge = f.codigo_ibge
JOIN workspace.gs.dim_nivel_alerta n ON n.id_nivel = f.id_nivel_alerta
LEFT JOIN inscritos c ON c.codigo_ibge = f.codigo_ibge AND c.data = f.data;

-- COMMAND ----------

SELECT * FROM workspace.gs.ouro_painel_alerta ORDER BY data DESC, chuva_total_mm DESC LIMIT 20;
