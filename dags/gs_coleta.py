"""DAG gs_coleta (Airflow 3): coleta diária das fontes do Alerta Chuva SP.

Fluxo:
    inicio >> [coletar_clima, coletar_ibge, gerar_inscricoes] >> conferir_chegada >> notificar

- As 3 coletas rodam em paralelo (requisito 03).
- Cada uma grava UM arquivo .jsonl na pasta chegada/ com a data no nome (requisito 02).
- conferir_chegada confere se os 3 arquivos chegaram e têm conteúdo (requisito 03).
- notificar avisa o resultado no log e, se existir a Variable WEBHOOK_URL, também num webhook
  (Discord/Slack). Credenciais e URLs ficam em Admin > Variables, nunca no código.

Cada execução coleta o DIA ANTERIOR ao da execução (dia fechado, com 24 horas completas).
"""
from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # garante que coletores.py (mesma pasta) seja encontrado
import coletores as col  # noqa: E402

log = logging.getLogger(__name__)
TZ = "America/Sao_Paulo"


def pasta_chegada() -> Path:
    """Pasta de pouso. Pode ser trocada pela Variable CHEGADA_DIR (Admin > Variables)."""
    return Path(Variable.get("CHEGADA_DIR", default="/opt/airflow/chegada"))


def data_referencia(logical_date) -> date:
    """Dia fechado a ser coletado: o dia anterior à data lógica, no fuso de São Paulo."""
    return (pendulum.instance(logical_date).in_timezone(TZ) - timedelta(days=1)).date()


def nome_arquivo(fonte: str, data_ref: date) -> str:
    return f"{fonte}_{data_ref.isoformat()}.jsonl"


with DAG(
    dag_id="gs_coleta",
    description="Coleta diária: clima (Open-Meteo), população (IBGE) e inscrições simuladas",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["gs", "citizentech", "bigdata"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    @task
    def coletar_clima(logical_date=None) -> dict:
        data_ref = data_referencia(logical_date)
        registros = col.coletar_clima(data_ref, col.ler_municipios())
        destino = pasta_chegada() / "clima" / nome_arquivo("clima", data_ref)
        total = col.gravar_jsonl(registros, destino)
        log.info("Clima: %s linhas gravadas em %s", total, destino)
        return {"fonte": "clima", "arquivo": str(destino), "linhas": total}

    @task
    def coletar_ibge(logical_date=None) -> dict:
        data_ref = data_referencia(logical_date)
        registros = col.coletar_ibge(col.ler_municipios())
        destino = pasta_chegada() / "ibge" / nome_arquivo("ibge", data_ref)
        total = col.gravar_jsonl(registros, destino)
        log.info("IBGE: %s linhas gravadas em %s", total, destino)
        return {"fonte": "ibge", "arquivo": str(destino), "linhas": total}

    @task
    def gerar_inscricoes(logical_date=None) -> dict:
        data_ref = data_referencia(logical_date)
        registros = col.gerar_inscricoes(data_ref, col.ler_municipios())
        destino = pasta_chegada() / "inscricoes" / nome_arquivo("inscricoes", data_ref)
        total = col.gravar_jsonl(registros, destino)
        log.info("Inscrições simuladas: %s linhas gravadas em %s", total, destino)
        return {"fonte": "inscricoes", "arquivo": str(destino), "linhas": total}

    @task
    def conferir_chegada(logical_date=None) -> list[dict]:
        """Falha (e deixa a execução vermelha) se algum arquivo faltar, estiver vazio ou estragado."""
        data_ref = data_referencia(logical_date)
        resumo = []
        for fonte in ("clima", "ibge", "inscricoes"):
            caminho = pasta_chegada() / fonte / nome_arquivo(fonte, data_ref)
            resumo.append({"fonte": fonte, "arquivo": caminho.name, "linhas": col.contar_linhas_validas(caminho)})
        log.info("Chegada conferida: %s", resumo)
        return resumo

    @task(trigger_rule="all_done")
    def notificar(logical_date=None) -> str:
        """Avisa no log (e no webhook, se configurado) se o dia foi coletado com sucesso."""
        data_ref = data_referencia(logical_date)
        try:
            partes = []
            for fonte in ("clima", "ibge", "inscricoes"):
                caminho = pasta_chegada() / fonte / nome_arquivo(fonte, data_ref)
                partes.append(f"{fonte}={col.contar_linhas_validas(caminho)}")
            mensagem = f"[gs_coleta] OK {data_ref}: " + ", ".join(partes)
        except Exception as erro:  # noqa: BLE001 - qualquer problema vira aviso
            mensagem = f"[gs_coleta] FALHA {data_ref}: {erro}"
        log.info(mensagem)
        url = Variable.get("WEBHOOK_URL", default="")
        if url:
            try:
                requests.post(url, json={"content": mensagem, "text": mensagem}, timeout=15)
            except requests.RequestException as erro:
                log.warning("Não consegui enviar ao webhook: %s", erro)
        return mensagem

    clima, ibge, inscricoes = coletar_clima(), coletar_ibge(), gerar_inscricoes()
    conferido = conferir_chegada()
    inicio >> [clima, ibge, inscricoes] >> conferido >> notificar()
