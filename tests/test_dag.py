"""Testa a estrutura da DAG e roda uma execução completa com APIs falsas (sem internet)."""
import os
import sys
from pathlib import Path

import pendulum

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "dags"))
sys.path.insert(0, str(RAIZ / "tests"))


def carregar_dag():
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(RAIZ / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["gs_coleta"]


def test_estrutura():
    dag = carregar_dag()
    ids = set(dag.task_ids)
    assert ids == {"inicio", "coletar_clima", "coletar_ibge", "gerar_inscricoes", "conferir_chegada", "notificar"}
    paralelas = {"coletar_clima", "coletar_ibge", "gerar_inscricoes"}
    assert set(dag.get_task("conferir_chegada").upstream_task_ids) == paralelas
    for t in paralelas:  # nenhuma depende das outras: rodam em paralelo
        assert set(dag.get_task(t).upstream_task_ids) == {"inicio"}
    assert dag.schedule == "@daily" and dag.catchup is True


def test_execucao_completa(tmp_path, monkeypatch):
    import coletores as col
    from fakes import falso_get_json
    monkeypatch.setattr(col, "_get_json", falso_get_json)
    monkeypatch.setattr(col.time, "sleep", lambda s: None)
    monkeypatch.setenv("AIRFLOW_VAR_CHEGADA_DIR", str(tmp_path))
    dag = carregar_dag()
    # logical_date = 11/11 00:00 em São Paulo -> deve coletar o dia 10/11
    run = dag.test(logical_date=pendulum.datetime(2026, 11, 11, tz="America/Sao_Paulo"))
    estados = {ti.task_id: str(ti.state) for ti in run.get_task_instances()}
    assert all(s.endswith("success") for s in estados.values()), estados
    for fonte in ("clima", "ibge", "inscricoes"):
        assert (tmp_path / fonte / f"{fonte}_2026-11-10.jsonl").stat().st_size > 0
