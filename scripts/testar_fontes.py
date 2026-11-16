"""Teste rápido (10 segundos) das fontes de dados, SEM Airflow.

Rode na sua máquina antes de subir o Airflow:
    pip install requests
    python scripts/testar_fontes.py

Ele consulta o Open-Meteo e o IBGE de verdade e mostra o que veio. Se algo falhar, o erro
diz qual fonte deu problema.
"""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
import coletores as col  # noqa: E402


def main() -> int:
    municipios = col.ler_municipios()
    ontem = date.today() - timedelta(days=1)
    falhas = 0

    print(f"1) Open-Meteo, clima de {ontem} em {municipios[0]['nome']} ...")
    try:
        linha = col.coletar_clima(ontem, municipios[:1], pausa=0)[0]
        h = linha["hourly"]
        print(f"   OK: {len(h['time'])} horas; chuva total {sum(x or 0 for x in h['precipitation']):.1f} mm; "
              f"temperatura máx {max(x for x in h['temperature_2m'] if x is not None):.1f} °C")
    except Exception as erro:  # noqa: BLE001
        falhas += 1
        print(f"   FALHOU: {erro}")

    print("2) IBGE, população dos municípios ...")
    try:
        linhas = col.coletar_ibge(municipios)
        for l in linhas[:3]:
            serie = l["resposta"][0]["resultados"][0]["series"][0]
            print(f"   OK: {serie['localidade']['nome']} -> {serie['serie']}")
        print(f"   ({len(linhas)} municípios no total; API usada: {linhas[0]['fonte']})")
    except Exception as erro:  # noqa: BLE001
        falhas += 1
        print(f"   FALHOU: {erro}")

    print("3) Inscrições simuladas ...")
    registros = col.gerar_inscricoes(ontem, municipios)
    print(f"   OK: {len(registros)} inscrições fictícias, exemplo: {registros[0]['nome']} <{registros[0]['email']}>")

    print("\nTudo certo!" if not falhas else f"\n{falhas} fonte(s) com problema. Me mande a mensagem acima.")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
