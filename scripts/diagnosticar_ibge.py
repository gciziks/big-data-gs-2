"""Diagnóstico do IBGE: tenta várias formas de pedir a população e mostra o que funciona.

Use só se o scripts/testar_fontes.py continuar falhando no IBGE:
    python scripts/diagnosticar_ibge.py
Depois copie a saída inteira e me mande.
"""
import sys

import requests

NAVEGADOR = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json,text/plain,*/*",
}
PROJETO = {"User-Agent": "gs-citizentech-fiap/1.0 (projeto academico)"}
BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/2022/variaveis/93"
SP = "3550308"
DOZE = "3550308,3518800,3534401,3547809,3548708,3548807,3529401,3513801,3505708,3552809,3516309,3530607"

TESTES = [
    ("agregados, 1 município, identificação do projeto", f"{BASE}?localidades=N6[{SP}]", PROJETO),
    ("agregados, 1 município, identificação de navegador", f"{BASE}?localidades=N6[{SP}]", NAVEGADOR),
    ("agregados, colchetes codificados (%5B %5D)", f"{BASE}?localidades=N6%5B{SP}%5D", NAVEGADOR),
    ("agregados, 12 municípios de uma vez", f"{BASE}?localidades=N6[{DOZE}]", NAVEGADOR),
    ("SIDRA (plano B), 1 município", f"https://apisidra.ibge.gov.br/values/t/4709/n6/{SP}/v/93/p/2022", NAVEGADOR),
    ("SIDRA (plano B), 12 municípios", f"https://apisidra.ibge.gov.br/values/t/4709/n6/{DOZE}/v/93/p/2022", NAVEGADOR),
]


def main() -> int:
    ok = 0
    for nome, url, cab in TESTES:
        print(f"\n> {nome}\n  {url}")
        try:
            r = requests.get(url, headers=cab, timeout=30)
            print(f"  HTTP {r.status_code}, {len(r.content)} bytes")
            print(f"  início da resposta: {r.text[:300]!r}")
            ok += r.status_code == 200
        except Exception as erro:  # noqa: BLE001
            print(f"  FALHOU: {type(erro).__name__}: {erro}")
    print(f"\n{ok} de {len(TESTES)} pedidos deram HTTP 200. Copie tudo isto e me mande.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
