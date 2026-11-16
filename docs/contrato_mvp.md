# Contrato entre o pipeline e o MVP

O MVP consome **uma tabela da camada Ouro: `workspace.gs.ouro_painel_alerta`**, exportada pelo notebook
`databricks/05_exportar_mvp.py` como `painel_alerta.json` (guarde uma cópia em `mvp_dados/`).
Nenhum dado pessoal passa por aqui, só números agregados por município e por dia.

## Formato do arquivo

```json
{
  "gerado_de": "workspace.gs.ouro_painel_alerta",
  "registros": [ { "...um registro por município e por dia..." } ]
}
```

## Campos de cada registro

| Campo | Tipo | O que é |
|---|---|---|
| `data` | texto `AAAA-MM-DD` | Dia da chuva |
| `codigo_ibge` | texto (7 dígitos) | Código do município no IBGE |
| `nome_municipio` | texto | Nome do município |
| `populacao` | número | Habitantes (Censo 2022) |
| `id_nivel_alerta` | número 1 a 4 | 1 Baixo, 2 Moderado, 3 Forte, 4 Muito forte |
| `nivel_alerta` | texto | Nome do nível |
| `chuva_total_mm` | número | Chuva total do dia |
| `chuva_max_hora_mm` | número | Maior chuva em 1 hora no dia |
| `horas_com_chuva` | número | Quantas horas do dia tiveram chuva |
| `temp_max_c` | número | Temperatura máxima do dia |
| `qtd_inscritos` | número | Cidadãos inscritos no município até aquele dia |
| `inscritos_a_avisar` | número | Inscritos que receberiam aviso (nível 3 ou 4), senão 0 |

Níveis (critério didático do projeto, não é o critério oficial da Defesa Civil), pela chuva total do dia:
Baixo menos de 5 mm; Moderado de 5 a menos de 20 mm; Forte de 20 a menos de 50 mm; Muito forte 50 mm ou mais.

## Telas sugeridas para o MVP

1. **Painel do dia:** lista ou mapa dos 12 municípios com a cor do nível de alerta.
2. **Meu município:** histórico dos últimos dias (chuva total e nível).
3. **Alcance do alerta:** população sob chuva e inscritos que seriam avisados (a consulta de impacto).
