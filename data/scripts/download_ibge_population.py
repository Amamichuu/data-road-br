"""
Baixa dados de população do IBGE (SIDRA)
Fonte: Tabela 6579 - População residente estimada
Compatível com estrutura do data-road-br
"""

import pandas as pd
from pathlib import Path
import os

ENV = os.getenv("ENV", "dev")
DATA_DIR = Path(f"data/raw/{ENV}/ibge_population")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Tabela: 6579 - População residente (estimada)
URL = "https://apisidra.ibge.gov.br/values/t/6579/n1/all/n3/all/v/93/p/last"

print("⬇️ Baixando população IBGE ...")
df = pd.read_json(URL)

# A resposta da API precisa ser tratada
# Colunas fixas: D1N (Nome), D3C (Código Município), V (Valor População)
df = df.rename(columns={
    "D1N": "nivel",
    "D3C": "cod_municipio",
    "D3N": "municipio",
    "V": "populacao"
})

out_file = DATA_DIR / "ibge_population.csv"
df.to_csv(out_file, index=False)
print(f"✅ Salvo {out_file}")
