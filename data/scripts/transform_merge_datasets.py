"""
Transforma e integra datasets (Frota + População + SIM)
Gera tabelas normalizadas para visualização / dashboard
"""

import pandas as pd
from pathlib import Path
import os

ENV = os.getenv("ENV", "dev")
RAW_DIR = Path(f"data/raw/{ENV}")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Carrega Frota ---
frota_files = list(RAW_DIR.glob("Frota de veículos - Mensal - *.csv"))
frota = pd.concat([pd.read_csv(f, low_memory=False) for f in frota_files])
print(f"📊 Frota: {len(frota)} linhas")

# --- Carrega População ---
pop = pd.read_csv(RAW_DIR / "ibge_population" / "ibge_population.csv")

# --- Carrega SIM (se existir) ---
sim_path = RAW_DIR / "datasus_sim"
sim_files = list(sim_path.glob("*.csv"))
sim = pd.concat([pd.read_csv(f, low_memory=False) for f in sim_files]) if sim_files else None

# --- Normaliza colunas (exemplo básico) ---
# Aqui você ajusta conforme seus CSVs
frota.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
pop.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)

# --- Merge: Frota + População ---
merged = frota.merge(pop, how="left", left_on="codigo_municipio", right_on="cod_municipio")

# --- Indicadores ---
merged["frota_per_capita"] = merged["total_veiculos"] / merged["populacao"]

# --- (Opcional) Acidentes ---
if sim is not None:
    merged = merged.merge(sim, how="left", on="cod_municipio")

# --- Exporta ---
out_file = OUT_DIR / "base_analitica.csv"
merged.to_csv(out_file, index=False)
print(f"✅ Base integrada salva em {out_file}")
