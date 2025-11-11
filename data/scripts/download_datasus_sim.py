"""
Script de coleta automática dos dados do SIM (DATASUS)
Filtra apenas registros de acidentes de transporte (CID-10: V01–V99)
Compatível com estrutura do data-road-br (ambientes dev/prod)
"""

import os
import requests
from pathlib import Path
from zipfile import ZipFile
from dbfread import DBF
import pandas as pd
import io
import argparse

# --- Configurações ---
ENV = os.getenv("ENV", "dev")
DATA_DIR = Path(f"data/raw/{ENV}/datasus_sim")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES"

# --- Argumentos de linha de comando ---
parser = argparse.ArgumentParser(description="Baixa e processa dados do SIM (DATASUS)")
parser.add_argument("--start-year", type=int, required=True, help="Ano inicial (ex: 2010)")
parser.add_argument("--end-year", type=int, required=True, help="Ano final (ex: 2022)")
args = parser.parse_args()

# --- Estados disponíveis (códigos UF do DATASUS) ---
UF_CODES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

# --- Função: baixa arquivo ZIP do SIM ---
def download_sim_zip(year, uf):
    url = f"{BASE_URL}/DO{uf}{str(year)[-2:]}.DBF.zip"
    local_zip = DATA_DIR / f"DO{uf}{year}.zip"
    try:
        r = requests.get(url, stream=True, timeout=60)
        if r.status_code != 200:
            print(f"⚠️ {uf}-{year}: não encontrado ({r.status_code})")
            return None
        with open(local_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Baixado {local_zip.name}")
        return local_zip
    except Exception as e:
        print(f"⚠️ Erro ao baixar {uf}-{year}: {e}")
        return None

# --- Função: extrai DBF e filtra acidentes ---
def extract_and_filter(zip_path):
    try:
        with ZipFile(zip_path, "r") as z:
            dbf_name = [n for n in z.namelist() if n.lower().endswith(".dbf")][0]
            with z.open(dbf_name) as dbf_file:
                # leitura direta do DBF
                table = DBF(io.BytesIO(dbf_file.read()), encoding="latin-1")
                df = pd.DataFrame(iter(table))

                # Filtra apenas causas externas V01–V99 (acidentes de transporte)
                if "CAUSABAS" in df.columns:
                    df = df[df["CAUSABAS"].str.match(r"V\d{2}", na=False)]

                out_file = DATA_DIR / f"{Path(dbf_name).stem}.csv"
                df.to_csv(out_file, index=False)
                print(f"📦 Extraído e salvo {out_file.name}")
    except Exception as e:
        print(f"⚠️ Erro ao extrair {zip_path.name}: {e}")

# --- Execução principal ---
for year in range(args.start_year, args.end_year + 1):
    for uf in UF_CODES:
        zip_file = download_sim_zip(year, uf)
        if zip_file:
            extract_and_filter(zip_file)
            zip_file.unlink()  # remove ZIP após processar

print("🎉 Todos os arquivos do SIM baixados e filtrados com sucesso!")
