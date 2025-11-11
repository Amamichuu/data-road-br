import os
import requests
from pathlib import Path
from zipfile import ZipFile
import pandas as pd
import io

# --- Configurações ---
TOKEN = "SEU_TOKEN_AQUI"
ENV = os.getenv("ENV", "dev")
DATA_DIR = Path(f"data/raw/{ENV}")
DATA_DIR.mkdir(parents=True, exist_ok=True)

API_URL = "https://dados.transportes.gov.br/api/3/action/package_show"
DATASET_ID = "registro-nacional-de-veiculos-automotores-renavam"
HEADERS = {"Authorization": TOKEN}

# --- Consulta o dataset ---
resp = requests.get(API_URL, params={"id": DATASET_ID}, headers=HEADERS)
resp.raise_for_status()
data = resp.json()["result"]

# --- Filtra CSV e ZIP ---
resources = [r for r in data["resources"] if r["format"].lower() in ("csv", "zip")]
print(f"📂 {len(resources)} recursos encontrados.")

# --- Função para baixar e processar ZIP em memória ---
def process_zip_from_url(url, output_dir):
    print(f"⬇️ Baixando ZIP {url.split('/')[-1]} ...")
    with requests.get(url, headers=HEADERS, stream=True) as r:
        r.raise_for_status()
        file_bytes = io.BytesIO()
        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB por vez
            file_bytes.write(chunk)
        file_bytes.seek(0)

    with ZipFile(file_bytes, "r") as zip_ref:
        for file_name in zip_ref.namelist():
            if "frota" in file_name.lower() and ("uf" in file_name.lower() or "tipo" in file_name.lower()):
                print(f"📄 Encontrado {file_name}")
                with zip_ref.open(file_name) as f:
                    try:
                        # Detecta automaticamente o separador ; ou |
                        first_line = f.readline().decode("latin1")
                        sep = ";" if ";" in first_line else "|"
                        f.seek(0)  # volta pro início
                        
                        # Lê em blocos pra evitar overload de memória
                        chunks = pd.read_csv(
                            f, 
                            sep=sep, 
                            encoding="latin1", 
                            engine="python", 
                            chunksize=200_000
                        )
                        out_file = output_dir / f"{Path(file_name).stem}.csv"

                        with open(out_file, "w", encoding="utf-8", newline="") as csv_out:
                            first_chunk = True
                            for chunk in chunks:
                                if first_chunk:
                                    chunk.to_csv(csv_out, index=False)
                                    first_chunk = False
                                else:
                                    chunk.to_csv(csv_out, index=False, header=False)
                        
                        print(f"✅ Extraído e salvo {out_file}")
                        return
                    except Exception as e:
                        print(f"⚠️ Erro lendo {file_name}: {e}")
                        continue

# --- Processa cada recurso ---
for r in resources:
    url = r["url"]
    fmt = r["format"].lower()

    if fmt == "csv":
        name = Path(r["name"]).with_suffix(".csv")
        out_file = DATA_DIR / name
        try:
            r_csv = requests.get(url, headers=HEADERS)
            r_csv.raise_for_status()
            with open(out_file, "wb") as f:
                f.write(r_csv.content)
            print(f"✅ Baixado {name}")
        except Exception as e:
            print(f"⚠️ Erro ao baixar {name}: {e}")

    elif fmt == "zip":
        process_zip_from_url(url, DATA_DIR)

print("🎉 Concluído com sucesso!")
