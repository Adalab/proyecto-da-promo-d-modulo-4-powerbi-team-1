import requests
import pandas as pd
import time
import os
import pycountry

# Establecer rutas absolutas del proyecto
base_folder = "/Users/Elena1/Documents/GitHub/proyecto-da-promo-d-modulo-4-powerbi-team-1/raw_worldbank_data"
os.makedirs(base_folder, exist_ok=True)
final_folder = "/Users/Elena1/Documents/GitHub/proyecto-da-promo-d-modulo-4-powerbi-team-1/final_powerbi_data"
os.makedirs(final_folder, exist_ok=True)

# Indicadores organizados por categoría
indicadores_por_categoria = {
    "availability": [
        "ER.H2O.INTR.PC",
        "ER.H2O.INTR.K3",
    ],
    "water_stress": [
        "ER.H2O.FWTL.K3",
        "ER.H2O.FWST.ZS"
    ],
    "use_by_sector": [
        "ER.H2O.FWAG.ZS",
        "ER.H2O.FWIN.ZS",
        "ER.H2O.FWDM.ZS",
    ],
    "efficiency": [
        "ER.GDP.FWTL.M3.KD",
    ],
    "access_to_water": [
        "SH.H2O.BASW.ZS",
        "SH.H2O.SMDW.ZS",
    ],
    "climate_impact": [
        "AG.LND.IRIG.AG.ZS",
        "AG.LND.PRCP.MM"
    ],
    "industry_economy": [
        "NV.IND.TOTL.ZS",
        "NV.IND.MANF.ZS",
    ],
    "poverty_development": [
        "SI.POV.DDAY",
        "SI.POV.GINI"
    ]
}

start_year = 2000
end_year = 2021

def descargar_indicador(indicador):
    print(f"Descargando: {indicador}")
    url = f"http://api.worldbank.org/v2/country/all/indicator/{indicador}?date={start_year}:{end_year}&format=json&per_page=1000&language=en"
    resultados = []
    page = 1
    while True:
        response = requests.get(f"{url}&page={page}")
        data = response.json()
        if page == 1 and isinstance(data, list) and len(data) < 2:
            print(f"No data for indicator {indicador}")
            break
        registros = data[1]
        resultados.extend(registros)
        if page >= data[0]["pages"]:
            break
        page += 1
        time.sleep(0.2)
    filas = []
    for r in resultados:
        if r["countryiso3code"] and r["country"]["id"] not in [
            "WLD", "EAS", "LCN", "ARB", "ECA", "MEA", "NAC", "SAS",
            "SSF", "HIC", "LIC", "MIC", "LMC", "UMC"
        ]:
            filas.append({
                "Country": r["country"]["value"],
                "Country Code": r["countryiso3code"],
                "Indicator": r["indicator"]["value"],
                "Indicator Code": r["indicator"]["id"],
                "Year": int(r["date"]),
                "Value": r["value"]
            })
    df = pd.DataFrame(filas)
    return df

# Descargar y guardar por categoría
for categoria, indicadores in indicadores_por_categoria.items():
    categoria_folder = os.path.join(base_folder, categoria)
    os.makedirs(categoria_folder, exist_ok=True)
    for indicador in indicadores:
        df = descargar_indicador(indicador)
        if not df.empty:
            archivo = os.path.join(categoria_folder, f"{indicador}.csv")
            df.to_csv(archivo, index=False)
            print(f"Guardado en {archivo}\n")
        else:
            print(f"No se guardó ningún dato para {indicador}\n")

# Unir todos los CSV descargados en un único DataFrame
all_data = []
for categoria in indicadores_por_categoria:
    categoria_folder = os.path.join(base_folder, categoria)
    for file in os.listdir(categoria_folder):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(categoria_folder, file))
            df["Category"] = categoria
            all_data.append(df)

# Combinar todos los datos
if all_data:
    df_combined = pd.concat(all_data, ignore_index=True)

    # Guardar archivo final limpio
    final_path = os.path.join(final_folder, "worldbank_data_combined.csv")
    df_combined.to_csv(final_path, index=False)
    print(f"\nTodos los datos combinados han sido guardados en '{final_path}'")
else:
    print("\nNo se encontraron datos para combinar.")