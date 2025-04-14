
import requests
import pandas as pd
import time
import os

base_folder = "raw_worldbank_data"
os.makedirs(base_folder, exist_ok=True)

# Indicadores organizados por categoría
indicadores_por_categoria = {
    "availability": [
        "ER.H2O.INTR.PC",  # Renewable water per capita
        "ER.H2O.INTR.K3",  # Total internal water
    ],
    "water_stress": [
        "ER.H2O.FWST.ZS",   # Annual freshwater withdrawals (% of internal resources)
        "ER.H2O.FWTL.K3"  # Total withdrawals
    ],
    "use_by_sector": [
        "ER.H2O.FWAG.ZS",  # Agricultural use
        "ER.H2O.FWIN.ZS",  # Industrial use
        "ER.H2O.FWDM.ZS",  # Domestic use
    ],
    "efficiency": [
        "ER.GDP.FWTL.M3.KD",  # Water productivity
    ],
    "access_to_water": [
        "SH.H2O.BASW.ZS",  # Basic drinking water access
        "SH.H2O.SMDW.ZS",  # Safely managed drinking water
    ],
    "climate_impact": [
        "AG.LND.IRIG.AG.ZS", # Irrigated land (% of total agricultural land)
        "AG.LND.PRCP.MM",  # Precipitation
    ],
    "industry_economy": [
        "NV.IND.TOTL.ZS",  # Industry value added (% GDP)
        "NV.IND.MANF.ZS",  # Manufacturing value added (% GDP)
    ],
    "poverty_development": [
        "SI.POV.DDAY",  # Poverty headcount ratio
        "SI.POV.GINI"  # Gini index
    ]
}

#Definimos los años que nos interesan
start_year = 2000
end_year = 2021

#Creamos la funcion para conectar a la API del banco mundial
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
                "Country Name": r["country"]["value"],
                "Country Code": r["countryiso3code"],
                "Indicator Name": r["indicator"]["value"],
                "Indicator Code": r["indicator"]["id"],
                "Year": int(r["date"]),
                "Value": r["value"]
            })

    df = pd.DataFrame(filas)
    return df

#darcargamos datos y guardamos por categoria
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
            print(f"No se guardaron datos para {indicador}\n")
