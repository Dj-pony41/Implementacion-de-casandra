# -*- coding: utf-8 -*-
import pandas as pd
import random
import uuid
import json
from faker import Faker

# ———————————————————————————————————————
# 0. Función punto-en-polígono (ray casting)
# ———————————————————————————————————————
def point_in_polygon(point, polygon):
    """
    point: (lon, lat)
    polygon: list de (lon, lat) en orden
    """
    x, y = point
    inside = False
    n = len(polygon)
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > y) != (yj > y)) and \
           (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside

# ———————————————————————————————————————
# 1. Carga del GeoJSON y obtención del único polígono
# ———————————————————————————————————————
with open("distritos.geojson", encoding="utf-8") as f:
    geo = json.load(f)

# Tomamos el primer (y único) Feature y su primer anillo de coordenadas
raw_coords = geo["features"][0]["geometry"]["coordinates"][0]
# Convertimos a lista de tuplas (lon, lat)
polygon = [(lon, lat) for lon, lat in raw_coords]

# ———————————————————————————————————————
# 2. Generador de punto aleatorio en polígono
# ———————————————————————————————————————
def generar_punto_en_poligono(polygon):
    lons = [p[0] for p in polygon]
    lats = [p[1] for p in polygon]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    while True:
        lon = random.uniform(min_lon, max_lon)
        lat = random.uniform(min_lat, max_lat)
        if point_in_polygon((lon, lat), polygon):
            return round(lat, 6), round(lon, 6)

# ———————————————————————————————————————
# 3. Lectura de Excel y configuración inicial
# ———————————————————————————————————————
fake = Faker('es_ES')
random.seed(42)

archivo = "Recursos Practica 5.xlsx"
xlsx = pd.ExcelFile(archivo)

# 3.1 Tarifario
tarifario_df = xlsx.parse('Tarifario')
tarifario_limpio = tarifario_df.iloc[1:10, [0, 1]]
tarifario_limpio.columns = ["DescripcionCategoria","Categoria"]
tarifario_limpio["DescripcionCategoria"].fillna(method="ffill", inplace=True)
tarifario_limpio.dropna(subset=["Categoria","DescripcionCategoria"], inplace=True)
categorias = tarifario_limpio.to_dict(orient="records")

# 3.2 Tipos de infraestructura
infraestructura_df = xlsx.parse('Infraestructuras')
tipos_infraestructura = infraestructura_df["Unnamed: 1"].dropna().tolist()

# 3.3 Distritos desde Excel
distritos_df = xlsx.parse('Distritos')
distritos_limpio = distritos_df.iloc[1:,[0,1,3]]
distritos_limpio.columns = ["SubAlcaldía","Distrito","Zona"]
distritos_limpio.dropna(subset=["SubAlcaldía","Distrito","Zona"], inplace=True)
distritos = distritos_limpio.to_dict(orient="records")

# ———————————————————————————————————————
# 4. Generación de personas
# ———————————————————————————————————————
personas = []
for _ in range(21000):
    personas.append({
        "Nombre": fake.name(),
        "Email": "medranoledezmamariajustina@gmail.com",
        "Telefono": "+591 67420354",
        "CI/NIT": fake.random_number(digits=8),
        "Razon Social": ""
    })
for _ in range(1400):
    personas.append({
        "Nombre": "",
        "Email": "medranoledezmamariajustina@gmail.com",
        "Telefono": "+591 67420354",
        "CI/NIT": fake.random_number(digits=8),
        "Razon Social": fake.company()
    })

infra_count = random.choices([1,2,3,4,5], weights=[40,30,15,10,5], k=len(personas))

# ———————————————————————————————————————
# 5. Generar infraestructuras con puntos en el polígono
# ———————————————————————————————————————
infraestructuras = []
codigo_contrato = 1

for idx, persona in enumerate(personas):
    for _ in range(infra_count[idx]):
        contrato_id = f"CT-{str(codigo_contrato).zfill(6)}"
        categoria = random.choice(categorias)
        tipo_infra = random.choice(tipos_infraestructura)
        ubic = random.choice(distritos)

        # Generamos la coordenada dentro del polígono único
        lat, lon = generar_punto_en_poligono(polygon)

        # Medidores aleatorios
        medidores = ['MD-'+uuid.uuid4().hex[:10].upper() for _ in range(random.randint(1,3))]

        infraestructuras.append({
            "ContratoID": contrato_id,
            "Categoria": categoria["Categoria"],
            "DescripcionCategoria": categoria["DescripcionCategoria"],
            "Nombre": persona["Nombre"],
            "Email": persona["Email"],
            "Telefono": persona["Telefono"],
            "CI/NIT": persona["CI/NIT"],
            "Razon Social": persona["Razon Social"],
            "Tipo Infraestructura": tipo_infra,
            "SubAlcaldía": ubic["SubAlcaldía"],
            "Distrito": ubic["Distrito"],
            "Zona": ubic["Zona"],
            "Latitud": lat,
            "Longitud": lon,
            "Medidores": medidores
        })
        codigo_contrato += 1

# ———————————————————————————————————————
# 6. Guardar a JSON
# ———————————————————————————————————————
with open("infraestructuras_generadas3.json", "w", encoding="utf-8") as f:
    json.dump(infraestructuras, f, ensure_ascii=False, indent=2)

print("✅ Archivo generado: infraestructuras_generadas3.json")
