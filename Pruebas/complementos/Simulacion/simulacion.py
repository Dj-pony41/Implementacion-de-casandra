# -*- coding: utf-8 -*-
import pandas as pd
import random
import uuid
import json
from faker import Faker

def point_in_polygon(point, polygon):
    x, y = point
    n = len(polygon)
    inside = False
    p1x, p1y = polygon[0]
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y)*(p2x - p1x)/(p2y - p1y)+p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

def generar_punto_en_poligono(poly):
    min_lat = min(p[1] for p in poly)
    max_lat = max(p[1] for p in poly)
    min_lng = min(p[0] for p in poly)
    max_lng = max(p[0] for p in poly)

    for _ in range(100):
        lat = round(random.uniform(min_lat, max_lat), 6)
        lng = round(random.uniform(min_lng, max_lng), 6)
        if point_in_polygon((lng, lat), poly):
            return lat, lng
    return min_lat, min_lng  # fallback

# ————— Inicialización —————
fake = Faker('es_ES')
random.seed(42)

# 1. Leer Excel
xlsx = pd.ExcelFile("Recursos Practica 5.xlsx")

# 1.a. Categorías
df_cat = xlsx.parse('Tarifario').iloc[1:10, [0,1]]
df_cat.columns = ["DescripcionCategoria","Categoria"]
df_cat["DescripcionCategoria"] = df_cat.ffill()["DescripcionCategoria"]
cats = df_cat.dropna(subset=["Categoria"]).to_dict('records')

# 1.b. Tipos de infraestructura
tipos = xlsx.parse('Infraestructuras')["Unnamed: 1"].dropna().tolist()

# 1.c. Distritos / SubAlcaldías / Zonas
df_dist = xlsx.parse('Distritos').iloc[1:, [0,1,3]]
df_dist.columns = ["SubAlcaldía","Distrito","Zona"]
df_dist = df_dist.dropna(subset=["SubAlcaldía"])
distritos = df_dist.to_dict('records')

# 2. Mapeo estático de zonas por distrito
distritos_zonas = {
    "D0": ["Cercado"],
    "D1": ["Queru Queru Alto","Aranjuez Alto"],
    # ... hasta D15 ...
    "D15": ["Khara Khara Arrumani","Pukara Grande Norte","Pukara Grande Sur",
            "Pukara Grande Oeste","Valle Hermoso Oeste","1° de Mayo","Muyurina","Las Cuadras"]
}

# 3. Cargar GeoJSON (polígonos por código D#)
with open("distritosCochabamba.geojson", encoding="utf-8") as f:
    geo = json.load(f)

poligonos_por_distrito = {}
for feat in geo["features"]:
    name = feat["properties"].get("name","").strip()
    geom = feat["geometry"]
    if geom["type"] == "Polygon":
        poligonos_por_distrito[name] = geom["coordinates"][0]
    elif geom["type"] == "MultiPolygon":
        poligonos_por_distrito[name] = geom["coordinates"][0][0]

# DEBUG: asegura que tienes D0, D1, ... en tu geojson
print("Polígonos disponibles:", sorted(poligonos_por_distrito.keys()))

# 4. Crear personas
personas = []
for _ in range(80000):
    personas.append({
        "Nombre": fake.name(),
        "Email": f"{fake.first_name().lower()}.{fake.last_name().lower()}{random.randint(10,999)}@gmail.com",
        "Telefono": f"+591 {random.randint(60000000,79999999)}",
        "CI/NIT": fake.random_number(8),
        "Razon Social": ""
    })
for _ in range(5000):
    emp = fake.company().replace(" ","").lower()
    personas.append({
        "Nombre": "",
        "Email": f"contacto.{emp}{random.randint(100,999)}@gmail.com",
        "Telefono": f"+591 {random.randint(60000000,79999999)}",
        "CI/NIT": fake.random_number(8),
        "Razon Social": fake.company()
    })

infra_count = random.choices([1,2,3,4,5], weights=[40,30,15,10,5], k=len(personas))

# 5. Generar infraestructuras
infraestructuras = []
ct = 1

for idx, persona in enumerate(personas):
    for _ in range(infra_count[idx]):
        # 5.a. Elijo Distrito y Zona
        dist_code = random.choice(list(distritos_zonas.keys()))  # e.g. "D2"
        zona      = random.choice(distritos_zonas[dist_code]).upper()

        # 5.b. Cojo siempre el polígono del distrito
        poly = poligonos_por_distrito.get(dist_code)
        if not poly:
            continue

        lat, lon = generar_punto_en_poligono(poly)

        # 5.c. Busco SubAlcaldía en tu Excel
        num = int(dist_code[1:])  # "D2" -> 2
        try:
            sub = next(d["SubAlcaldía"]
                       for d in distritos
                       if (str(d["Distrito"]).isdigit() and int(d["Distrito"])==num)
                          and d["Zona"].strip().upper()==zona)
        except StopIteration:
            # si no encuentra, salto
            continue

        cat = random.choice(cats)
        tipo = random.choice(tipos)
        meds = ["MD-"+uuid.uuid4().hex[:10].upper()
                for __ in range(random.randint(1,3))]

        infraestructuras.append({
            "ContratoID":           f"CT-{str(ct).zfill(6)}",
            "Categoria":            cat["Categoria"],
            "DescripcionCategoria": cat["DescripcionCategoria"],
            "Nombre":               persona["Nombre"],
            "Email":                persona["Email"],
            "Telefono":             persona["Telefono"],
            "CI/NIT":               persona["CI/NIT"],
            "Razon Social":         persona["Razon Social"],
            "Tipo Infraestructura": tipo,
            "SubAlcaldia":          sub,
            "Distrito":             dist_code,
            "Zona":                 zona,
            "Latitud":              lat,
            "Longitud":             lon,
            "Medidores":            meds
        })
        ct += 1

# 6. Guardar y mostrar total
with open("infraestructuras_generadas_v3.json", "w", encoding="utf-8") as f:
    json.dump(infraestructuras, f, ensure_ascii=False, indent=2)

print(f"✅ Generadas {len(infraestructuras)} infraestructuras en infraestructuras_generadas_v3.json")
