# -*- coding: utf-8 -*-
import pandas as pd
import random
import uuid
import json
from faker import Faker
 
# Función para verificar si un punto está dentro de un polígono
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
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside
 
# Generar punto aleatorio dentro del polígono
def generar_punto_en_poligono(poligono):
    min_lat = min(p[1] for p in poligono)
    max_lat = max(p[1] for p in poligono)
    min_lng = min(p[0] for p in poligono)
    max_lng = max(p[0] for p in poligono)
 
    for _ in range(100):
        lat = round(random.uniform(min_lat, max_lat), 6)
        lng = round(random.uniform(min_lng, max_lng), 6)
        if point_in_polygon((lng, lat), poligono):
            return lat, lng
    return min_lat, min_lng
 
# Inicialización
fake = Faker('es_ES')
random.seed(42)
 
# Cargar Excel
archivo = "Recursos Practica 5.xlsx"
xlsx = pd.ExcelFile(archivo)
 
# 1. Categorías y descripciones
tarifario_df = xlsx.parse('Tarifario')
tarifario_limpio = tarifario_df.iloc[1:10, [0, 1]]
tarifario_limpio.columns = ["DescripcionCategoria", "Categoria"]
tarifario_limpio["DescripcionCategoria"] = tarifario_limpio["DescripcionCategoria"].fillna(method="ffill")
tarifario_limpio = tarifario_limpio.dropna(subset=["Categoria", "DescripcionCategoria"])
categorias = tarifario_limpio.to_dict(orient="records")
 
# 2. Tipos de infraestructura
infraestructura_df = xlsx.parse('Infraestructuras')
tipos_infraestructura = infraestructura_df["Unnamed: 1"].dropna().tolist()
 
# 3. SubAlcaldías, Zonas y Distritos
distritos_df = xlsx.parse('Distritos')
distritos_limpio = distritos_df.iloc[1:, [0, 1, 3]]
distritos_limpio.columns = ["SubAlcaldía", "Distrito", "Zona"]
distritos_limpio = distritos_limpio.dropna(subset=["SubAlcaldía", "Distrito", "Zona"])
distritos = distritos_limpio.to_dict(orient="records")
 
# 4. Mapeo de zonas por distrito
distritos_zonas = {
    "D1": ["QUERU QUERU ALTO", "ARANJUEZ ALTO"],
    "D2": ["CALA CALA", "SARCO"],
    "D3": ["TEMPORAL PAMPA", "MAYORAZGO"],
    "D4": ["MESADILLA", "CONDEBAMBA"],
    "D5": ["LA TEMIBLE CARA CARA", "VILLA BUSCH"],
    "D6": ["HIPODROMO", "SARCOBAMBA"],
    "D7": ["CHIQUICOLLO", "LA CHIMBA"],
    "D8": ["COÑA COÑA", "SUDESTE", "LA MAICA"],
    "D9": ["JAIHUAYCO", "ALALAY NORTE", "ALALAY SUD", "TICTI", "USPHA USPHA", "LACMA", "VALLE HERMOSO"],
    "D10": ["CALA CALA", "TUPURAYA"],
    "D11": ["HIPODROMO", "QUERU QUERU"],
    "D12": ["SARCO"],
    "D13": ["NOROESTE", "NORESTE"],
    "D14": ["LA MAICA", "COÑA COÑA", "SUDOESTE", "SUDESTE"],
    "D15": ["KHARA KHARA ARRUMANI", "PUKARA GRANDE NORTE", "PUKARA GRANDE SUR", "PUKARA GRANDE OESTE", "VALLE HERMOSO OESTE", "1° DE MAYO", "MUYURINA", "LAS CUADRAS"]
}
 
# 5. Cargar GeoJSON de distritos con polígonos
with open("distritosCochabamba.geojson", "r", encoding="utf-8") as f:
    geojson_data = json.load(f)
 
poligonos_por_distrito = {}
for feature in geojson_data['features']:
    name = feature['properties'].get('name', '').upper()
    geometry = feature['geometry']
    if geometry['type'] == 'Polygon':
        poligonos_por_distrito[name] = geometry['coordinates'][0]
    elif geometry['type'] == 'MultiPolygon':
        poligonos_por_distrito[name] = geometry['coordinates'][0][0]
 
# 6. Generar personas naturales y jurídicas
personas = []
for _ in range(80000):
    nombre = fake.first_name().lower()
    apellido = fake.last_name().lower()
    numero = random.randint(10, 999)
    email = f"{nombre}.{apellido}{numero}@gmail.com"
    telefono = f"+591 {random.randint(60000000, 79999999)}"
    personas.append({
        "Nombre": fake.name(),
        "Email": email,
        "Telefono": telefono,
        "CI/NIT": fake.random_number(digits=8),
        "Razon Social": ""
    })
 
for _ in range(5000):
    empresa = fake.company().replace(" ", "").lower()
    numero = random.randint(100, 999)
    email = f"contacto.{empresa}{numero}@gmail.com"
    telefono = f"+591 {random.randint(60000000, 79999999)}"
    personas.append({
        "Nombre": "",
        "Email": email,
        "Telefono": telefono,
        "CI/NIT": fake.random_number(digits=8),
        "Razon Social": fake.company()
    })
 
# 7. Infraestructuras por persona
infra_count = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5], k=len(personas))
 
# 8. Generar infraestructuras
infraestructuras = []
codigo_contrato = 1
codigo_medidor = 1
 
for idx, persona in enumerate(personas):
    for _ in range(infra_count[idx]):
        contrato_id = f"CT-{str(codigo_contrato).zfill(6)}"
        categoria = random.choice(categorias)
        tipo_infra = random.choice(tipos_infraestructura)
        ubicacion = random.choice(distritos)
        zona_clean = str(ubicacion["Zona"]).strip().upper()
 
        distrito_match = None
        for distrito, zonas in distritos_zonas.items():
            if zona_clean in zonas:
                distrito_match = distrito
                break
 
        poligono = poligonos_por_distrito.get(distrito_match)
        if not poligono:
            continue
 
        lat, lon = generar_punto_en_poligono(poligono)
 
        num_medidores = random.randint(1, 3)
        medidores = []
        for _ in range(num_medidores):
            medidores.append('MD-' + uuid.uuid4().hex[:10].upper())
            codigo_medidor += 1
 
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
            "SubAlcaldia": ubicacion["SubAlcaldía"],
            "Distrito": distrito_match or ubicacion["Distrito"],
            "Zona": ubicacion["Zona"],
            "Latitud": lat,
            "Longitud": lon,
            "Medidores": medidores
        })
 
        codigo_contrato += 1
 
# 9. Guardar en JSON
with open("infraestructuras_generadas.json", "w", encoding="utf-8") as f:
    json.dump(infraestructuras, f, ensure_ascii=False, indent=2)
 
print("✅ Archivo generado: infraestructuras_generadas.json")