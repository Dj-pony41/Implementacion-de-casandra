import json
import pandas as pd
import random
import uuid
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

# Cargar polígonos por zona desde archivo JSON
with open("plantilla_zonas_con_poligonos.json", "r", encoding="utf-8") as f:
    zonas_poligonos_data = json.load(f)

zonas_poligonos = {zona['nombre'].strip().upper(): zona['poligono'] for zona in zonas_poligonos_data['ZONAS']}

# Inicialización
fake = Faker('es_ES')
random.seed(42)

# Dummy DataFrames ya que no tenemos el archivo Excel original
categorias = [{"Categoria": "Doméstico", "DescripcionCategoria": "Uso residencial"},
              {"Categoria": "Comercial", "DescripcionCategoria": "Pequeño comercio"}]

tipos_infraestructura = ["Casa", "Departamento", "Negocio"]

distritos = [
    {"SubAlcaldía": "Centro", "Distrito": 1, "Zona": "CALA CALA"},
    {"SubAlcaldía": "Noroeste", "Distrito": 2, "Zona": "SARCOBAMBA"},
    {"SubAlcaldía": "Noreste", "Distrito": 3, "Zona": "TUPURAYA"}
]

# Generar personas naturales y jurídicas
personas = []
for _ in range(200):
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

for _ in range(50):
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

# Infraestructura por persona
infra_count = random.choices([1, 2], weights=[70, 30], k=len(personas))

# Generar infraestructuras respetando los polígonos por zona
infraestructuras = []
codigo_contrato = 1

for idx, persona in enumerate(personas):
    for _ in range(infra_count[idx]):
        contrato_id = f"CT-{str(codigo_contrato).zfill(6)}"
        categoria = random.choice(categorias)
        tipo_infra = random.choice(tipos_infraestructura)
        ubicacion = random.choice(distritos)
        zona_clean = str(ubicacion["Zona"]).strip().upper()

        # Obtener polígono y generar coordenadas válidas
        poligono = zonas_poligonos.get(zona_clean)
        if not poligono:
            continue  # saltar si no hay polígono para esta zona

        min_lat = min(p[0] for p in poligono)
        max_lat = max(p[0] for p in poligono)
        min_lng = min(p[1] for p in poligono)
        max_lng = max(p[1] for p in poligono)

        intentos = 0
        while True:
            intentos += 1
            lat = round(random.uniform(min_lat, max_lat), 6)
            lng = round(random.uniform(min_lng, max_lng), 6)
            if point_in_polygon((lat, lng), poligono):
                break
            if intentos > 100:
                lat, lng = min_lat, min_lng  # fallback para evitar loop infinito
                break

        num_medidores = random.randint(1, 3)
        medidores = ['MD-' + uuid.uuid4().hex[:10].upper() for _ in range(num_medidores)]

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
            "Distrito": int(ubicacion["Distrito"]),
            "Zona": ubicacion["Zona"],
            "Latitud": lat,
            "Longitud": lng,
            "Medidores": medidores
        })
        codigo_contrato += 1

# Guardar en archivo JSON
with open("infraestructuras_por_zona_delimitada.json", "w", encoding="utf-8") as f:
    json.dump(infraestructuras, f, ensure_ascii=False, indent=2)

import ace_tools as tools; tools.display_dataframe_to_user(name="Infraestructuras por Zona", dataframe=pd.DataFrame(infraestructuras))
