import json
import random

# Coordenadas centrales aproximadas de Cochabamba centro
center_lat = -17.393
center_lng = -66.157

# Generar 10000 puntos con variación aleatoria (radio más grande)
data = []
for i in range(1, 100001):
    # Aumenté el rango de variación de 0.02 a 0.1 (5 veces más grande)
    lat = center_lat + random.uniform(-0.8, 0.8)
    lng = center_lng + random.uniform(-0.8, 0.8)
    consumo = random.randint(30, 1000)
    data.append({
        "id": str(i),
        "lat": round(lat, 6),
        "lng": round(lng, 6),
        "consumo": consumo
    })

# Guardar como archivo JSON
output_path = "heatmapData_extenso_10000.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

output_path