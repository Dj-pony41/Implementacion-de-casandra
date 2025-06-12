import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
 
fake = Faker()
random.seed(42)
start_date = datetime(2025, 4, 1)
end_date = datetime.now()
 
# Rutas
archivo_excel = "Recursos Practica 5.xlsx"
archivo_json = "infraestructuras_generadas3.json"
salida_dir = "lecturas2"
os.makedirs(salida_dir, exist_ok=True)
 
# Cargar infraestructuras
with open(archivo_json, "r", encoding="utf-8") as f:
    infraestructuras = json.load(f)
 
# Cargar errores y modelos
xlsx = pd.ExcelFile(archivo_excel)
errores_df = xlsx.parse("ErroresIOT")
errores_iot = errores_df["Descripcion"].dropna().tolist()
 
modelos_df = xlsx.parse("ModeloMedidores")
modelos = modelos_df["Modelo / Referencia"].dropna().tolist()
 
# Parámetros de escala para tarifa
min_tarifa = 16.74
max_tarifa = 145.98
consumo_min = 0
consumo_max = 1300  # Límite máximo realista del rango generado
 
# Generar lecturas por contrato
for infra in infraestructuras:
    contrato_id = infra["ContratoID"]
 
    # Detener al llegar al contrato CT-000100
    if contrato_id == "CT-000101":
        break
 
    categoria = infra["Categoria"].strip().upper()
    es_residencial = "residencial" in infra["DescripcionCategoria"].lower()
 
    fecha_instalacion = fake.date_between_dates(
        date_start=datetime(2020, 1, 1),
        date_end=datetime(2025, 3, 1)
    )
 
    lecturas = []
    acumulados = {}
 
    for medidor in infra["Medidores"]:
        acumulados[medidor] = 0
 
        for d in range((end_date - start_date).days + 1):
            fecha_base = start_date + timedelta(days=d)
            if fecha_base.date() < fecha_instalacion:
                continue
 
            horarios = [("00:00", (0, 1300)), ("08:00", (0, 380)), ("16:00", (0, 190))]
            for hora, rango in horarios:
                consumo_periodo = random.randint(*rango) if es_residencial else random.randint(0, 250)
                acumulados[medidor] += consumo_periodo
 
                estado = "Automatico (Bien)"
                if random.random() < 0.005:
                    estado = random.choice(errores_iot)
 
                # Escalar consumo a tarifa entre min y max
                tarifa_estim = min_tarifa + ((consumo_periodo - consumo_min) / (consumo_max - consumo_min)) * (max_tarifa - min_tarifa)
                tarifa_estim = max(min_tarifa, min(max_tarifa, round(tarifa_estim, 2)))  # Asegura estar en rango
 
                lectura = {
                    "CodigoMedidor": medidor,
                    "Antena": random.randint(1, 5),
                    "Modelo": random.choice(modelos),
                    "Estado": estado,
                    "FechaHora": "{} {}".format(fecha_base.strftime('%Y-%m-%d'), hora),
                    "Lectura": acumulados[medidor],
                    "ConsumoPeriodo": consumo_periodo,
                    "TarifaUSD": f"${tarifa_estim:.2f}",
                    "FechaInstalacion": fecha_instalacion.strftime('%Y-%m-%d')
                }
                lecturas.append(lectura)
 
    # Agregar 0.07% duplicados
    duplicados = random.sample(lecturas, k=int(len(lecturas) * 0.0007))
    lecturas.extend(duplicados)
 
    # Guardar archivo por contrato
    salida_path = os.path.join(salida_dir, f"lecturas_{contrato_id}.json")
    with open(salida_path, "w", encoding="utf-8") as f:
        json.dump(lecturas, f, ensure_ascii=False, indent=2)
 
print("✅ Generación completa hasta CT-000100")