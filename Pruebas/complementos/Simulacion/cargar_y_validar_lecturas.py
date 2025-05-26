# -*- coding: utf-8 -*-
import os
import json
import uuid
import datetime
import platform
from cassandra.cluster import Cluster

# Conexión a Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('semapa_distribuidos')

# Carpeta con archivos JSON
directorio = 'lecturas'

errores_insertados = 0
lecturas_insertadas = 0
lectura_vista = set()

for archivo in os.listdir(directorio):
    if archivo.endswith('.json'):
        print(f"\n📁 Procesando archivo: {archivo}")
        ruta = os.path.join(directorio, archivo)
        with open(ruta, 'r', encoding='utf-8') as f:
            try:
                registros = json.load(f)
            except:
                print(f"❌ Error al leer: {archivo}")
                continue

            for item in registros:
                try:
                    cod = item['CodigoMedidor']
                    estado_txt = item['Estado']
                    estado = "1" if estado_txt == "Automatico (Bien)" else "2" if estado_txt == "Manual" else "X"
                    lectura = int(item['Lectura'])
                    tarifa = float(item['TarifaUSD'].replace('$',''))
                    fecha = datetime.datetime.strptime(item['FechaHora'], "%Y-%m-%d %H:%M")

                    # Insertar en tabla lectura
                    session.execute(
                        "INSERT INTO lectura (codigo_medidor, fecha_hora, antena, consumo_periodo, estado, lectura, modelo, tarifa) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            cod,
                            fecha,
                            int(item['Antena']),
                            int(item['ConsumoPeriodo']),
                            estado,
                            lectura,
                            item['Modelo'],
                            tarifa
                        )
                    )
                    lecturas_insertadas += 1

                    if lecturas_insertadas % 1000 == 0:
                        print(f"✅ {lecturas_insertadas} lecturas insertadas hasta ahora...")

                    # Validación de errores
                    if estado not in ("1", "2"):
                        descripcion = f"Estado inválido: {estado_txt}"
                        print(f"🟠 Error estado → Medidor: {cod} | Fecha: {fecha} | Estado: {estado_txt}")
                        session.execute(
                            "INSERT INTO errores_lectura (id, codigo_medidor, created_at, descripcion, estado, fecha_hora, lectura, tarifa) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                            (
                                uuid.uuid4(), cod, datetime.datetime.now(),
                                descripcion,
                                estado_txt, fecha, lectura, tarifa
                            )
                        )
                        errores_insertados += 1

                    clave = (cod, lectura)
                    if clave in lectura_vista:
                        descripcion = "Lectura duplicada (valor repetido)"
                        print(f"🔁 Duplicado → Medidor: {cod} | Lectura: {lectura} | Fecha: {fecha}")
                        session.execute(
                            "INSERT INTO errores_lectura (id, codigo_medidor, created_at, descripcion, estado, fecha_hora, lectura, tarifa) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                            (
                                uuid.uuid4(), cod, datetime.datetime.now(),
                                descripcion,
                                estado_txt, fecha, lectura, tarifa
                            )
                        )
                        errores_insertados += 1
                    else:
                        lectura_vista.add(clave)

                except Exception as e:
                    print(f"⚠ Error procesando registro en {archivo}: {e}")

# Resultado final
print(f"\n✅ Lecturas insertadas: {lecturas_insertadas}")
print(f"🚨 Errores registrados: {errores_insertados}")

# Señal sonora al terminar (solo Windows)
if platform.system() == "Windows":
    import winsound
    winsound.Beep(1000, 500)
