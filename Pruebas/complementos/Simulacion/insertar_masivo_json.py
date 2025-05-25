# -*- coding: utf-8 -*-
import os
os.environ["CASS_DRIVER_NO_CYTHON"] = "1"

import ijson
from cassandra.io.twistedreactor import TwistedConnection
from cassandra.cluster import Cluster

# Conexión a Cassandra
cluster = Cluster(
    ['127.0.0.1'],
    port=9042,
    connection_class=TwistedConnection
)
session = cluster.connect('semapa_distribuidos')

# Consultas preparadas
insert_contrato = session.prepare("""
    INSERT INTO contrato (
        contrato_id, ci_nit, email, nombre, razon_social, telefono
    ) VALUES (?, ?, ?, ?, ?, ?)
""")

insert_infraestructura = session.prepare("""
    INSERT INTO infraestructura (
        contrato_id, distrito, latitud, longitud,
        subalcaldia, tipo_infraestructura, zona
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
""")

insert_medidor = session.prepare("""
    INSERT INTO medidor (
        codigo_medidor, contrato_id
    ) VALUES (?, ?)
""")

# Procesar archivo en modo streaming
with open('infraestructuras_generadas.json', 'r', encoding='utf-8') as f:
    registros = ijson.items(f, 'item')

    count = 0
    for item in registros:
        contrato_id = item['ContratoID']

        # Insertar en contrato
        session.execute(insert_contrato, (
            contrato_id,
            int(item['CI/NIT']),
            item['Email'],
            item['Nombre'],
            item.get('Razon Social', ''),
            item['Telefono']
        ))

        # Insertar en infraestructura
        session.execute(insert_infraestructura, (
            contrato_id,
            item['Distrito'],
            float(item['Latitud']),
            float(item['Longitud']),
            item['SubAlcaldia'],
            item['Tipo Infraestructura'],
            item['Zona']
        ))

        # Insertar todos los medidores
        for medidor in item['Medidores']:
            session.execute(insert_medidor, (
                medidor,
                contrato_id
            ))

        count += 1
        if count % 1000 == 0:
            print(f"✅ {count} registros procesados...")

print("🎉 Inserción masiva finalizada correctamente.")
