#!/usr/bin/env python3
"""
Script para exportar datos de contratos desde Cassandra a un JSON para una hora fija.
Ejecuta:
  python export_contratos.py --fecha YYYY-MM-DDTHH:MM:SS --output contratos.json
"""
import os
import json
import argparse
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.io.twistedreactor import TwistedConnection


def fetch_contratos(fecha: datetime, host: str, port: int, keyspace: str):
    """Conecta a Cassandra y retorna la lista de contratos con su infraestructura y lecturas en la hora exacta."""
    cluster = Cluster([host], port=port, connection_class=TwistedConnection)
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory

    # 1) Consulta contratos
    contratos = session.execute(
        """
        SELECT contrato_id, nombre, ci_nit, email, telefono
        FROM contrato
        """
    ).all()

    # 2) Pre-cargar medidores agrupados
    medidor_rows = session.execute(
        "SELECT contrato_id, codigo_medidor FROM medidor"
    ).all()
    med_map = {}
    for mr in medidor_rows:
        cid = mr.get('contrato_id')
        med_map.setdefault(cid, []).append(mr.get('codigo_medidor'))

    result = []
    for c in contratos:
        contrato_id = c.get('contrato_id')
        nombre      = c.get('nombre')
        ci_nit      = c.get('ci_nit')
        email       = c.get('email')
        telefono    = c.get('telefono')

        # Infraestructura
        infra = session.execute(
            "SELECT latitud, longitud, distrito, zona FROM infraestructura WHERE contrato_id = %s", (contrato_id,)
        ).one() or {}

        latitud  = infra.get('latitud')
        longitud = infra.get('longitud')
        distrito = infra.get('distrito')
        zona     = infra.get('zona')

        # Medidores
        medidores = med_map.get(contrato_id, [])
        md_list = []
        for codigo in medidores:
            lects = session.execute(
                """
                SELECT codigo_medidor  AS CodigoMedidor,
                       modelo          AS Modelo,
                       estado          AS Estado,
                       fecha_hora      AS FechaHora,
                       lectura         AS Lectura,
                       consumo_periodo AS ConsumoPeriodo,
                       tarifa          AS TarifaUSD
                FROM lectura
                WHERE codigo_medidor = %s
                  AND fecha_hora = %s
                ALLOW FILTERING
                """,
                (codigo, fecha)
            ).all()
            md_list.extend(lects)

        # Armar registro
        record = {
            'ContratoID': contrato_id,
            'Nombre':     nombre,
            'CI_NIT':     ci_nit,
            'Email':      email,
            'Telefono':   telefono,
            'Latitud':    latitud,
            'Longitud':   longitud,
            'Distrito':   distrito,
            'Zona':       zona,
            'Medidores':  md_list
        }
        result.append(record)

    cluster.shutdown()
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Exporta contratos de Cassandra a JSON para una hora fija')
    parser.add_argument('--fecha', required=True, help='Fecha y hora exacta (YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--output', default='contratos.json', help='Archivo JSON de salida')
    parser.add_argument('--host', default=os.getenv('CASSANDRA_HOST','127.0.0.1'), help='Host de Cassandra')
    parser.add_argument('--port', type=int, default=int(os.getenv('CASSANDRA_PORT',9042)), help='Puerto de Cassandra')
    parser.add_argument('--keyspace', default=os.getenv('CASSANDRA_KEYSPACE','semapa_distribuidos'), help='Keyspace a usar')
    args = parser.parse_args()

    # Parse fecha
    fecha = datetime.fromisoformat(args.fecha)
    data = fetch_contratos(fecha, args.host, args.port, args.keyspace)

    # Guardar en JSON
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(data, f, default=str, ensure_ascii=False, indent=2)

    print(f"Exportados {len(data)} contratos a {args.output}")