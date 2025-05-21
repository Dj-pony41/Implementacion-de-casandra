import os
import json
import argparse
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.io.asyncioreactor import AsyncioConnection
from datetime import datetime

# Función para obtener datos de Cassandra
def fetch_contratos(fecha_inicio: datetime, fecha_fin: datetime, host: str, port: int, keyspace: str):
    cluster = Cluster([host], port=port, connection_class=AsyncioConnection)
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory

    # Consulta contratos
    contratos = session.execute(
        """
        SELECT contrato_id AS ContratoID,
               nombre    AS Nombre,
               ci_nit    AS CI_NIT,
               email     AS Email,
               telefono  AS Telefono
        FROM contrato
        """
    ).all()

    result = []
    for c in contratos:
        infra = session.execute(
            "SELECT latitud AS Latitud, longitud AS Longitud, distrito AS Distrito, zona AS Zona "
            "FROM infraestructura WHERE contrato_id = %s", (c['ContratoID'],)
        ).one()
        # Lecturas
        medidores = session.execute(
            "SELECT codigo_medidor AS CodigoMedidor FROM medidor WHERE contrato_id = %s", (c['ContratoID'],)
        ).all()
        md_list = []
        for m in medidores:
            lects = session.execute(
                """
                SELECT codigo_medidor AS CodigoMedidor,
                       modelo          AS Modelo,
                       estado          AS Estado,
                       fecha_hora      AS FechaHora,
                       lectura         AS Lectura,
                       consumo_periodo AS ConsumoPeriodo,
                       tarifa          AS TarifaUSD
                FROM lectura
                WHERE codigo_medidor = %s
                  AND fecha_hora >= %s
                  AND fecha_hora <= %s
                """,
                (m['CodigoMedidor'], fecha_inicio, fecha_fin)
            ).all()
            md_list.extend(lects)
        c.update(infra)
        c['Medidores'] = md_list
        result.append(c)

    cluster.shutdown()
    return result

# Main
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Exporta contratos de Cassandra a JSON')
    parser.add_argument('--fecha_inicio', required=True, help='Fecha inicial (YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--fecha_fin', required=True, help='Fecha final   (YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--output', default='contratos.json', help='Archivo JSON de salida')
    parser.add_argument('--host', default=os.getenv('CASSANDRA_HOST','127.0.0.1'), help='Host Cassandra')
    parser.add_argument('--port', type=int, default=int(os.getenv('CASSANDRA_PORT',9042)), help='Puerto Cassandra')
    parser.add_argument('--keyspace', default=os.getenv('CASSANDRA_KEYSPACE','semapa_distribuidos'), help='Keyspace')
    args = parser.parse_args()

    # Parse fechas
    inicio = datetime.fromisoformat(args.fecha_inicio)
    fin    = datetime.fromisoformat(args.fecha_fin)

    data = fetch_contratos(inicio, fin, args.host, args.port, args.keyspace)

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(data, f, default=str, ensure_ascii=False, indent=2)

    print(f"Exportados {len(data)} contratos a {args.output}")
