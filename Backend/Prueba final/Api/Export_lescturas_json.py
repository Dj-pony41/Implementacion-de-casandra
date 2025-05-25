#!/usr/bin/env python3
# export_lecturas.py

import argparse
import json
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

def format_tarifa(v: float) -> str:
    return f"${v:.2f}"

def get_session():
    cluster = Cluster(['127.0.0.1'], protocol_version=4)
    session = cluster.connect('semapa_v9')
    session.row_factory = dict_factory
    return session

def export_lecturas(fecha_hora: str):
    # 1) Parsear la fecha
    naive = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M")
    fh = naive.replace(tzinfo=timezone.utc)

    session = get_session()

    # 2) Traer todas las lecturas para esa hora
    cql_lect = """
        SELECT codigo_medidor, modelo, estado, lectura,
               consumo_periodo, tarifa_usd, fecha_hora
        FROM lecturas_medidor
        WHERE fecha_hora = %s
        ALLOW FILTERING
    """
    lect_rows = session.execute(SimpleStatement(cql_lect), (fh,)).all()

    # 3) Agrupar lecturas por medidor
    lect_by_med = {}
    for r in lect_rows:
        lect_by_med.setdefault(r['codigo_medidor'], []).append(r)

    # 4) Cargar toda la infraestructura
    infra_rows = session.execute("""
        SELECT contrato_id, nombre, ci_nit, email, telefono,
               latitud, longitud, distrito, zona, medidores
        FROM infraestructura
    """).all()

    # 5) Construir el JSON de salida
    output = []
    for inf in infra_rows:
        cid = inf['contrato_id']
        medidores = inf.get('medidores') or []
        detalles = []
        for md in medidores:
            if md in lect_by_med:
                for r in lect_by_med[md]:
                    detalles.append({
                        "CodigoMedidor":  md,
                        "Modelo":         r.get('modelo', "Unknown"),
                        "Estado":         r.get('estado', "Unknown"),
                        "FechaHora":      r['fecha_hora'].strftime("%Y-%m-%d %H:%M"),
                        "Lectura":        r.get('lectura', 0),
                        "ConsumoPeriodo": r.get('consumo_periodo', 0),
                        "TarifaUSD":      format_tarifa(r.get('tarifa_usd', 0.0))
                    })

        if detalles:
            output.append({
                "ContratoID": cid,
                "Nombre":     inf.get('nombre', ""),
                "CI/NIT":     inf.get('ci_nit', 0),
                "Email":      inf.get('email', ""),
                "Telefono":   inf.get('telefono', ""),
                "Latitud":    inf.get('latitud', 0.0),
                "Longitud":   inf.get('longitud', 0.0),
                "Distrito":   inf.get('distrito', ""),
                "Zona":       inf.get('zona', ""),
                "Medidores":  detalles
            })

    # Cerrar sesión
    session.cluster.shutdown()
    return output

def main():
    parser = argparse.ArgumentParser(description="Exporta lecturas a JSON.")
    parser.add_argument(
        "--fecha_hora", "-f",
        required=True,
        help="Fecha y hora exacta: 'YYYY-MM-DD HH:MM'"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Ruta del fichero JSON de salida"
    )
    args = parser.parse_args()

    try:
        data = export_lecturas(args.fecha_hora)
        with open(args.output, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)
        print(f"✅ Exportado {len(data)} contratos con lecturas a {args.output}")
    except ValueError as ve:
        print(f"❌ Fecha inválida: {ve}")
    except Exception as e:
        print(f"🚨 Error: {e}")

if __name__ == "__main__":
    main()
