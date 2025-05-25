import json
import time
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args

# —————— Configuración ——————
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
KEYSPACE      = 'semapa_v9'
TABLE_INFRA   = 'infraestructura'
INPUT_FILE    = 'infraestructuras_generadas3.json'
CONCURRENCY   = 200

INSERT_CQL = f"""
INSERT INTO {TABLE_INFRA} (
    contrato_id, categoria, descripcion_categoria, nombre, email, telefono,
    ci_nit, razon_social, tipo_infraestructura, subalcaldia, distrito, zona,
    latitud, longitud, medidores
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

def safe_get(item, key, default=""):
    v = item.get(key)
    return v if v not in (None, "") else default

def chunked(lst, n):
    """Divide lista en trozos de tamaño n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    # 1) Carga de datos y construcción de la lista de parámetros
    print("→ Leyendo y transformando datos...", end="", flush=True)
    with open(INPUT_FILE, encoding='utf-8') as f:
        raw = json.load(f)

    params = []
    for item in raw:
        med = item.get("Medidores", [])
        if not isinstance(med, list):
            med = [str(med)]

        raw_distrito = item.get("Distrito", "")
        distrito_str = f"D{raw_distrito}" if raw_distrito != "" else ""

        params.append((
            safe_get(item, "ContratoID"),
            safe_get(item, "Categoria"),
            safe_get(item, "DescripcionCategoria"),
            safe_get(item, "Nombre"),
            safe_get(item, "Email"),
            safe_get(item, "Telefono"),
            int(safe_get(item, "CI/NIT", 0)),
            safe_get(item, "Razon Social"),
            safe_get(item, "Tipo Infraestructura"),
            safe_get(item, "SubAlcaldia").replace("\n", " ").strip(),
            distrito_str,
            safe_get(item, "Zona"),
            float(item.get("Latitud", 0.0)),
            float(item.get("Longitud", 0.0)),
            med
        ))
    total = len(params)
    print(f" hecho. {total} registros listos.")

    # 2) Conexión y preparación de la consulta
    print("→ Conectando a Cassandra...", flush=True)
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, load_balancing_policy=RoundRobinPolicy())
    session = cluster.connect(KEYSPACE)
    prepared = session.prepare(INSERT_CQL)

    # 3) Inserción concurrente por lotes
    print(f"→ Inyectando {total} registros en {TABLE_INFRA}...", flush=True)
    start = time.time()
    inserted = 0

    for batch in chunked(params, CONCURRENCY):
        execute_concurrent_with_args(session, prepared, batch, concurrency=CONCURRENCY)
        inserted += len(batch)
        print(f"\r   Registros insertados: {inserted}/{total}", end="", flush=True)
    print()  # salto de línea

    # 4) Tiempo total
    elapsed = time.time() - start
    m, s = divmod(int(elapsed), 60)
    print(f"\n✅ Inserción completada en {m}m{s}s: {inserted} registros.")

if __name__ == "__main__":
    main()
