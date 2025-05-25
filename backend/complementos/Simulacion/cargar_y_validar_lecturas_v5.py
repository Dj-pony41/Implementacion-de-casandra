import os
import json
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args
from pybloom_live import BloomFilter  # pip install pybloom-live

# —————— Configuración ——————
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
KEYSPACE     = 'semapa_v6'
TABLE_READ   = 'lecturas_medidor'
TABLE_ERROR  = 'errores_iot'
IN_DIR       = './lecturas'
CONCURRENCY  = 200
NUM_PROCESSES = max(1, cpu_count() - 1)

# CQL
INSERT_READ_CQL = f"""
INSERT INTO {KEYSPACE}.{TABLE_READ} (
    codigo_medidor, fecha_hora, antena, modelo, estado,
    lectura, consumo_periodo, tarifa_usd, fecha_instalacion
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
INSERT_ERR_CQL = f"""
INSERT INTO {KEYSPACE}.{TABLE_ERROR} (
    codigo_medidor, fecha_hora, tipo_error
) VALUES (?, ?, ?)
"""

def init_worker():
    """Inicializa BloomFilter en cada worker (no Cassandra)."""
    pass

def procesar_archivo(archivo):
    """Lee un JSON y genera listas de params para lecturas y errores."""
    bloom = BloomFilter(capacity=1_000_000, error_rate=0.001)
    inserts_read = []
    inserts_err  = []

    path = os.path.join(IN_DIR, archivo)
    try:
        data = json.load(open(path, encoding='utf-8'))
    except:
        return inserts_read, inserts_err

    for rec in data:
        try:
            cod = rec["CodigoMedidor"]
            fh  = datetime.strptime(rec["FechaHora"], "%Y-%m-%d %H:%M")
            key = f"{cod}|{fh.isoformat()}"

            # duplicado en este filtro local?
            if key in bloom:
                inserts_err.append((cod, fh, "DUPLICADO"))
                continue
            bloom.add(key)

            estado = rec.get("Estado","").strip()
            if estado not in ("Automatico (Bien)", "Manual"):
                inserts_err.append((cod, fh, estado or "Sin estado"))
                continue

            # lectura válida
            inserts_read.append((
                cod, fh,
                int(rec.get("Antena",0)),
                rec.get("Modelo",""),
                estado,
                int(rec.get("Lectura",0)),
                int(rec.get("ConsumoPeriodo",0)),
                float(str(rec.get("TarifaUSD","$0")).replace("$","")),
                datetime.strptime(rec["FechaInstalacion"],"%Y-%m-%d").date()
            ))
        except:
            # si falla parseo, registra como error genérico
            try:
                inserts_err.append((rec.get("CodigoMedidor"), fh, "PARSE_ERROR"))
            except:
                pass

    return inserts_read, inserts_err

def chunked(lst, n):
    """Divide la lista lst en sublistas de tamaño n."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    archivos = sorted(f for f in os.listdir(IN_DIR) if f.endswith('.json'))
    total = len(archivos)
    print(f"→ {total} archivos a procesar")

    t0 = time.time()
    all_reads = []
    all_errs  = []

    # Contador de archivos procesados
    file_count = 0

    # 1) Primero, parsear y validar TODOS los archivos en paralelo
    with Pool(NUM_PROCESSES, initializer=init_worker) as pool:
        for reads, errs in pool.imap_unordered(procesar_archivo, archivos):
            file_count += 1
            all_reads.extend(reads)
            all_errs.extend(errs)
            # Informe de progreso de parseo
            print(
                f"\r✅ {len(all_reads)} lecturas válidas, "
                f"{len(all_errs)} errores, "
                f"archivos procesados: {file_count}/{total}",
                end=''
            )

    print("\n→ Conectando a Cassandra para insertar...")

    # 2) Ahora conectamos en el main y preparamos los statements
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, load_balancing_policy=RoundRobinPolicy())
    session = cluster.connect(KEYSPACE)
    read_ps = session.prepare(INSERT_READ_CQL)
    err_ps  = session.prepare(INSERT_ERR_CQL)

    # 3) Bulk insert concurrente con indicador de progreso
    # Inserción de lecturas
    total_reads = len(all_reads)
    inserted_reads = 0
    print(f"→ Inyectando {total_reads} lecturas en Cassandra...")
    for batch in chunked(all_reads, CONCURRENCY):
        execute_concurrent_with_args(session, read_ps, batch, concurrency=CONCURRENCY)
        inserted_reads += len(batch)
        print(f"\r   Lecturas insertadas: {inserted_reads}/{total_reads}", end='')
    print()  # salto de línea tras terminar las lecturas

    # Inserción de errores
    total_errs = len(all_errs)
    inserted_errs = 0
    print(f"→ Inyectando {total_errs} errores en Cassandra...")
    for batch in chunked(all_errs, CONCURRENCY):
        execute_concurrent_with_args(session, err_ps, batch, concurrency=CONCURRENCY)
        inserted_errs += len(batch)
        print(f"\r   Errores insertados: {inserted_errs}/{total_errs}", end='')
    print()  # salto de línea tras terminar los errores

    elapsed = time.time() - t0
    m, s = divmod(int(elapsed), 60)
    print(f"\n🎉 ¡Hecho en {m}m{s}s! Insertadas {inserted_reads} lecturas y {inserted_errs} errores.")

if __name__=="__main__":
    main()
