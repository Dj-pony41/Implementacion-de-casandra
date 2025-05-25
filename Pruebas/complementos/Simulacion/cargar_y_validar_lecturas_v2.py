import os
import json
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel

# —————— Configuración ——————
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
KEYSPACE     = 'semapa_v2'
TABLE_READ   = 'lecturas_medidor'
TABLE_ERROR  = 'errores_iot'
IN_DIR       = './lecturas'
BATCH_SIZE   = 100
NUM_PROCESSES = max(1, cpu_count() - 1)

# Statements CQL
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
SELECT_DUP_CQL = f"""
SELECT codigo_medidor 
  FROM {KEYSPACE}.{TABLE_READ}
 WHERE codigo_medidor = ? AND fecha_hora = ? LIMIT 1
"""

# Variables globales que inicializa cada worker
session = None
dup_stmt = None
read_stmt = None
err_stmt = None

def worker_init():
    """Inicializa sesión y statements para cada proceso."""
    global session, dup_stmt, read_stmt, err_stmt
    cluster = Cluster(
        CASSANDRA_CONTACT_POINTS,
        load_balancing_policy=RoundRobinPolicy()
    )
    session = cluster.connect(KEYSPACE)
    read_stmt = session.prepare(SELECT_DUP_CQL)
    dup_stmt  = read_stmt  # mismo stmt
    read_stmt = session.prepare(INSERT_READ_CQL)
    err_stmt  = session.prepare(INSERT_ERR_CQL)

def procesar_archivo(archivo):
    """Procesa un JSON: valida duplicados/estado, inserta errores y lecturas por lotes."""
    print(f"\n📁 Procesando archivo: {archivo}", flush=True)
    path = os.path.join(IN_DIR, archivo)
    try:
        with open(path, encoding='utf-8') as f:
            lecturas = json.load(f)
    except Exception as e:
        print(f"❌ No se pudo leer {archivo}: {e}", flush=True)
        return 0

    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    batch.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    inserted = 0
    processed = 0

    for rec in lecturas:
        processed += 1
        try:
            codigo     = rec["CodigoMedidor"]
            fecha_hora = datetime.strptime(rec["FechaHora"], "%Y-%m-%d %H:%M")
            estado     = rec.get("Estado", "").strip()
            lectura_v  = int(rec.get("Lectura", 0))

            # Duplicado?
            if session.execute(dup_stmt, (codigo, fecha_hora)).one():
                print(f"🔁 Duplicado → Medidor: {codigo} | Lectura: {lectura_v} | Fecha: {fecha_hora}", flush=True)
                session.execute(err_stmt, (codigo, fecha_hora, "DUPLICADO"))
                continue

            # Estado válido?
            if estado not in ("Automatico (Bien)", "Manual"):
                tipo_err = estado or "Sin estado"
                print(f"🟠 Error estado → Medidor: {codigo} | Fecha: {fecha_hora} | Estado: {tipo_err}", flush=True)
                session.execute(err_stmt, (codigo, fecha_hora, tipo_err))
                continue

            # Prepara para batch
            params = (
                codigo,
                fecha_hora,
                int(rec.get("Antena", 0)),
                rec.get("Modelo", ""),
                estado,
                lectura_v,
                int(rec.get("ConsumoPeriodo", 0)),
                float(str(rec.get("TarifaUSD", "$0")).replace("$", "")),
                datetime.strptime(rec["FechaInstalacion"], "%Y-%m-%d").date()
            )
            batch.add(read_stmt, params)
            inserted += 1

            # Envía batch
            if inserted % BATCH_SIZE == 0:
                session.execute(batch)
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                batch.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        except Exception:
            continue

    # Envía resto
    if len(batch) > 0:
        session.execute(batch)

    print(f"✅ {inserted} lecturas insertadas en {archivo} (procesadas: {processed})", flush=True)
    return inserted

def main():
    archivos = sorted(f for f in os.listdir(IN_DIR) if f.endswith('.json'))
    total_files = len(archivos)
    print(f"→ {total_files} archivos en '{IN_DIR}'\n", flush=True)

    t0 = time.time()
    total_inserted = 0

    with Pool(processes=NUM_PROCESSES, initializer=worker_init) as pool:
        for cnt in pool.imap(procesar_archivo, archivos):
            total_inserted += cnt
            print(f"✅ {total_inserted} lecturas insertadas hasta ahora...", flush=True)

    t_total = time.time() - t0
    m, s   = divmod(int(t_total), 60)
    print(f"\n🎉 Completado: {total_files} archivos → {total_inserted} lecturas insertadas en '{KEYSPACE}'", flush=True)
    print(f"⏱️ Tiempo total: {m} min {s} s", flush=True)

if __name__ == "__main__":
    main()
