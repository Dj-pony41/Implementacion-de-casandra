from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime, timezone
from typing import List
from fastapi.middleware.cors import CORSMiddleware
import logging

# --------------------------------------------
# Configuración básica
# --------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Semapa API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------
# Modelos Pydantic
# --------------------------------------------
class MedidorResponse(BaseModel):
    CodigoMedidor: str
    Modelo: str
    Estado: str
    FechaHora: str
    Lectura: int
    ConsumoPeriodo: int
    TarifaUSD: str

class ContratoResponse(BaseModel):
    ContratoID: str
    Nombre: str
    CI_NIT: int
    Email: str
    Telefono: str
    Latitud: float
    Longitud: float
    Distrito: str
    Zona: str
    Medidores: List[str]

class ContratoDetalleResponse(BaseModel):
    ContratoID: str
    Nombre: str
    CI_NIT: int
    Email: str
    Telefono: str
    Latitud: float
    Longitud: float
    Distrito: str
    Zona: str
    Medidores: List[MedidorResponse]

# --------------------------------------------
# Conexión Cassandra y consultas preparadas
# --------------------------------------------
cluster = Cluster(['127.0.0.1'], protocol_version=4)
session = cluster.connect('semapa_v9')
session.row_factory = dict_factory

stmt_infra_limit = session.prepare("""
    SELECT contrato_id, nombre, ci_nit, email, telefono,
           latitud, longitud, distrito, zona, medidores
      FROM infraestructura
     WHERE latitud  >= ?
       AND latitud  <= ?
       AND longitud >= ?
       AND longitud <= ?
     LIMIT ?
     ALLOW FILTERING
""")

stmt_lect_by_codes = session.prepare("""
    SELECT codigo_medidor, modelo, estado, lectura, consumo_periodo, tarifa_usd, fecha_hora
      FROM lecturas_medidor
     WHERE fecha_hora = ?
       AND codigo_medidor IN ?
     ALLOW FILTERING
""")

stmt_infra_by_id = session.prepare("""
    SELECT contrato_id, nombre, ci_nit, email, telefono,
           latitud, longitud, distrito, zona, medidores
      FROM infraestructura
     WHERE contrato_id = ?
""")

stmt_infra_by_name = session.prepare("""
    SELECT contrato_id, nombre, ci_nit, email, telefono,
           latitud, longitud, distrito, zona, medidores
      FROM infraestructura
     WHERE nombre = ?
     ALLOW FILTERING
""")

stmt_infra_all = session.prepare("""
    SELECT contrato_id, nombre, ci_nit, email, telefono,
           latitud, longitud, distrito, zona, medidores
      FROM infraestructura
     ALLOW FILTERING
""")

# --------------------------------------------
# Utilidad
# --------------------------------------------
def format_tarifa(v: float) -> str:
    return f"${v:.2f}"

# --------------------------------------------
# /lecturas: Solo estructuras sin lecturas
# --------------------------------------------
@app.get("/lecturas", response_model=List[ContratoResponse])
def lecturas(
    lat_min: float = Query(...),
    lat_max: float = Query(...),
    lon_min: float = Query(...),
    lon_max: float = Query(...),
    record_limit: int = Query(...)
):
    try:
        infra_rows = session.execute(
            stmt_infra_limit,
            (lat_min, lat_max, lon_min, lon_max, record_limit)
        ).all()

        result = []
        for inf in infra_rows:
            result.append(ContratoResponse(
                ContratoID=inf['contrato_id'],
                Nombre=inf['nombre'],
                CI_NIT=inf['ci_nit'],
                Email=inf['email'],
                Telefono=inf['telefono'],
                Latitud=inf['latitud'],
                Longitud=inf['longitud'],
                Distrito=inf['distrito'],
                Zona=inf['zona'],
                Medidores=inf.get('medidores') or []
            ))
        return result

    except Exception as e:
        logger.error(f"Error en /lecturas: {e}", exc_info=True)
        raise HTTPException(500, "Error interno del servidor")

# --------------------------------------------
# /lecturas/buscar: Detalle con lecturas (por contrato o nombre exacto)
# --------------------------------------------
@app.get("/lecturas/buscar", response_model=ContratoDetalleResponse)
def buscar(
    fecha_hora: str = Query(...),
    q: str = Query(...)
):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(400, "Formato inválido de fecha_hora")

    infra = session.execute(stmt_infra_by_id, (q,)).one()
    if not infra:
        infra = session.execute(stmt_infra_by_name, (q,)).one()
    if not infra:
        raise HTTPException(404, f"No se encontró infraestructura para '{q}'")

    meds = infra.get('medidores') or []
    if not meds:
        raise HTTPException(404, f"No hay medidores asociados a '{q}'")

    rows = session.execute(stmt_lect_by_codes, (fh, meds)).all()
    if not rows:
        raise HTTPException(404, f"No hay lecturas en {fecha_hora} para '{q}'")

    lect_by_med = {}
    for r in rows:
        lect_by_med.setdefault(r['codigo_medidor'], []).append(r)

    lista_med = []
    for md in meds:
        if md in lect_by_med:
            for r in lect_by_med[md]:
                lista_med.append(MedidorResponse(
                    CodigoMedidor=md,
                    Modelo=r.get('modelo') or "Unknown",
                    Estado=r.get('estado') or "Unknown",
                    FechaHora=r['fecha_hora'].strftime("%Y-%m-%d %H:%M"),
                    Lectura=r.get('lectura') or 0,
                    ConsumoPeriodo=r.get('consumo_periodo') or 0,
                    TarifaUSD=format_tarifa(r.get('tarifa_usd') or 0.0)
                ))

    return ContratoDetalleResponse(
        ContratoID=infra['contrato_id'],
        Nombre=infra['nombre'],
        CI_NIT=infra['ci_nit'],
        Email=infra['email'],
        Telefono=infra['telefono'],
        Latitud=infra['latitud'],
        Longitud=infra['longitud'],
        Distrito=infra['distrito'],
        Zona=infra['zona'],
        Medidores=lista_med
    )

# --------------------------------------------
# /lecturas/identificar: búsqueda por contrato, nombre o código medidor
# --------------------------------------------
@app.get("/lecturas/identificar", response_model=List[ContratoDetalleResponse])
def identificar(
    fecha_hora: str = Query(...),
    q: str = Query(...)
):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(400, "Formato inválido de fecha_hora")

    resultados = []
    contr_found = session.execute(stmt_infra_by_id, (q,)).one()
    if contr_found:
        contratos = [contr_found]
    else:
        contratos = session.execute(stmt_infra_by_name, (q,)).all()
        if not contratos:
            contratos = session.execute(stmt_infra_all).all()
            contratos = [c for c in contratos if q in (c.get("medidores") or [])]

    for inf in contratos:
        meds = inf.get('medidores') or []
        if not meds:
            continue

        rows = session.execute(stmt_lect_by_codes, (fh, meds)).all()
        if not rows:
            continue

        lect_by_med = {}
        for r in rows:
            lect_by_med.setdefault(r['codigo_medidor'], []).append(r)

        lista_med = []
        for md in meds:
            if md in lect_by_med:
                for r in lect_by_med[md]:
                    lista_med.append(MedidorResponse(
                        CodigoMedidor=md,
                        Modelo=r.get('modelo') or "Unknown",
                        Estado=r.get('estado') or "Unknown",
                        FechaHora=r['fecha_hora'].strftime("%Y-%m-%d %H:%M"),
                        Lectura=r.get('lectura') or 0,
                        ConsumoPeriodo=r.get('consumo_periodo') or 0,
                        TarifaUSD=format_tarifa(r.get('tarifa_usd') or 0.0)
                    ))

        resultados.append(ContratoDetalleResponse(
            ContratoID=inf['contrato_id'],
            Nombre=inf['nombre'],
            CI_NIT=inf['ci_nit'],
            Email=inf['email'],
            Telefono=inf['telefono'],
            Latitud=inf['latitud'],
            Longitud=inf['longitud'],
            Distrito=inf['distrito'],
            Zona=inf['zona'],
            Medidores=lista_med
        ))

    if not resultados:
        raise HTTPException(404, f"No se encontró ningún contrato relacionado con '{q}'")

    return resultados


from cassandra.query import SimpleStatement

@app.get("/dashboard/consumo_total")
def consumo_total(fecha_hora: str = Query(...)):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        cql = SimpleStatement("""
            SELECT consumo_periodo FROM lecturas_medidor WHERE fecha_hora = %s ALLOW FILTERING
        """)
        rows = session.execute(cql, (fh,))
        total = sum(r['consumo_periodo'] or 0 for r in rows)
        return {"consumo_total": total}
    except Exception as e:
        logger.error(f"Error en /dashboard/consumo_total: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al calcular consumo total.")

@app.get("/dashboard/medidores_reportando")
def medidores_reportando(fecha_hora: str = Query(...)):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        cql = SimpleStatement("""
            SELECT codigo_medidor FROM lecturas_medidor WHERE fecha_hora = %s ALLOW FILTERING
        """)
        rows = session.execute(cql, (fh,))
        count = len(set(r['codigo_medidor'] for r in rows))
        return {"medidores_reportando": count}
    except Exception as e:
        logger.error(f"Error en /dashboard/medidores_reportando: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al contar medidores reportando.")

@app.get("/dashboard/medidores_con_errores")
def medidores_con_errores(fecha_hora: str = Query(...)):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        cql = SimpleStatement("""
            SELECT codigo_medidor FROM errores_iot WHERE fecha_hora = %s ALLOW FILTERING
        """)
        rows = session.execute(cql, (fh,))
        count = len(set(r['codigo_medidor'] for r in rows))
        return {"medidores_con_errores": count}
    except Exception as e:
        logger.error(f"Error en /dashboard/medidores_con_errores: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al contar medidores con errores.")

@app.get("/dashboard/consumo_por_zona")
def consumo_por_zona_opt(fecha_hora: str = Query(...)):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

        # 1. Todas las lecturas a esa hora
        cql = SimpleStatement("""
            SELECT codigo_medidor, consumo_periodo FROM lecturas_medidor WHERE fecha_hora = %s ALLOW FILTERING
        """)
        lecturas = session.execute(cql, (fh,))
        consumo_por_medidor = {r['codigo_medidor']: r['consumo_periodo'] or 0 for r in lecturas}

        # 2. Infraestructura con zonas
        infra_all = session.execute(stmt_infra_all).all()
        zona_totales = {}

        for inf in infra_all:
            zona = inf.get("zona", "SIN_ZONA")
            for med in inf.get("medidores") or []:
                if med in consumo_por_medidor:
                    zona_totales[zona] = zona_totales.get(zona, 0) + consumo_por_medidor[med]

        return zona_totales

    except Exception as e:
        logger.error(f"Error en /dashboard/consumo_por_zona (opt): {e}", exc_info=True)
        raise HTTPException(500, "Error interno al calcular consumo por zona.")


@app.get("/dashboard/consumo_promedio")
def consumo_promedio(fecha_hora: str = Query(...)):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        cql = SimpleStatement("""
            SELECT consumo_periodo FROM lecturas_medidor WHERE fecha_hora = %s ALLOW FILTERING
        """)
        rows = session.execute(cql, (fh,))
        consumos = [r['consumo_periodo'] or 0 for r in rows]
        promedio = sum(consumos) / len(consumos) if consumos else 0
        return {"consumo_promedio": round(promedio, 2)}
    except Exception as e:
        logger.error(f"Error en /dashboard/consumo_promedio: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al calcular consumo promedio.")
    

from collections import defaultdict
from cassandra.query import SimpleStatement

@app.get("/dashboard/consumo_diario")
def consumo_diario():
    """
    Devuelve el consumo total de los últimos 15 días agrupado por fecha (sin filtrar por zona).
    """
    try:
        cql = SimpleStatement("""
            SELECT fecha_hora, consumo_periodo FROM lecturas_medidor ALLOW FILTERING
        """)
        rows = session.execute(cql)

        consumo_por_fecha = defaultdict(int)
        for r in rows:
            fecha = r["fecha_hora"].date().isoformat()
            consumo_por_fecha[fecha] += r.get("consumo_periodo", 0)

        # Ordenar por fecha descendente y tomar los últimos 15 días
        ultimos_15_dias = sorted(consumo_por_fecha.items(), key=lambda x: x[0], reverse=True)[:15]

        # Devolver ordenado de forma ascendente para la gráfica
        return [{"fecha": fecha, "consumo": consumo} for fecha, consumo in reversed(ultimos_15_dias)]

    except Exception as e:
        logger.error(f"Error en /dashboard/consumo_diario: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al obtener el consumo diario.")
    
@app.get("/dashboard/errores_por_zona")
def errores_por_zona(fecha_hora: str = Query(...)):
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        rows = session.execute("SELECT codigo_medidor FROM errores_iot WHERE fecha_hora = %s ALLOW FILTERING", (fh,))
        medidores_con_errores = set(r["codigo_medidor"] for r in rows)

        infra_all = session.execute(stmt_infra_all).all()
        zona_errores = {}

        for inf in infra_all:
            zona = inf.get("zona", "SIN_ZONA")
            for med in inf.get("medidores") or []:
                if med in medidores_con_errores:
                    zona_errores[zona] = zona_errores.get(zona, 0) + 1

        return [{"zona": z, "errores": c} for z, c in zona_errores.items()]
    except Exception as e:
        logger.error(f"Error en /dashboard/errores_por_zona: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al contar errores por zona.")
    
from collections import Counter

@app.get("/dashboard/top_errores")
def top_errores(fecha_hora: str = Query(...)):
    """
    Devuelve los tipos de error más frecuentes para una hora dada (top 5).
    """
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        rows = session.execute("""
            SELECT tipo_error FROM errores_iot WHERE fecha_hora = %s ALLOW FILTERING
        """, (fh,))
        tipos = [r["tipo_error"] for r in rows if r.get("tipo_error")]
        top = Counter(tipos).most_common(5)
        return [{"tipo_error": t, "cantidad": c} for t, c in top]

    except Exception as e:
        logger.error(f"Error en /dashboard/top_errores: {e}", exc_info=True)
        raise HTTPException(500, "Error al obtener los errores más frecuentes.")

@app.get("/dashboard/modelos_uso")
def modelos_uso(fecha_hora: str = Query(...)):
    """
    Devuelve la cantidad de lecturas por modelo de medidor en una hora dada.
    """
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        rows = session.execute("""
            SELECT modelo FROM lecturas_medidor WHERE fecha_hora = %s ALLOW FILTERING
        """, (fh,))
        modelos = [r["modelo"] or "DESCONOCIDO" for r in rows]
        conteo = Counter(modelos)
        return [{"modelo": m, "cantidad": c} for m, c in conteo.items()]
    except Exception as e:
        logger.error(f"Error en /dashboard/modelos_uso: {e}", exc_info=True)
        raise HTTPException(500, "Error al obtener el uso de modelos.")

from collections import defaultdict

@app.get("/dashboard/consumo_por_categoria")
def consumo_por_categoria(fecha_hora: str = Query(...)):
    """
    Devuelve el consumo total agrupado por descripción de categoría (ej. Residencial, Comercial, etc.)
    para una hora específica.
    """
    try:
        fh = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

        # 1. Obtener todas las lecturas en esa hora
        lecturas = session.execute("""
            SELECT codigo_medidor, consumo_periodo FROM lecturas_medidor WHERE fecha_hora = %s ALLOW FILTERING
        """, (fh,))
        consumo_por_medidor = {
            r["codigo_medidor"]: r["consumo_periodo"] or 0 for r in lecturas if r.get("codigo_medidor")
        }

        # 2. Obtener infraestructura y agrupar por descripcion_categoria
        infra_all = session.execute(stmt_infra_all).all()
        categoria_total = defaultdict(int)

        for inf in infra_all:
            desc = (inf.get("descripcion_categoria") or "Otros").strip().title()
            for med in inf.get("medidores") or []:
                if med in consumo_por_medidor:
                    categoria_total[desc] += consumo_por_medidor[med]

        return [{"categoria": cat, "consumo": consumo} for cat, consumo in categoria_total.items()]
    except Exception as e:
        logger.error(f"Error en /dashboard/consumo_por_categoria: {e}", exc_info=True)
        raise HTTPException(500, "Error interno al calcular consumo por categoría.")


@app.get("/dashboard/debug_categorias")
def debug_categorias():
    try:
        rows = session.execute("SELECT descripcion_categoria FROM infraestructura ALLOW FILTERING")
        unicos = set((r.get("descripcion_categoria") or "").strip().title() for r in rows)
        return {"categorias_encontradas": sorted(unicos)}
    except Exception as e:
        logger.error("Error al depurar categorias", exc_info=True)
        raise HTTPException(500, "Error interno en depuración de categorías.")



# --------------------------------------------
# Apagado: cerrar sesión Cassandra
# --------------------------------------------
@app.on_event("shutdown")
def shutdown_event():
    session.cluster.shutdown()
