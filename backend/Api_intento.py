from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement
from datetime import datetime, timezone
from typing import List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Semapa v7 API (Con límite)")

# Modelos Pydantic
class MedidorResponse(BaseModel):
    CodigoMedidor:   str
    Modelo:          str
    Estado:          str
    FechaHora:       str
    Lectura:         int
    ConsumoPeriodo:  int
    TarifaUSD:       str

class ContratoResponse(BaseModel):
    ContratoID: str
    Nombre:     str
    CI_NIT:     int
    Email:      str
    Telefono:   str
    Latitud:    float
    Longitud:   float
    Distrito:   str
    Zona:       str
    Medidores:  List[MedidorResponse]

def format_tarifa(v: float) -> str:
    return f"${v:.2f}"

def get_session():
    cluster = Cluster(['127.0.0.1'], protocol_version=4)
    session = cluster.connect('semapa_v7')
    session.row_factory = dict_factory
    return session

@app.get("/lecturas", response_model=List[ContratoResponse])
def lecturas(
    fecha_hora: str,
    lat_min:    float = Query(..., description="Latitud mínima"),
    lat_max:    float = Query(..., description="Latitud máxima"),
    lon_min:    float = Query(..., description="Longitud mínima"),
    lon_max:    float = Query(..., description="Longitud máxima"),
    max_rows:   int   = Query(10000, description="Máximo de lecturas a recuperar")
):
    try:
        # 1) parsear fecha
        naive = datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M")
        fh = naive.replace(tzinfo=timezone.utc)

        session = get_session()

        # 2) Traer hasta max_rows lecturas de esa hora
        cql_lect = (
            "SELECT codigo_medidor, modelo, estado, lectura, consumo_periodo, tarifa_usd, fecha_hora "
            "FROM lecturas_medidor WHERE fecha_hora = %s LIMIT %s ALLOW FILTERING"
        )
        lect_rows = session.execute(
            SimpleStatement(cql_lect),
            (fh, max_rows)
        ).all()
        if not lect_rows:
            return []

        # 3) Agrupar lecturas por medidor
        lect_by_med = {}
        for r in lect_rows:
            lect_by_med.setdefault(r['codigo_medidor'], []).append(r)

        # 4) Cargar infraestructura completa y filtrar por bounding‐box
        infra_rows = session.execute(
            "SELECT contrato_id, nombre, ci_nit, email, telefono, "
            "latitud, longitud, distrito, zona, medidores "
            "FROM infraestructura"
        ).all()

        contrato_info = {}
        med_to_contrato = {}
        for inf in infra_rows:
            lat, lon = inf['latitud'], inf['longitud']
            if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
                continue
            cid = inf['contrato_id']
            contrato_info[cid] = {
                "nombre":    inf['nombre'],
                "ci_nit":    inf['ci_nit'],
                "email":     inf['email'],
                "telefono":  inf['telefono'],
                "latitud":   lat,
                "longitud":  lon,
                "distrito":  inf['distrito'],
                "zona":      inf['zona'],
                "medidores": inf['medidores'] or []
            }
            for md in inf['medidores'] or []:
                med_to_contrato[md] = cid

        if not contrato_info:
            return []

        # 5) Ensamblar respuesta: para cada contrato, los medidores que estén en lect_by_med
        result: List[ContratoResponse] = []
        for cid, info in contrato_info.items():
            lista_med = []
            for md in info['medidores']:
                if md in lect_by_med:
                    for r in lect_by_med[md]:
                        lista_med.append(MedidorResponse(
                            CodigoMedidor= md,
                            Modelo=         r['modelo'] or "Unknown",
                            Estado=         r['estado'] or "Unknown",
                            FechaHora=      r['fecha_hora'].strftime("%Y-%m-%d %H:%M"),
                            Lectura=        r['lectura'] or 0,
                            ConsumoPeriodo= r['consumo_periodo'] or 0,
                            TarifaUSD=      format_tarifa(r['tarifa_usd'] or 0.0)
                        ))
            if lista_med:
                result.append(ContratoResponse(
                    ContratoID=cid,
                    Nombre=    info['nombre'],
                    CI_NIT=    info['ci_nit'],
                    Email=     info['email'],
                    Telefono=  info['telefono'],
                    Latitud=   info['latitud'],
                    Longitud=  info['longitud'],
                    Distrito=  info['distrito'],
                    Zona=      info['zona'],
                    Medidores= lista_med
                ))

        return result

    except ValueError:
        raise HTTPException(400, "Formato inválido de fecha_hora; usa YYYY-MM-DD HH:MM")
    except Exception as e:
        logger.error(f"Error en lecturas: {e}", exc_info=True)
        raise HTTPException(500, "Error interno del servidor")
    finally:
        try:
            session.cluster.shutdown()
        except:
            pass
