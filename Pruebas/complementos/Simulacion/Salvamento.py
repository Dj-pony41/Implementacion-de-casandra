import os
import json
from glob import glob
from collections import defaultdict
from datetime import datetime

# --- CONFIGURACIÓN ---
# Ruta al JSON maestro de contratos
CONTRATOS_FILE = 'infraestructuras_generadas.json'
# Carpeta que contiene los JSON de lecturas de medidores
LECTURAS_FOLDER = 'lecturas'
# Carpeta de salida para los snapshots
OUTPUT_FOLDER = 'snapshots'

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --- 1) Cargo contratos ---
with open(CONTRATOS_FILE, 'r', encoding='utf-8') as f:
    contratos = json.load(f)

# Mapa de ContratoID a datos básicos
contratos_map = {c['ContratoID']: c for c in contratos}

# --- 2) Cargo todas las lecturas ---
# Estructura: lecturas[ timestamp ][ ContratoID ] = [ lista de lecturas de cada medidor ]
lecturas = defaultdict(lambda: defaultdict(list))

for filepath in glob(os.path.join(LECTURAS_FOLDER, 'lecturas_CT-*.json')):
    with open(filepath, 'r', encoding='utf-8') as f:
        registros = json.load(f)
    for rec in registros:
        ts = rec['FechaHora']            # e.g. "2025-04-01 00:00"
        cod = rec['CodigoMedidor']       # e.g. "MD-9D7F7E8535"
        # Buscamos a qué contrato pertenece este medidor:
        for contrato in contratos:
            if cod in contrato.get('Medidores', []):
                # Extraemos sólo los campos que queremos en el snapshot
                lect = {
                    'CodigoMedidor': rec['CodigoMedidor'],
                    'Modelo':          rec['Modelo'],
                    'Estado':          rec['Estado'],
                    'FechaHora':       rec['FechaHora'],
                    'Lectura':         rec['Lectura'],
                    'ConsumoPeriodo':  rec['ConsumoPeriodo'],
                    'TarifaUSD':       rec['TarifaUSD']
                }
                lecturas[ts][contrato['ContratoID']].append(lect)
                break

# --- 3) Para cada timestamp único, genero un JSON agrupado ---
for ts, contratos_dict in lecturas.items():
    # Construyo la lista de contratos para este snapshot
    snapshot = []
    for ct_id, lects in contratos_dict.items():
        base = contratos_map[ct_id]
        entry = {
            'ContratoID': base['ContratoID'],
            'Nombre':     base['Nombre'],
            'CI/NIT':     base['CI/NIT'],
            'Email':      base['Email'],
            'Telefono':   base['Telefono'],
            'Latitud':    base['Latitud'],
            'Longitud':   base['Longitud'],
            'Distrito':   base['Distrito'],
            'Zona':       base['Zona'],
            'Medidores':  lects
        }
        snapshot.append(entry)

    # Normalizamos nombre de archivo: 2025-04-01_00-00.json
    fn_ts = datetime.strptime(ts, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d_%H-%M')
    out_path = os.path.join(OUTPUT_FOLDER, f'{fn_ts}.json')

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    print(f'✅ Generado snapshot: {out_path}')
