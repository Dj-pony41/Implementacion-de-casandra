from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate


def conectar_cassandra():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('semapa_v9')
    return session


def obtener_mapa_medidores(session):
    rows = session.execute("SELECT contrato_id, distrito, medidores FROM infraestructura")
    medidor_distrito = {}
    for row in rows:
        if row.medidores:
            for m in row.medidores:
                medidor_distrito[m] = row.distrito
    return medidor_distrito


def consulta1():
    """Consumo promedio por distrito en un rango de 8 horas"""
    session = conectar_cassandra()
    medidor_distrito = obtener_mapa_medidores(session)

    rows = session.execute("SELECT codigo_medidor, fecha_hora, lectura FROM lecturas_medidor")
    data = []
    for row in rows:
        distrito = medidor_distrito.get(row.codigo_medidor)
        if distrito:
            hora = row.fecha_hora.hour
            if hora < 8:
                franja = "0:00-08:00"
            elif hora < 16:
                franja = "08:00-16:00"
            else:
                franja = "16:00-24:00"
            data.append([distrito, franja, row.lectura])

    df = pd.DataFrame(data, columns=["Distrito", "Hora", "Consumo"])
    df_grouped = df.groupby(["Distrito", "Hora"]).sum().reset_index()
    distritos_unicos = df_grouped["Distrito"].unique()[:5]
    df_limitado = df_grouped[df_grouped["Distrito"].isin(distritos_unicos)]

    df_limitado["Consumo (m³)"] = df_limitado["Consumo"].apply(lambda x: f"{x:,}")
    df_limitado = df_limitado.drop(columns=["Consumo"])
    df_limitado = df_limitado[["Distrito", "Hora", "Consumo (m³)"]]

    print("\n🔹 Consulta 1: Consumo promedio por distrito en rangos de 8 horas\n")
    print(tabulate(df_limitado, headers="keys", tablefmt="grid"))


def consulta2():
    """Comparativa de consumo entre las 4 últimas semanas en 3 o más distritos"""
    session = conectar_cassandra()
    medidor_distrito = obtener_mapa_medidores(session)

    fecha_hoy = datetime.now()
    fecha_inicio = fecha_hoy - timedelta(days=28)

    rows = session.execute("""
        SELECT codigo_medidor, fecha_hora, lectura
        FROM lecturas_medidor
        WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_inicio,))

    data = []
    for row in rows:
        distrito = medidor_distrito.get(row.codigo_medidor)
        if distrito:
            semana = ((fecha_hoy - row.fecha_hora).days) // 7
            semana_label = f"S{4 - semana}" if semana < 4 else None
            if semana_label:
                data.append([distrito, semana_label, row.lectura])

    df = pd.DataFrame(data, columns=["Distrito", "Semana", "Consumo"])
    df_grouped = df.groupby(["Distrito", "Semana"]).sum().reset_index()
    distritos_top5 = df_grouped["Distrito"].unique()[:5]
    df_grouped = df_grouped[df_grouped["Distrito"].isin(distritos_top5)]

    df_pivot = df_grouped.pivot(index="Semana", columns="Distrito", values="Consumo").fillna(0)
    df_pivot = df_pivot.apply(lambda col: col.map(lambda x: f"{int(x):,}"))
    df_pivot = df_pivot.reindex(sorted(df_pivot.index), axis=0)

    print("\n🔹 Consulta 2: Consumo por semana en 3 o más distritos\n")
    print(tabulate(df_pivot, headers="keys", tablefmt="grid"))

def consulta3():
    """Contratos con consumo excesivo (más de 45 m³/mes en tarifa residencial)"""
    session = conectar_cassandra()

    # Obtener medidor → (contrato_id, tarifa)
    infra_rows = session.execute("SELECT contrato_id, medidores, categoria FROM infraestructura")
    medidor_info = {}
    for row in infra_rows:
        if row.medidores:
            for m in row.medidores:
                medidor_info[m] = (row.contrato_id, row.categoria)

    # Obtener lecturas por medidor
    lectura_rows = session.execute("SELECT codigo_medidor, lectura FROM lecturas_medidor")

    consumo_por_contrato = {}

    for row in lectura_rows:
        info = medidor_info.get(row.codigo_medidor)
        if info:
            contrato_id, tarifa = info
            if "RESIDENCIAL" in tarifa.upper():
                if contrato_id not in consumo_por_contrato:
                    consumo_por_contrato[contrato_id] = 0
                consumo_por_contrato[contrato_id] += row.lectura or 0

    # Evaluar excesos
    consumo_minimo_litros = 300 * 30 * 5  # 45,000 L
    data = []
    for contrato_id, consumo_l in consumo_por_contrato.items():
        if consumo_l > consumo_minimo_litros:
            exceso_pct = ((consumo_l - consumo_minimo_litros) / consumo_minimo_litros) * 100
            data.append([contrato_id, "Residencial", f"{consumo_l:,}", f"{exceso_pct:.2f}%"])

    df = pd.DataFrame(data, columns=["Contrato", "Tarifa", "Consumo (L/mes)", "Exceso (%)"])

    print("\n🔹 Consulta 3: Contratos con consumo excesivo\n")
    print(tabulate(df.head(5), headers="keys", tablefmt="grid"))


def consulta4():
    """Cantidad de medidores activos por distrito y zona"""
    session = conectar_cassandra()

    rows = session.execute("SELECT contrato_id, distrito, zona, medidores FROM infraestructura")

    data = []
    for row in rows:
        if row.medidores:
            for m in row.medidores:
                data.append((row.distrito, row.zona, m))

    df = pd.DataFrame(data, columns=["Distrito", "Zona", "Medidor"])

    # Suponemos activos por existencia en infraestructura (ajusta si tienes campo "estado")
    df_grouped = df.groupby(["Distrito", "Zona"]).count().reset_index()
    df_grouped.rename(columns={"Medidor": "Medidores Activos"}, inplace=True)

    print("\n🔹 Consulta 4: Medidores activos por distrito y zona\n")
    print(tabulate(df_grouped.head(5), headers="keys", tablefmt="grid"))


def consulta5():
    """Medidores fuera de servicio por distrito y zona"""
    session = conectar_cassandra()
    fecha_limite = datetime.now() - timedelta(days=30)

    medidores_con_lectura = session.execute("""
        SELECT codigo_medidor, fecha_hora FROM lecturas_medidor WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_limite,))

    medidores_activos = set([row.codigo_medidor for row in medidores_con_lectura])

    rows = session.execute("SELECT contrato_id, distrito, zona, medidores FROM infraestructura")
    data = []
    for row in rows:
        if row.medidores:
            for med in row.medidores:
                if med not in medidores_activos:
                    data.append((row.distrito, row.zona, med))

    df = pd.DataFrame(data, columns=["Distrito", "Zona", "Medidor"])
    df_grouped = df.groupby(["Distrito", "Zona"]).count().reset_index()
    df_grouped.rename(columns={"Medidor": "Medidores Fuera de servicio"}, inplace=True)

    print("\n🔹 Consulta 5: Medidores fuera de servicio por distrito y zona\n")
    print(tabulate(df_grouped.head(5), headers="keys", tablefmt="grid"))


def consulta6():
    """Modelos de medidor con mayor tasa de fallos reportados"""
    session = conectar_cassandra()

    # Obtener modelo desde lecturas y errores desde errores_iot
    # Este ejemplo asume que errores_iot tiene código_medidor y tipo_error

    errores = session.execute("SELECT codigo_medidor, tipo_error FROM errores_iot")

    errores_por_medidor = {}
    for row in errores:
        if row.codigo_medidor not in errores_por_medidor:
            errores_por_medidor[row.codigo_medidor] = []
        errores_por_medidor[row.codigo_medidor].append(row.tipo_error)

    # Obtener modelos
    lecturas = session.execute("SELECT codigo_medidor, modelo FROM lecturas_medidor")
    medidor_modelo = {}
    for row in lecturas:
        if row.codigo_medidor and row.modelo:
            medidor_modelo[row.codigo_medidor] = row.modelo

    modelo_fallos = {}
    for medidor, fallos in errores_por_medidor.items():
        modelo = medidor_modelo.get(medidor)
        if modelo:
            if modelo not in modelo_fallos:
                modelo_fallos[modelo] = []
            modelo_fallos[modelo].extend(fallos)

    data = []
    for modelo, fallos in modelo_fallos.items():
        tipos_fallo = set(fallos)
        data.append([modelo, ", ".join(tipos_fallo), len(fallos)])

    df = pd.DataFrame(data, columns=["Modelo", "Fallos Reportados", "Cantidad"])
    df_sorted = df.sort_values(by="Cantidad", ascending=False).head(5)

    print("\n🔹 Consulta 6: Modelos con mayor tasa de fallos técnicos\n")
    print(tabulate(df_sorted, headers="keys", tablefmt="grid"))

def consulta7():
    """Consumo mensual promedio en m³ por tarifa y distrito"""
    session = conectar_cassandra()

    # Requiere join lógico entre lecturas y contratos (infraestructura)
    infra = session.execute("SELECT contrato_id, distrito, medidores FROM infraestructura")
    medidor_distrito = {}
    for row in infra:
        if row.medidores:
            for m in row.medidores:
                medidor_distrito[m] = row.distrito

    rows = session.execute("SELECT codigo_medidor, fecha_hora, lectura, tarifa_usd FROM lecturas_medidor")
    data = []
    for row in rows:
        distrito = medidor_distrito.get(row.codigo_medidor)
        tarifa = str(row.tarifa_usd).upper() if row.tarifa_usd else None
        if distrito and tarifa:
            mes = row.fecha_hora.strftime("%Y-%m")
            data.append([distrito, tarifa, mes, row.lectura])

    df = pd.DataFrame(data, columns=["Distrito", "Tarifa", "Mes", "Lectura"])
    df_grouped = df.groupby(["Distrito", "Tarifa", "Mes"]).sum().reset_index()
    df_avg = df_grouped.groupby(["Distrito", "Tarifa"])["Lectura"].mean().reset_index()
    df_avg["Lectura m³"] = df_avg["Lectura"] / 1000
    df_pivot = df_avg.pivot(index="Distrito", columns="Tarifa", values="Lectura m³").fillna(0)
    df_pivot = df_pivot.round(1)

    print("\n🔹 Consulta 7: Consumo mensual promedio (m³) por tarifa y distrito\n")
    print(tabulate(df_pivot.head(6), headers="keys", tablefmt="grid"))


def consulta8():
    """Zonas con más medidores con consumo anómalo (cero o excesivo)"""
    session = conectar_cassandra()

    # Umbral ONU en litros/mes
    limite_superior = 45000

    # Mapa medidor → zona
    infra = session.execute("SELECT zona, medidores FROM infraestructura")
    medidor_zona = {}
    for row in infra:
        if row.medidores:
            for m in row.medidores:
                medidor_zona[m] = row.zona

    rows = session.execute("SELECT codigo_medidor, lectura FROM lecturas_medidor")
    zona_anomalias = {}

    for row in rows:
        zona = medidor_zona.get(row.codigo_medidor)
        if zona:
            if row.lectura == 0 or row.lectura > limite_superior:
                if zona not in zona_anomalias:
                    zona_anomalias[zona] = 0
                zona_anomalias[zona] += 1

    data = sorted(zona_anomalias.items(), key=lambda x: x[1], reverse=True)[:5]
    df = pd.DataFrame(data, columns=["Zona", "Medidores Anómalos"])

    print("\n🔹 Consulta 8: Zonas con más medidores con consumo anómalo\n")
    print(tabulate(df, headers="keys", tablefmt="grid"))

def consulta9():
    """Lecturas fallidas o inconsistentes por tipo de medidor en el último mes"""
    session = conectar_cassandra()
    limite = datetime.now() - timedelta(days=30)

    errores = session.execute("""
        SELECT codigo_medidor, tipo_error, fecha_hora FROM errores_iot 
        WHERE fecha_hora >= %s ALLOW FILTERING
    """, (limite,))

    lecturas = session.execute("SELECT codigo_medidor, modelo FROM lecturas_medidor")
    medidor_modelo = {row.codigo_medidor: row.modelo for row in lecturas if row.modelo}

    conteo = {}
    for row in errores:
        modelo = medidor_modelo.get(row.codigo_medidor, "Desconocido")
        clave = (row.tipo_error.strip(), modelo)
        conteo[clave] = conteo.get(clave, 0) + 1

    data = []
    for (error, modelo), cantidad in conteo.items():
        data.append([error, modelo, cantidad])

    df = pd.DataFrame(data, columns=["Descripción", "Modelo", "Cantidad"])
    df_pivot = df.pivot(index="Descripción", columns="Modelo", values="Cantidad").fillna(0).astype(int)

    print("\n🔹 Consulta 9: Errores reportados en el último mes por modelo de medidor\n")
    print(tabulate(df_pivot.head(5), headers="keys", tablefmt="grid"))


def consulta10():
    """Porcentaje de medidores con más de 4 años de antigüedad"""
    session = conectar_cassandra()
    años_limite = datetime.now().date() - timedelta(days=4 * 365)

    rows = session.execute("SELECT codigo_medidor, fecha_instalacion FROM lecturas_medidor")

    vistos = set()
    total = 0
    fuera_garantia = 0

    for row in rows:
        if row.codigo_medidor not in vistos:
            vistos.add(row.codigo_medidor)
            if row.fecha_instalacion:
                try:
                    fecha = datetime.strptime(str(row.fecha_instalacion), "%Y-%m-%d").date()
                    total += 1
                    if fecha < años_limite:
                        fuera_garantia += 1
                except Exception as e:
                    print(f"Error al convertir fecha: {row.fecha_instalacion} → {e}")

    porcentaje = (fuera_garantia / total) * 100 if total > 0 else 0

    print("\n🔹 Consulta 10: Medidores fuera de garantía (>4 años de antigüedad)\n")
    print(f"Total Medidores: {total}")
    print(f"Medidores > 4 años: {fuera_garantia}")
    print(f"% Porcentaje: {porcentaje:.2f}%")



def consulta11():
    """Zonas con mayor consumo por categoría residencial"""

    session = conectar_cassandra()

    # 1. Crear mapa medidor → (zona, categoria)
    rows_infra = session.execute("SELECT zona, categoria, medidores FROM infraestructura")
    medidor_info = {}
    for row in rows_infra:
        if row.medidores:
            for m in row.medidores:
                medidor_info[m] = (row.zona, row.categoria)

    # 2. Recorrer lecturas y clasificar
    rows_lectura = session.execute("SELECT codigo_medidor, lectura FROM lecturas_medidor")
    data = {}
    for row in rows_lectura:
        zona_cat = medidor_info.get(row.codigo_medidor)
        if zona_cat:
            zona, categoria = zona_cat
            if zona and categoria and "RESIDENCIAL" in categoria.upper():
                key = (zona.strip(), categoria.strip().upper())
                data[key] = data.get(key, 0) + (row.lectura or 0)

    if not data:
        print("\n⚠️ No hay datos residenciales encontrados para consulta11().\n")
        return

    # 3. Convertir a DataFrame
    filas = []
    for (zona, categoria), consumo in data.items():
        filas.append({"Zona": zona, "Categoría": categoria, "Consumo (m³)": round(consumo / 1000)})

    df = pd.DataFrame(filas)

    # 4. Pivotear tabla para formato como el ejemplo
    df_pivot = df.pivot(index="Zona", columns="Categoría", values="Consumo (m³)").fillna(0).astype(int)

    print("\n🔹 Consulta 11: Consumo por categoría residencial agrupado por zona\n")
    print(tabulate(df_pivot.head(5), headers="keys", tablefmt="grid"))



def consulta12():
    """Top 3 clientes/servicios que más consumen por distrito del mes activo"""
    session = conectar_cassandra()

    fecha_limite = datetime.now() - timedelta(days=30)

    # Infraestructura (contrato, zona, distrito)
    infra = session.execute("SELECT contrato_id, distrito, nombre FROM infraestructura")
    contrato_distrito_nombre = {}
    for row in infra:
        contrato_distrito_nombre[row.contrato_id] = (row.distrito, row.nombre)

    # Lecturas del mes
    rows = session.execute("""
        SELECT codigo_medidor, lectura FROM lecturas_medidor WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_limite,))

    # Mapeo medidor → contrato
    infra2 = session.execute("SELECT contrato_id, medidores FROM infraestructura")
    medidor_contrato = {}
    for row in infra2:
        if row.medidores:
            for m in row.medidores:
                medidor_contrato[m] = row.contrato_id

    data = []
    consumo_map = {}

    for row in rows:
        contrato = medidor_contrato.get(row.codigo_medidor)
        info = contrato_distrito_nombre.get(contrato)
        if contrato and info:
            distrito, cliente = info
            key = (distrito, contrato, cliente)
            consumo_map[key] = consumo_map.get(key, 0) + row.lectura

    for (distrito, contrato, cliente), consumo in consumo_map.items():
        data.append([distrito, contrato, cliente, round(consumo / 1000)])  # m³

    df = pd.DataFrame(data, columns=["Distrito", "Servicio", "Cliente", "Consumo m3"])
    df_sorted = df.sort_values(by=["Distrito", "Consumo m3"], ascending=[True, False])

    # Top 3 por distrito
    df_top = df_sorted.groupby("Distrito").head(3).reset_index(drop=True)

    print("\n🔹 Consulta 12: Top 3 clientes por distrito (último mes)\n")
    print(tabulate(df_top.head(15), headers="keys", tablefmt="grid"))

def consulta13():
    """Zonas que requieren renovación de medidores según errores reportados"""
    session = conectar_cassandra()

    # Medidor → zona + distrito
    infra = session.execute("SELECT distrito, zona, medidores FROM infraestructura")
    medidor_zona_distrito = {}
    for row in infra:
        if row.medidores:
            for m in row.medidores:
                medidor_zona_distrito[m] = (row.zona, row.distrito)

    # Errores
    errores = session.execute("SELECT codigo_medidor, tipo_error FROM errores_iot")

    conteo = {}
    for row in errores:
        zona_distrito = medidor_zona_distrito.get(row.codigo_medidor)
        if zona_distrito:
            zona, distrito = zona_distrito
            key = (row.tipo_error.strip(), distrito, zona)
            conteo[key] = conteo.get(key, 0) + 1

    data = []
    for (descripcion, distrito, zona), cantidad in conteo.items():
        data.append([descripcion, distrito, zona, cantidad])

    df = pd.DataFrame(data, columns=["Descripción", "Distrito", "Zona", "Nro Reportes"])
    df_sorted = df.sort_values(by="Nro Reportes", ascending=False)

    print("\n🔹 Consulta 13: Zonas que requieren renovación por cantidad de errores\n")
    print(tabulate(df_sorted.head(5), headers="keys", tablefmt="grid"))


def consulta15(distrito_objetivo="MOLLE"):
    """Errores más reportados por zona en un distrito específico (e.g., MOLLE)"""
    session = conectar_cassandra()

    # Medidor → zona + distrito
    infra = session.execute("SELECT distrito, zona, medidores FROM infraestructura")
    medidor_zona = {}
    for row in infra:
        if row.medidores and row.distrito.upper() == distrito_objetivo.upper():
            for m in row.medidores:
                medidor_zona[m] = row.zona

    # Errores
    errores = session.execute("SELECT codigo_medidor, tipo_error FROM errores_iot")

    conteo = {}
    for row in errores:
        zona = medidor_zona.get(row.codigo_medidor)
        if zona:
            key = (row.tipo_error.strip(), zona)
            conteo[key] = conteo.get(key, 0) + 1

    data = []
    for (descripcion, zona), cantidad in conteo.items():
        data.append([descripcion, zona, cantidad])

    df = pd.DataFrame(data, columns=["Descripción", "Zona", "Nro Reportes"])
    df_sorted = df.sort_values(by="Nro Reportes", ascending=False)

    print(f"\n🔹 Consulta 15: Zonas con más errores en distrito '{distrito_objetivo}'\n")
    print(tabulate(df_sorted.head(5), headers="keys", tablefmt="grid"))

def consulta17():
    """Zonas con mayor cobertura de antenas"""
    session = conectar_cassandra()

    # medidor → zona
    infra = session.execute("SELECT zona, medidores FROM infraestructura")
    medidor_zona = {}
    for row in infra:
        if row.medidores:
            for m in row.medidores:
                medidor_zona[m] = row.zona

    rows = session.execute("SELECT codigo_medidor, antena FROM lecturas_medidor")
    data = {}

    for row in rows:
        zona = medidor_zona.get(row.codigo_medidor)
        if zona and row.antena:
            key = (f"LoRaWan-{row.antena}", zona)
            data[key] = data.get(key, 0) + 1

    registros = []
    for (radio, zona), cantidad in data.items():
        registros.append([radio, zona, cantidad])

    df = pd.DataFrame(registros, columns=["RadioBase", "Zona", "Conexiones"])
    df_sorted = df.sort_values(by="Conexiones", ascending=False)

    print("\n🔹 Consulta 17: Zonas con mayor cobertura de antenas\n")
    print(tabulate(df_sorted.head(10), headers="keys", tablefmt="grid"))



def consulta18():
    """Proyección de demanda de agua por 5 años usando datos reales por distrito"""

    session = conectar_cassandra()

    # Mapeo medidor → distrito
    rows_infra = session.execute("SELECT distrito, medidores FROM infraestructura")
    medidor_distrito = {}
    for row in rows_infra:
        if row.medidores:
            for m in row.medidores:
                medidor_distrito[m] = row.distrito

    # Lecturas recientes (últimos 30 días)
    fecha_limite = datetime.now() - timedelta(days=30)
    rows = session.execute("""
        SELECT codigo_medidor, lectura, fecha_hora
        FROM lecturas_medidor
        WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_limite,))

    consumo_por_distrito = {}

    for row in rows:
        distrito = medidor_distrito.get(row.codigo_medidor)
        if distrito:
            consumo_por_distrito[distrito] = consumo_por_distrito.get(distrito, 0) + (row.lectura or 0)

    # Proyección: factor de crecimiento 2.6% anual
    factor = 1.026
    años = [2025, 2026, 2027, 2028, 2029]

    data = []
    for distrito, consumo_litros in consumo_por_distrito.items():
        consumo_m3_base = consumo_litros / 1000
        fila = {"Distrito": distrito}
        valor = consumo_m3_base
        for año in años:
            fila[f"{año} m3"] = round(valor)
            valor *= factor
        data.append(fila)

    df = pd.DataFrame(data)
    df_sorted = df.sort_values(by="2025 m3", ascending=False)

    print("\n🔹 Consulta 18: Proyección de demanda de agua 2025–2029 (desde Cassandra)\n")
    print(tabulate(df_sorted.head(6), headers="keys", tablefmt="grid"))

def consulta20():
    """Impacto económico de cambiar tarifa Preferencial P a Residencial R4"""

    session = conectar_cassandra()

    # Mapeo medidor → categoria
    rows_infra = session.execute("SELECT categoria, medidores FROM infraestructura")
    medidores_P = set()
    for row in rows_infra:
        if row.categoria and row.categoria.upper().startswith("P") and row.medidores:
            for m in row.medidores:
                medidores_P.add(m)

    # Consumo total de los medidores en categoría P
    rows_lecturas = session.execute("SELECT codigo_medidor, lectura FROM lecturas_medidor")
    total_m3 = 0
    total_medidores = 0
    for row in rows_lecturas:
        if row.codigo_medidor in medidores_P:
            total_m3 += row.lectura or 0
            total_medidores += 1

    total_m3 = total_m3 / 1000  # litros a m³

    tarifa_P = 4.58
    tarifa_R4 = 8.69

    ingreso_P = total_m3 * tarifa_P
    ingreso_R4 = total_m3 * tarifa_R4
    incremento = ingreso_R4 - ingreso_P

    print("\n🔹 Consulta 20: Impacto económico de cambiar tarifa P → R4\n")
    print(tabulate([[
        len(medidores_P),
        round(total_m3),
        f"${ingreso_P:,.2f}",
        f"${ingreso_R4:,.2f}",
        f"${incremento:,.2f}"
    ]], headers=["Nro Contratos Cat P", "m³", "P $us 4.58", "R4 $us 8.69", "Incremento"], tablefmt="grid"))

def consulta21():
    """Medidores que no reportaron consumo"""
    session = conectar_cassandra()

    # Medidores activos en infraestructura
    rows_infra = session.execute("SELECT distrito, zona, razon_social, medidores FROM infraestructura")
    medidor_info = {}
    for row in rows_infra:
        if row.medidores:
            for m in row.medidores:
                medidor_info[m] = (row.distrito, row.zona, row.razon_social, m)

    # Medidores que sí reportaron
    rows_lectura = session.execute("SELECT DISTINCT codigo_medidor FROM lecturas_medidor")
    medidores_con_lectura = {row.codigo_medidor for row in rows_lectura}

    # Medidores sin consumo
    sin_consumo = []
    for m, (dist, zona, dir, serie) in medidor_info.items():
        if m not in medidores_con_lectura:
            sin_consumo.append([dist, zona, dir, f"SN={serie}"])

    df = pd.DataFrame(sin_consumo, columns=["Distrito", "Zona", "Dirección", "Numero Serie"])
    print("\n🔹 Consulta 21: Medidores que no reportaron consumo\n")
    print(tabulate(df.head(10), headers="keys", tablefmt="grid"))

def consulta22():
    """Proyección de ingresos por tipo de tarifa (en m³ y USD)"""
    session = conectar_cassandra()

    # Map medidor → tarifa
    infra = session.execute("SELECT categoria, medidores FROM infraestructura")
    medidor_tarifa = {}
    for row in infra:
        if row.medidores:
            for m in row.medidores:
                medidor_tarifa[m] = row.categoria

    # Consumos últimos 30 días
    fecha_limite = datetime.now() - timedelta(days=30)
    rows = session.execute("""
        SELECT codigo_medidor, lectura, fecha_hora FROM lecturas_medidor
        WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_limite,))

    consumo_por_tarifa = {}

    for row in rows:
        tarifa = medidor_tarifa.get(row.codigo_medidor)
        if tarifa:
            consumo_por_tarifa[tarifa] = consumo_por_tarifa.get(tarifa, 0) + (row.lectura or 0)

    # Tarifa en USD por m³ (puedes ajustar)
    tarifa_usd = {
        "R1": 1.4, "R2": 2.78, "R3": 5.22, "R4": 8.69,
        "C": 10.43, "CE": 12.15, "I": 9.38, "P": 4.58, "S": 7.65
    }

    registros = []
    for tarifa, consumo_litros in consumo_por_tarifa.items():
        consumo_m3 = consumo_litros / 1000
        usd = tarifa_usd.get(tarifa.upper(), 0)
        ingresos = consumo_m3 * usd
        registros.append([tarifa.upper(), int(consumo_m3), f"${ingresos:,.2f}"])

    df = pd.DataFrame(registros, columns=["Categoría", "Consumo m³", "Ingresos $us"])
    df = df.sort_values(by="Ingresos $us", ascending=False)

    print("\n🔹 Consulta 22: Ingreso proyectado por tipo de tarifa (m³, USD)\n")
    print(tabulate(df.head(10), headers="keys", tablefmt="grid"))

def consulta23():
    """¿Quiénes deben pagar consumo mínimo en categoría Residencial (<10 m³)?"""
    session = conectar_cassandra()

    # Medidor → (zona, distrito, contrato)
    rows_infra = session.execute("SELECT zona, distrito, medidores, razon_social FROM infraestructura")
    medidor_info = {}
    for row in rows_infra:
        if row.medidores:
            for m in row.medidores:
                medidor_info[m] = (row.distrito, row.zona, row.razon_social)

    # Lecturas
    fecha_limite = datetime.now() - timedelta(days=30)
    rows = session.execute("""
        SELECT codigo_medidor, lectura, fecha_hora FROM lecturas_medidor
        WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_limite,))

    consumo_por_medidor = {}
    for row in rows:
        consumo_por_medidor[row.codigo_medidor] = consumo_por_medidor.get(row.codigo_medidor, 0) + (row.lectura or 0)

    # Detectar menores a 10 m³
    min_consumidores = []
    for m, litros in consumo_por_medidor.items():
        consumo_m3 = litros / 1000
        if consumo_m3 < 10:
            info = medidor_info.get(m)
            if info:
                dist, zona, nombre = info
                min_consumidores.append([dist, zona, nombre, round(consumo_m3, 2)])

    df = pd.DataFrame(min_consumidores, columns=["Distrito", "Zona", "Cliente", "Consumo m³"])
    df = df.sort_values(by="Consumo m³")

    print("\n🔹 Consulta 23: Clientes con consumo menor al mínimo (10 m³)\n")
    print(tabulate(df.head(10), headers="keys", tablefmt="grid"))

def consulta24():
    """Proyección de ingresos por tarifa en pies cúbicos"""
    session = conectar_cassandra()

    # Mismo proceso que consulta22 pero convertido a pies³
    infra = session.execute("SELECT categoria, medidores FROM infraestructura")
    medidor_tarifa = {}
    for row in infra:
        if row.medidores:
            for m in row.medidores:
                medidor_tarifa[m] = row.categoria

    fecha_limite = datetime.now() - timedelta(days=30)
    rows = session.execute("""
        SELECT codigo_medidor, lectura FROM lecturas_medidor
        WHERE fecha_hora >= %s ALLOW FILTERING
    """, (fecha_limite,))

    consumo_por_tarifa = {}

    for row in rows:
        tarifa = medidor_tarifa.get(row.codigo_medidor)
        if tarifa:
            consumo_por_tarifa[tarifa] = consumo_por_tarifa.get(tarifa, 0) + (row.lectura or 0)

    tarifa_usd = {
        "R1": 1.4, "R2": 2.78, "R3": 5.22, "R4": 8.69,
        "C": 10.43, "CE": 12.15, "I": 9.38, "P": 4.58, "S": 7.65
    }

    registros = []
    for tarifa, consumo_l in consumo_por_tarifa.items():
        consumo_p3 = (consumo_l / 1000) * 35.3147  # m³ → pies³
        usd = tarifa_usd.get(tarifa.upper(), 0)
        ingresos = (consumo_l / 1000) * usd
        registros.append([tarifa.upper(), int(consumo_p3), f"${ingresos:,.2f}"])

    df = pd.DataFrame(registros, columns=["Categoría", "Consumo pies³", "Ingresos $us"])
    df = df.sort_values(by="Ingresos $us", ascending=False)

    print("\n🔹 Consulta 24: Ingreso proyectado en pies cúbicos por tarifa\n")
    print(tabulate(df.head(10), headers="keys", tablefmt="grid"))










# Llamadas de prueba
if __name__ == "__main__":
    consulta1()
    consulta2()
    consulta3()
    consulta4()
    consulta5()
    consulta6()
    consulta7()
    consulta8()
    consulta9()
    consulta10()
    consulta11()
    consulta12()
    consulta13()
    consulta15()
    consulta17()
    consulta18()
    consulta20()
    consulta21()
    consulta22()
    consulta23()
    consulta24()


