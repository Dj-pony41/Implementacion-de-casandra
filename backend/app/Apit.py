from flask import Flask, jsonify, request
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import math

app = Flask(__name__)

# Configuración básica de Cassandra
CASSANDRA_HOSTS = ['127.0.0.1']  # Cambia por tus direcciones IP de nodos Cassandra
CASSANDRA_PORT = 9042            # Puerto default de Cassandra
KEYSPACE = 'semapa_distribuidos'         # Nombre de tu keyspace

def get_cassandra_session():
    """Crea conexión a Cassandra sin autenticación"""
    cluster = Cluster(
        CASSANDRA_HOSTS, 
        port=CASSANDRA_PORT,
        connect_timeout=60  # Timeout de conexión de 60 segundos
    )
    return cluster.connect(KEYSPACE)

def round_to_nearest_8_hours(dt):
    """Redondea al múltiplo de 8 horas más cercano"""
    total_seconds = dt.hour * 3600 + dt.minute * 60 + dt.second
    nearest_8h = round(total_seconds / (8*3600)) * (8*3600)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=nearest_8h)

@app.route('/api/clientes', methods=['GET'])
def get_clientes():
    """Obtiene todos los clientes con sus medidores"""
    try:
        # Procesar parámetro de fecha/hora si existe
        fecha_hora_param = request.args.get('fecha_hora')
        if fecha_hora_param:
            try:
                fecha_hora = datetime.strptime(fecha_hora_param, '%Y-%m-%d %H:%M')
                fecha_hora_redondeada = round_to_nearest_8_hours(fecha_hora)
            except ValueError:
                return jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD HH:MM"}), 400
        else:
            fecha_hora_redondeada = round_to_nearest_8_hours(datetime.now())
        
        session = get_cassandra_session()
        
        # Obtener todos los clientes
        query_clientes = "SELECT * FROM clientes"
        rows_clientes = session.execute(query_clientes)
        
        clientes = []
        
        for cliente in rows_clientes:
            # Obtener medidores para este cliente en el periodo
            query_medidores = """
            SELECT * FROM medidores 
            WHERE ContratoID = %s AND FechaHora = %s
            """
            medidores = session.execute(query_medidores, [cliente.contratoid, fecha_hora_redondeada])
            
            medidores_lista = []
            for medidor in medidores:
                medidores_lista.append({
                    "CodigoMedidor": medidor.codigomedidor,
                    "Modelo": medidor.modelo,
                    "Estado": medidor.estado,
                    "FechaHora": medidor.fechahora.strftime('%Y-%m-%d %H:%M'),
                    "Lectura": float(medidor.lectura),
                    "ConsumoPeriodo": float(medidor.consumoperiodo),
                    "TarifaUSD": medidor.tarifausd
                })
            
            # Solo agregar clientes con medidores en este periodo
            if medidores_lista:
                clientes.append({
                    "ContratoID": cliente.contratoid,
                    "Nombre": cliente.nombre,
                    "CI/NIT": cliente["ci/nit"],
                    "Email": cliente.email,
                    "Telefono": cliente.telefono,
                    "Latitud": float(cliente.latitud),
                    "Longitud": float(cliente.longitud),
                    "Distrito": cliente.distrito,
                    "Zona": cliente.zona,
                    "Medidores": medidores_lista
                })
        
        return jsonify({
            "fecha_hora_consulta": fecha_hora_redondeada.strftime('%Y-%m-%d %H:%M'),
            "clientes": clientes
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    finally:
        if 'session' in locals():
            session.shutdown()

@app.route('/api/clientes/<contrato_id>', methods=['GET'])
def get_cliente(contrato_id):
    """Obtiene un cliente específico por su ID de contrato"""
    try:
        # Procesar parámetro de fecha/hora si existe
        fecha_hora_param = request.args.get('fecha_hora')
        if fecha_hora_param:
            try:
                fecha_hora = datetime.strptime(fecha_hora_param, '%Y-%m-%d %H:%M')
                fecha_hora_redondeada = round_to_nearest_8_hours(fecha_hora)
            except ValueError:
                return jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD HH:MM"}), 400
        else:
            fecha_hora_redondeada = round_to_nearest_8_hours(datetime.now())
        
        session = get_cassandra_session()
        
        # Obtener el cliente
        query_cliente = "SELECT * FROM clientes WHERE ContratoID = %s"
        cliente = session.execute(query_cliente, [contrato_id]).one()
        
        if not cliente:
            return jsonify({"error": "Cliente no encontrado"}), 404
        
        # Obtener medidores para este cliente
        query_medidores = """
        SELECT * FROM medidores 
        WHERE ContratoID = %s AND FechaHora = %s
        """
        medidores = session.execute(query_medidores, [contrato_id, fecha_hora_redondeada])
        
        medidores_lista = []
        for medidor in medidores:
            medidores_lista.append({
                "CodigoMedidor": medidor.codigomedidor,
                "Modelo": medidor.modelo,
                "Estado": medidor.estado,
                "FechaHora": medidor.fechahora.strftime('%Y-%m-%d %H:%M'),
                "Lectura": float(medidor.lectura),
                "ConsumoPeriodo": float(medidor.consumoperiodo),
                "TarifaUSD": medidor.tarifausd
            })
        
        return jsonify({
            "fecha_hora_consulta": fecha_hora_redondeada.strftime('%Y-%m-%d %H:%M'),
            "cliente": {
                "ContratoID": cliente.contratoid,
                "Nombre": cliente.nombre,
                "CI/NIT": cliente["ci/nit"],
                "Email": cliente.email,
                "Telefono": cliente.telefono,
                "Latitud": float(cliente.latitud),
                "Longitud": float(cliente.longitud),
                "Distrito": cliente.distrito,
                "Zona": cliente.zona,
                "Medidores": medidores_lista
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    finally:
        if 'session' in locals():
            session.shutdown()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)