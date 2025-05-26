const BASE_URL = 'http://localhost:8000';

export async function fetchVisiblePoints(params: {
  fecha_hora: string;
  lat_min: number;
  lat_max: number;
  lon_min: number;
  lon_max: number;
  record_limit: number;
}): Promise<any[]> {
  const url = new URL(`${BASE_URL}/lecturas`);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, String(value));
  });
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error('Error al obtener puntos visibles');
  return await res.json();
}

export async function fetchMedidorDetailByContratoID(
  contratoId: string,
  fechaHora: string
): Promise<any> {
  const res = await fetch(
    `${BASE_URL}/lecturas/buscar?fecha_hora=${encodeURIComponent(fechaHora)}&q=${contratoId}`
  );
  if (!res.ok) throw new Error('Error al obtener detalles del contrato');
  return await res.json();
}

export async function fetchIdentificarPorBusqueda(
  search: string,
  fechaHora: string
): Promise<any> {
  const res = await fetch(
    `${BASE_URL}/lecturas/identificar?fecha_hora=${encodeURIComponent(fechaHora)}&q=${search}`
  );
  if (!res.ok) return null;
  const data = await res.json();
  return Array.isArray(data) ? data[0] : data;
}


export async function fetchConsumoTotal(fechaHora: string): Promise<number> {
  const res = await fetch(`${BASE_URL}/dashboard/consumo_total?fecha_hora=${encodeURIComponent(fechaHora)}`);
  if (!res.ok) throw new Error('Error al obtener consumo total');
  const json = await res.json();
  return json.consumo_total;
}

export async function fetchMedidoresReportando(fechaHora: string): Promise<number> {
  const res = await fetch(`${BASE_URL}/dashboard/medidores_reportando?fecha_hora=${encodeURIComponent(fechaHora)}`);
  if (!res.ok) throw new Error('Error al obtener medidores reportando');
  const json = await res.json();
  return json.medidores_reportando;
}

export async function fetchMedidoresConErrores(fechaHora: string): Promise<number> {
  const res = await fetch(`${BASE_URL}/dashboard/medidores_con_errores?fecha_hora=${encodeURIComponent(fechaHora)}`);
  if (!res.ok) throw new Error('Error al obtener medidores con errores');
  const json = await res.json();
  return json.medidores_con_errores;
}

export async function fetchConsumoPromedio(fechaHora: string): Promise<number> {
  const res = await fetch(`${BASE_URL}/dashboard/consumo_promedio?fecha_hora=${encodeURIComponent(fechaHora)}`);
  if (!res.ok) throw new Error('Error al obtener consumo promedio');
  const json = await res.json();
  return json.consumo_promedio;
}


export async function fetchConsumoPorZona(fechaHora: string): Promise<{ nombre: string; valor: number }[]> {
  const res = await fetch(`${BASE_URL}/dashboard/consumo_por_zona?fecha_hora=${encodeURIComponent(fechaHora)}`);
  if (!res.ok) throw new Error('Error al obtener consumo por zona');
  const raw = await res.json();
  return Object.entries(raw).map(([nombre, valor]) => ({ nombre, valor }));
}

export async function fetchTopErrores(fechaHora: string): Promise<Array<{ tipo_error: string, cantidad: number }>> {
  const res = await fetch(`${BASE_URL}/dashboard/top_errores?fecha_hora=${encodeURIComponent(fechaHora)}`);
  if (!res.ok) throw new Error('Error al obtener el top de errores');
  return await res.json();
}





// Placeholder temporal para evitar errores en StatsDashboard
import dashboardStatsMock from '../mock/dashboardStats.json';

export async function fetchDashboardStats(zone: any, date: string) {
  return dashboardStatsMock;
}

