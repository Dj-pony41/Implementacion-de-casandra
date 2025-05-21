// src/services/api.ts
import type { Zone, MedidorDetail } from '../types';

// URL base del backend
const API_BASE =  'http://localhost:8000';

/**
 * Obtiene los datos de contratos para el heatmap desde la API.
 * Devuelve array de objetos con la misma estructura que antes del mock.
 */
export async function fetchHeatmapData(
  zone: Zone,
  search: string,
  date: string
): Promise<Array<{
  Latitud: number;
  Longitud: number;
  ContratoID: string;
  Nombre: string;
  Email: string;
  Telefono: string;
  CI_NIT: number;
  Medidores: any[];
}>> {
  const params = new URLSearchParams({
    fecha_inicio: `${date}T00:00:00`,
    fecha_fin:    `${date}T23:59:59`,
  });
  const res = await fetch(`${API_BASE}/contratos?${params.toString()}`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Error cargando contratos: ${text}`);
  }
  return res.json();
}

// Las funciones de mock pueden mantenerse si aún las necesitas
import heatmapData from '../mock/heatmapData.json';
import medidorDetailMock from '../mock/medidorDetail.json';
import dashboardStatsMock from '../mock/dashboardStats.json';

export async function fetchMedidorDetail(id: string): Promise<MedidorDetail> {
  // Si tu API tiene endpoint para detalle, podrías hacer fetch aquí en lugar del mock
  return medidorDetailMock as MedidorDetail;
}

export async function fetchDashboardStats(zone: Zone, date: string) {
  return dashboardStatsMock;
}
