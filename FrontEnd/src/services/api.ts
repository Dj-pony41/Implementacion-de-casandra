// src/services/api.ts
import heatmapData from '../mock/heatmapData.json';
import medidorDetailMock from '../mock/medidorDetail.json';
import dashboardStatsMock from '../mock/dashboardStats.json';
import type { MedidorDetail, Zone } from '../types';

export async function fetchHeatmapData(
  zone: Zone,
  search: string,
  date: string
): Promise<{ lat: number; lng: number; consumo: number; id: string }[]> {
  // Simulación: filtramos localmente si quieres
  return heatmapData;
}

export async function fetchMedidorDetail(id: string): Promise<MedidorDetail> {
  // Devuelve el objeto cuyo id coincida (o el primero)
  return medidorDetailMock as MedidorDetail;
}

export async function fetchDashboardStats(zone: Zone, date: string) {
  return dashboardStatsMock;
}
