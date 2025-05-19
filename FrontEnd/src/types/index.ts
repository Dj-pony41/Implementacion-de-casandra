export interface MedidorDetail {
  id: string;
  cuenta: string;
  medidor: string;
  consumoActual: number;
  consumoAcumulado: number;
  histograma: number[];
  lat: number;
  lng: number;
}

export interface Zone {
  subAlcaldiaId?: number;
  distritoId?: number;
  zonaId?: number;
}