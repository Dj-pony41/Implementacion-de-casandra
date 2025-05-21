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

export interface Zone_v2 {
  subAlcaldiaId?: number;
  distritoId?: String;
  zonaId?: number;
}




// src/types/index.ts

export interface Medidor {
  CodigoMedidor: string
  Modelo:        string
  Estado:        string
  FechaHora:     string   // o Date si parseas en el front
  Lectura:       number
  ConsumoPeriodo:number
  TarifaUSD:     string
}

export interface ContratoResponse {
  ContratoID: string
  Nombre:     string
  CI_NIT:     number
  Email:      string
  Telefono:   string
  Latitud:    number
  Longitud:   number
  Distrito:   string
  Zona:       string
  Medidores:  Medidor[]
}
