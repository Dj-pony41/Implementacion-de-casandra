import React, { useEffect, useState } from 'react';
import {
  fetchConsumoTotal,
  fetchMedidoresReportando,
  fetchMedidoresConErrores,
  fetchConsumoPromedio,
  fetchConsumoPorZona,
  fetchTopErrores
} from '../services/api';

import ZoneConsumptionChart from './ZoneConsumptionChart';
import Gauge from './Gauge';
import TopErroresChart from './MonthlyConsumptionChart';
import type { Zone } from '../types';

import './StatsDashboard.css';

interface Props {
  zone: Zone;
  date: string;
}

const StatsDashboard: React.FC<Props> = ({ zone, date }) => {
  const [consumoTotal, setConsumoTotal] = useState<number | null>(null);
  const [reportando, setReportando] = useState<number | null>(null);
  const [conErrores, setConErrores] = useState<number | null>(null);
  const [promedioOMS, setPromedioOMS] = useState<number | null>(null);
  const [porZona, setPorZona] = useState<Array<{ nombre: string; valor: number }> | null>(null);
  const [topErrores, setTopErrores] = useState<Array<{ tipo_error: string; cantidad: number }> | null>(null);

  useEffect(() => {
    fetchConsumoTotal(date).then(setConsumoTotal);
    fetchMedidoresReportando(date).then(setReportando);
    fetchMedidoresConErrores(date).then(setConErrores);
    fetchConsumoPromedio(date).then(setPromedioOMS);
    fetchConsumoPorZona(date).then(setPorZona);
    fetchTopErrores(date).then(setTopErrores);
  }, [date]);

  if (
    consumoTotal === null ||
    reportando === null ||
    conErrores === null ||
    promedioOMS === null ||
    porZona === null ||
    topErrores === null
  ) return null;

  return (
    <div className="stats-dashboard">
      <div className="top-stats">
        <div>
          <h3>Consumo m³/h</h3>
          <p className="big-number">{consumoTotal.toLocaleString()}</p>
        </div>
        <div>
          <h4>Medidores Reportando</h4>
          <p>{reportando.toLocaleString()}</p>
          <h4>Medidores con errores</h4>
          <p className="error-number">{conErrores.toLocaleString()}</p>
        </div>
      </div>

      <div className="chart-section">
        <TopErroresChart data={topErrores} />
        <ZoneConsumptionChart data={porZona} />
        <Gauge value={promedioOMS} />
        
      </div>
    </div>
  );
};

export default StatsDashboard;
