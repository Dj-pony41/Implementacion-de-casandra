import React, { useEffect, useState } from 'react';
import { fetchDashboardStats } from '../services/api';
import MonthlyConsumptionChart from './MonthlyConsumptionChart';
import ZoneConsumptionChart from './ZoneConsumptionChart';
import Gauge from './Gauge';
import type { Zone } from '../types';

interface Props {
  zone: Zone;
  date: string;
}

const StatsDashboard: React.FC<Props> = ({ zone, date }) => {
  const [stats, setStats] = useState<any>(null);

  useEffect(() => {
    fetchDashboardStats(zone, date).then(setStats);
  }, [zone, date]);

  if (!stats) return null;

  return (
    <div className="stats-dashboard">
      <div className="top-stats">
        <div>
          <h3>Consumo m³/h</h3>
          <p className="big-number">{stats.ciudadHora.toLocaleString()}</p>
        </div>
        <div>
          <h4>Medidores Reportando</h4>
          <p>{stats.reportando.toLocaleString()}</p>
          <h4>Medidores con errores</h4>
          <p className="error-number">{stats.conErrores.toLocaleString()}</p>
        </div>
      </div>
      <div className="chart-section">
        <MonthlyConsumptionChart data={stats.mensual} />
        <ZoneConsumptionChart data={stats.porZona} />
        <Gauge value={stats.promedioOMS} />
      </div>
    </div>
  );
};

export default StatsDashboard;