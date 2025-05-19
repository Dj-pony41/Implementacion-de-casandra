import React from 'react';
import { Bar } from 'react-chartjs-2';

interface Item { nombre: string; valor: number }
interface Props { data: Item[] }
const ZoneConsumptionChart: React.FC<Props> = ({ data }) => (
  <div className="zone-chart">
    <h4>Por Zona</h4>
<Bar
  key={JSON.stringify(data)}
  data={{
    labels: data.map(d => d.nombre),
    datasets: [{ data: data.map(d => d.valor) }]
  }}
  options={{
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    responsive: true,
    maintainAspectRatio: false
  }}
/>

  </div>
);
export default ZoneConsumptionChart;