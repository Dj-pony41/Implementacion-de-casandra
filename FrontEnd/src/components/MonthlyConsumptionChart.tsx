import React from 'react';
import { Bar } from 'react-chartjs-2';

interface Props { data: number[] }
const MonthlyConsumptionChart: React.FC<Props> = ({ data }) => (
  <div className="monthly-chart">
    <h4>Distribución mensual</h4>
<Bar
  key={JSON.stringify(data)}
  data={{
    labels: ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago'],
    datasets: [{ data }]
  }}
  options={{
    plugins: { legend: { display: false } },
    responsive: true,
    maintainAspectRatio: false
  }}
/>

  </div>
);
export default MonthlyConsumptionChart;