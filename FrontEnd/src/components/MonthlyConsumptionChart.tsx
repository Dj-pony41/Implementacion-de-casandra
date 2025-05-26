import React from 'react';
import { Bar } from 'react-chartjs-2';

import './TopErroresChart.css';


interface Props {
  data: Array<{ tipo_error: string; cantidad: number }>;
}

const TopErroresChart: React.FC<Props> = ({ data }) => {
  const chartData = {
    labels: data.map(d => d.tipo_error),
    datasets: [
      {
        label: 'Cantidad de errores',
        data: data.map(d => d.cantidad),
        backgroundColor: 'rgba(255, 99, 132, 0.6)',
        borderColor: 'rgba(255, 99, 132, 1)',
        borderWidth: 1
      }
    ]
  };

  const chartOptions = {
    indexAxis: 'y' as const,
    plugins: {
      legend: {
        display: false
      }
    },
    responsive: true,
    maintainAspectRatio: false
  };

  return (
    <div className="top-errores-chart">
      <h4>Errores más frecuentes</h4>
      <div style={{ height: '250px' }}>
        <Bar data={chartData} options={chartOptions} />
      </div>
    </div>
  );
};

export default TopErroresChart;
