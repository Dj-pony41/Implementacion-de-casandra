import React, { useEffect, useState } from 'react';
import { Bar } from 'react-chartjs-2';
import './zoneConsumption.css';

interface Item {
  nombre: string;
  valor: number;
}

interface Props {
  data: Item[];
}

const ZoneConsumptionChart: React.FC<Props> = ({ data }) => {
  const chartData = {
    labels: data.map(d => d.nombre),
    datasets: [
      {
        label: 'Consumo por zona (m³/h)',
        data: data.map(d => d.valor),
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgba(75, 192, 192, 1)',
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
    maintainAspectRatio: false,
    scales: {
      x: {
        ticks: {
          callback: (value: number) => value.toLocaleString()
        }
      }
    }
  };

  return (
    <div className="zone-chart">
      <h4>Por Zona</h4>
      <div style={{ height: '250px' }}>
        <Bar key={JSON.stringify(data)} data={chartData} options={chartOptions} />
      </div>
    </div>
  );
};

export default ZoneConsumptionChart;