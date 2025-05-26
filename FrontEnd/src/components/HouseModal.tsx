import React, { useEffect, useRef } from 'react';
import Modal from 'react-modal';
import Chart from 'chart.js/auto';
import type { MedidorDetail } from '../types';

interface Props {
  medidor: MedidorDetail;
  onClose: () => void;
}

const HouseModal: React.FC<Props> = ({ medidor, onClose }) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const chartRef = useRef<Chart | null>(null);

  useEffect(() => {
    if (!canvasRef.current) return;

    // Destruir si ya existe
    if (chartRef.current) {
      chartRef.current.destroy();
    }

    chartRef.current = new Chart(canvasRef.current, {
      type: 'bar',
      data: {
        labels: medidor.histograma.map((_, i) => `${i}`),
        datasets: [
          {
            label: 'Consumo horario (m³)',
            data: medidor.histograma,
            backgroundColor: 'rgba(54, 162, 235, 0.5)'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { display: false }
        }
      }
    });

    return () => {
      chartRef.current?.destroy();
    };
  }, [medidor]);

  return (
    <Modal isOpen onRequestClose={onClose} className="house-modal">
      <h2>Cuenta: {medidor.cuenta}</h2>
      <p>Medidor: {medidor.medidor}</p>
      <p>Actual: {medidor.consumoActual} m³</p>
      <p>Acumulado: {medidor.consumoAcumulado} m³</p>
      <div style={{ height: '300px' }}>
        <canvas ref={canvasRef} />
      </div>
    </Modal>
  );
};

export default HouseModal;
