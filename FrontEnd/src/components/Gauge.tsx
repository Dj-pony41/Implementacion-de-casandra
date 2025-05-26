import React from 'react';
import './CustomGauge.css';

import {
  GaugeContainer,
  GaugeValueArc,
  GaugeReferenceArc,
  useGaugeState
} from '@mui/x-charts/Gauge';

interface Props {
  value: number;
}

const CustomGauge: React.FC<Props> = ({ value }) => {
  const MIN = 0;
  const MAX = 180;
  const OMS = 100;

  return (
    <div className="gauge" style={{ textAlign: 'center', paddingTop: '10px' }}>
      <h4 style={{ fontSize: '14px', marginBottom: '10px' }}>
        Cantidad de agua promedio que un habitante está consumiendo por encima o debajo de los estándares de la OMS
      </h4>

      <GaugeContainer
        width={240}
        height={160}
        startAngle={-90}
        endAngle={90}
        value={value}
        valueMin={MIN}
        valueMax={MAX}
      >
        {/* Zona crítica izquierda */}
        <GaugeReferenceArc start={MIN} end={50} color="#e57373" />

        {/* Zona segura */}
        <GaugeReferenceArc start={50} end={100} color="#64b5f6" />

        {/* Zona crítica derecha */}
        <GaugeReferenceArc start={100} end={MAX} color="#e57373" />

        {/* Aguja */}
        <GaugeValueArc />

        {/* Valor numérico */}
        <text
          x="120"
          y="90"
          textAnchor="middle"
          fontSize="16"
          fill="#333"
          fontWeight="bold"
        >
          {value.toFixed(1)} L/hab
        </text>

        {/* Ícono de persona */}
        <text
          x="120"
          y="140"
          textAnchor="middle"
          fontSize="22"
        >
          👤
        </text>
      </GaugeContainer>
    </div>
  );
};

export default CustomGauge;
