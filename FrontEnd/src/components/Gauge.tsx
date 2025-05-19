import React from 'react';
interface Props { value: number }
const Gauge: React.FC<Props> = ({ value }) => (
  <div className="gauge">
    <h4>Consumo vs OMS</h4>
    <div className="gauge-chart">
      {/* TODO: implementar gauge con SVG o librería */}
      <p>{value}%</p>
    </div>
  </div>
);
export default Gauge;