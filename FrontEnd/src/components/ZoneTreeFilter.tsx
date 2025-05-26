import React, { useEffect, useRef, useState } from 'react';
import type { Zone_v2 } from '../types';
import './ZoneTreeFilter.css';


interface Props {
  selected: Zone_v2;
  onChange: (z: Zone_v2) => void;
}

const ZoneTreeFilter: React.FC<Props> = ({ selected, onChange }) => {
  const [distList, setDistList] = useState<{ id: string; name: string }[]>([]);

  useEffect(() => {
    setDistList([
      { id: 'D0', name: 'Cercado' },
      { id: 'D1', name: 'Distrito 1' },
      { id: 'D2', name: 'Distrito 2' },
      { id: 'D3', name: 'Distrito 3' },
      { id: 'D4', name: 'Distrito 4' },
      { id: 'D5', name: 'Distrito 5' },
      { id: 'D6', name: 'Distrito 6' },
      { id: 'D7', name: 'Distrito 7' },
      { id: 'D8', name: 'Distrito 8' },
      { id: 'D9', name: 'Distrito 9' },
      { id: 'D10', name: 'Distrito 10' },
      { id: 'D11', name: 'Distrito 11' },
      { id: 'D12', name: 'Distrito 12' },
      { id: 'D13', name: 'Distrito 13' },
      { id: 'D14', name: 'Distrito 14' },
      { id: 'D15', name: 'Distrito 15' }
    ]);
  }, []);

  return (
    <select
      className="combo-distrito"
      value={selected.distritoId || ''}
      onChange={(e) => onChange({ distritoId: e.target.value || undefined })}
    >
      <option value="">Todos los distritos</option>
      {distList.map((d) => (
        <option key={d.id} value={d.id}>{d.name}</option>
      ))}
    </select>
  );
};

export default ZoneTreeFilter;
