import React, { useEffect, useState } from 'react';
import type { Zone } from '../types';

interface Props {
  selected: Zone;
  onChange: (z: Zone) => void;
}

const ZoneTreeFilter: React.FC<Props> = ({ selected, onChange }) => {
  const [distList, setDistList] = useState<{ id: number; name: string }[]>([]);

  useEffect(() => {
    setDistList([
      { id: 1, name: 'Distrito 1' },
      { id: 2, name: 'Distrito 2' },
      { id: 3, name: 'Distrito 3' },
      { id: 4, name: 'Distrito 4' },
      { id: 5, name: 'Distrito 5' }
    ]);
  }, []);

  return (
    <select
      className="combo-distrito"
      value={selected.distritoId || ''}
      onChange={(e) => onChange({ distritoId: +e.target.value })}
    >
      <option value="">Todos los distritos</option>
      {distList.map((d) => (
        <option key={d.id} value={d.id}>{d.name}</option>
      ))}
    </select>
  );
};

export default ZoneTreeFilter;