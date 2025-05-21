import React from 'react';
import ZoneTreeFilter from './ZoneTreeFilter';

interface Props {
  search: string;
  onSearch: (s: string) => void;
  zone: any;
  onZoneChange: (z: any) => void;
  date: string;
  onDateChange: (d: string) => void;
  onSearchExecute: () => void; // Nueva prop para ejecutar la búsqueda
}

const Header: React.FC<Props> = ({ 
  search, 
  onSearch, 
  zone, 
  onZoneChange, 
  date, 
  onDateChange,
  onSearchExecute 
}) => (
  <header className="header">
    <div className="logo">SEMAPA</div>
    <div className="controls">
      <input
        type="text"
        placeholder="Buscar contrato, medidor o cliente"
        value={search}
        onChange={e => onSearch(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter') {
            onSearchExecute(); // Llama a la función de búsqueda
          }
        }}
      />
      <ZoneTreeFilter selected={zone} onChange={onZoneChange} />
      <input
        type="date"
        value={date}
        onChange={e => onDateChange(e.target.value)}
      />
    </div>
  </header>
);

export default Header;