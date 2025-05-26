import React from 'react';
import ZoneTreeFilter from './ZoneTreeFilter';
import './Header.css';


interface Props {
  search: string;
  onSearch: (s: string) => void;
  zone: any;
  onZoneChange: (z: any) => void;
  date: string;
  onDateChange: (d: string) => void;
  time: string;
  onTimeChange: (t: string) => void;
  onSearchExecute: () => void;
}

const Header: React.FC<Props> = ({ 
  search, 
  onSearch, 
  zone, 
  onZoneChange, 
  date, 
  onDateChange,
  time,
  onTimeChange,
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
            onSearchExecute();
          }
        }}
      />
      <ZoneTreeFilter selected={zone} onChange={onZoneChange} />
      <input
        type="date"
        value={date}
        onChange={e => onDateChange(e.target.value)}
      />
      <input
        type="time"
        value={time}
        onChange={e => onTimeChange(e.target.value)}
      />
      <button onClick={onSearchExecute}>🔍</button>
    </div>
  </header>
);

export default Header;
