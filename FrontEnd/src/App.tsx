import React, { useState, useCallback } from 'react';
import Header from './components/Header';
import MapView from './components/MapView';
import StatsDashboard from './components/StatsDashboard';
import HouseModal from './components/HouseModal';
import './App.css';
import type { Zone, MedidorDetail } from './types/index';

const App: React.FC = () => {
  const [search, setSearch] = useState('');
  const [zone, setZone] = useState<Zone>({});
  const [date, setDate] = useState<string>('2025-04-24');
  const [time, setTime] = useState<string>('08:00');
// Nuevo estado
  const [selected, setSelected] = useState<MedidorDetail | null>(null);
  const fechaHora = `${new Date(date).toISOString().slice(0, 10)} ${time}`;

   // Concatenado

  const handleSearchExecute = useCallback(() => {
    if (search.trim()) {
      window.dispatchEvent(new CustomEvent('execute-search', {
        detail: search
      }));
    }
  }, [search]);

  return (
    <div className="app-container">
      <Header
        search={search}
        onSearch={setSearch}
        zone={zone}
        onZoneChange={setZone}
        date={date}
        onDateChange={setDate}
        time={time}
        onTimeChange={setTime}
        onSearchExecute={handleSearchExecute}
      />
      <div className="main-content">
        <MapView
          search={search}
          zone={zone}
          date={fechaHora} // <- concatenado final
          onSelect={setSelected}
        />
        <StatsDashboard zone={zone} date={fechaHora} />
      </div>
    </div>
  );
};

export default App;
