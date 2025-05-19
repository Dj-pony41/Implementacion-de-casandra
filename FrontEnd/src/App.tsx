import React, { useState } from 'react';
import Header from './components/Header';
import MapView from './components/MapView';
import StatsDashboard from './components/StatsDashboard';
import HouseModal from './components/HouseModal';
import './App.css';
import type { Zone, MedidorDetail } from './types/index';

const App: React.FC = () => {
  const [search, setSearch] = useState('');
  const [zone, setZone] = useState<Zone>({});
  const [date, setDate] = useState<string>(new Date().toISOString().slice(0,10));
  const [selected, setSelected] = useState<MedidorDetail | null>(null);

  return (
    <div className="app-container">
      <Header
        search={search}
        onSearch={setSearch}
        zone={zone}
        onZoneChange={setZone}
        date={date}
        onDateChange={setDate}
      />
      <div className="main-content">
        <MapView
          search={search}
          zone={zone}
          date={date}
          onSelect={setSelected}
        />
        <StatsDashboard zone={zone} date={date} />
      </div>
      {selected && (
        <HouseModal medidor={selected} onClose={() => setSelected(null)} />
      )}
    </div>
  );
};

export default App;