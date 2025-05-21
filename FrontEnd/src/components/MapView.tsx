import React, { useEffect, useRef, useState, useCallback } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, Polygon, useMap } from 'react-leaflet';
import { HeatmapLayer } from 'react-leaflet-heatmap-layer-v3';
import 'leaflet/dist/leaflet.css';
import { fetchHeatmapData } from '../services/api';
import type { Zone } from '../types';
import { LatLngBounds, Map } from 'leaflet';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point, polygon } from '@turf/helpers';

interface Props {
  search: string;
  zone: Zone;
  date: string;
  onSelect: (id: any) => void;
}

const MapView: React.FC<Props> = ({ search, zone, date, onSelect }) => {
  const [allPoints, setAllPoints] = useState<any[]>([]);
  const [visiblePoints, setVisiblePoints] = useState<any[]>([]);
  const [zoom, setZoom] = useState<number>(12);
  const mapRef = useRef<Map | null>(null);
  const updateTimeoutRef = useRef<NodeJS.Timeout>();
  const [districtShapes, setDistrictShapes] = useState<Record<string, [number, number][]>>({});
  const prevDistritoIdRef = useRef<string | undefined>();

  useEffect(() => {
    fetch('/data/distritos.geojson')
      .then(res => res.json())
      .then(data => {
        const dict: Record<string, [number, number][]> = {};
        data.features.forEach((f: any) => {
          const id = f.properties.name;
          const coords = f.geometry.coordinates[0].map(([lng, lat]: [number, number]) => [lat, lng]);
          dict[id] = coords;
        });
        setDistrictShapes(dict);
      });
  }, []);

  useEffect(() => {
    fetchHeatmapData(zone, search, date).then((points) => {
      setAllPoints(points);
    });
  }, [zone, search, date]);

  const updateVisiblePoints = useCallback(() => {
    if (!mapRef.current || allPoints.length === 0) return;
    if (updateTimeoutRef.current) clearTimeout(updateTimeoutRef.current);

    updateTimeoutRef.current = setTimeout(() => {
      const currentZoom = mapRef.current!.getZoom();
      setZoom(currentZoom);

      const bounds = mapRef.current!.getBounds();
      let filtered = allPoints.filter(p => bounds.contains([p.lat, p.lng]));

      const key = zone.distritoId?.toString();
      if (key && districtShapes[key]) {
        const shape = polygon([districtShapes[key].map(([lat, lng]) => [lng, lat])]);
        filtered = filtered.filter(p => booleanPointInPolygon(point([p.lng, p.lat]), shape));
      }

      setVisiblePoints(filtered);
    }, 100);
  }, [allPoints, zone.distritoId, districtShapes]);

  function MapZoomWatcher() {
    const map = useMap();

    useEffect(() => {
      if (!map) return;
      mapRef.current = map;
      map.on('zoomend moveend', updateVisiblePoints);
      updateVisiblePoints();

      return () => {
        map.off('zoomend moveend', updateVisiblePoints);
        if (updateTimeoutRef.current) clearTimeout(updateTimeoutRef.current);
      };
    }, [map, updateVisiblePoints]);

    useEffect(() => {
      const id = zone.distritoId;
      const coords = id ? districtShapes[id] : null;

      if (mapRef.current && coords && id !== prevDistritoIdRef.current) {
        const bounds = new LatLngBounds(coords);
        mapRef.current.fitBounds(bounds, { padding: [20, 20] });
        prevDistritoIdRef.current = id;
      }
    }, [zone.distritoId, districtShapes]);

    return null;
  }

  const currentPolygon = zone.distritoId ? districtShapes[zone.distritoId.toString()] : null;

  return (
    <MapContainer
      center={[-17.39, -66.16]}
      zoom={zoom}
      style={{ height: '100%', width: '100%', zIndex: 1 }}
      preferCanvas={true}
    >
      <MapZoomWatcher />
      <TileLayer 
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      />

      {currentPolygon && (
        <Polygon
          positions={currentPolygon}
          pathOptions={{ color: 'green', fillOpacity: 0.2 }}
        />
      )}

      {zoom >= 10 && visiblePoints.length > 0 && (
        <HeatmapLayer
          fitBoundsOnLoad={false}
          fitBoundsOnUpdate={false}
          points={visiblePoints}
          longitudeExtractor={(m: any) => m.lng}
          latitudeExtractor={(m: any) => m.lat}
          intensityExtractor={(m: any) => m.consumo}
          radius={18}
          blur={15}
          max={1000}
          gradient={{
            0.0: 'rgba(255, 255, 255, 0)',
            0.4: 'rgba(255, 204, 0, 0.25)',
            0.6: 'rgba(255, 102, 0, 0.35)',
            0.8: 'rgba(255, 0, 0, 0.4)',
            1.0: 'rgba(153, 0, 0, 0.5)'
          }}
        />
      )}

      {zoom >= 14 && visiblePoints.map((p) => (
        <CircleMarker
          key={p.id}
          center={[p.lat, p.lng]}
          radius={5}
          fillOpacity={0.8}
          pathOptions={{ color: 'blue' }}
        >
          <Popup>
            <div style={{ minWidth: '180px' }}>
              <strong>Cuenta:</strong> {p.cuenta || p.id}<br />
              <strong>Medidor:</strong> {p.id}<br />
              <strong>Actual:</strong> {p.consumoActual || p.consumo} m³<br />
              <strong>Acumulado:</strong> {p.consumoTotal || p.consumo} m³
              <div style={{ marginTop: '0.5rem', display: 'flex', gap: '0.25rem' }}>
                <button onClick={() => window.open(`https://wa.me/?text=Consulta%20sobre%20medidor%20${p.id}`, '_blank')}>WhatsApp</button>
                <button onClick={() => window.open(`sms:?body=Consulta%20sobre%20medidor%20${p.id}`)}>SMS</button>
                <button onClick={() => window.open(`mailto:?subject=Consulta%20medidor%20${p.id}&body=Estoy%20consultando%20sobre%20el%20medidor%20${p.id}`)}>Correo</button>
              </div>
            </div>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
};

export default React.memo(MapView);
