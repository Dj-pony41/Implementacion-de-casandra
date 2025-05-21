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
  const [popupTargetId, setPopupTargetId] = useState<string | null>(null);
  const [allPoints, setAllPoints] = useState<any[]>([]);
  const [visiblePoints, setVisiblePoints] = useState<any[]>([]);
  const [zoom, setZoom] = useState<number>(12);
  const [districtShapes, setDistrictShapes] = useState<Record<string, [number, number][]>>({});
  const mapRef = useRef<Map | null>(null);
  const updateTimeoutRef = useRef<NodeJS.Timeout>();
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
    fetchHeatmapData(zone, search, date).then((casasConLecturas) => {
      setAllPoints(casasConLecturas);
    });
  }, [zone, search, date]);

  const updateVisiblePoints = useCallback(() => {
    if (!mapRef.current || allPoints.length === 0) return;
    if (updateTimeoutRef.current) clearTimeout(updateTimeoutRef.current);

    updateTimeoutRef.current = setTimeout(() => {
      const currentZoom = mapRef.current!.getZoom();
      setZoom(currentZoom);

      const bounds = mapRef.current!.getBounds();
      let filtered = allPoints.filter(p => bounds.contains([p.Latitud, p.Longitud]));

      const key = zone.distritoId?.toString();
      if (key && districtShapes[key]) {
        const shape = polygon([districtShapes[key].map(([lat, lng]) => [lng, lat])]);
        filtered = filtered.filter(p => booleanPointInPolygon(point([p.Longitud, p.Latitud]), shape));
      }

      setVisiblePoints(filtered);

      if (search.length > 0 && mapRef.current) {
        const lower = search.toLowerCase();
        const coincidencias = allPoints.filter(p =>
          p.ContratoID?.toLowerCase().includes(lower) ||
          p.Nombre?.toLowerCase().includes(lower) ||
          p.Medidores?.some((m: any) => m.CodigoMedidor?.toLowerCase().includes(lower))
        );

        if (coincidencias.length === 0) {
          alert('No se encontró ninguna coincidencia');
        } else if (coincidencias.length === 1) {
          mapRef.current.setView([coincidencias[0].Latitud, coincidencias[0].Longitud], 18);
          setPopupTargetId(coincidencias[0].ContratoID);
        } else {
          const bounds = new LatLngBounds(coincidencias.map(p => [p.Latitud, p.Longitud]));
          mapRef.current.fitBounds(bounds, { padding: [20, 20] });
        }
      }
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

      {zoom >= 12 && visiblePoints.length > 0 && (
        <HeatmapLayer
          fitBoundsOnLoad={false}
          fitBoundsOnUpdate={false}
          points={visiblePoints.map(c => ({ lat: c.Latitud, lng: c.Longitud, consumo: 1 }))}
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

      {zoom >= 14 && visiblePoints.map((casa) => (
        <CircleMarker
          key={casa.ContratoID}
          center={[casa.Latitud, casa.Longitud]}
          radius={6}
          fillOpacity={0.8}
          pathOptions={{ color: 'blue' }}
          ref={ref => {
            if (ref && popupTargetId === casa.ContratoID) {
              setTimeout(() => ref.openPopup(), 300);
            }
          }}>
          <Popup>
            <MedidorTabs casa={casa} />
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
};

const MedidorTabs: React.FC<{ casa: any }> = ({ casa }) => {
  const [active, setActive] = useState(0);
  const medidor = casa.Medidores[active];

  return (
    <div style={{ minWidth: '220px' }}>
      <strong>Nombre:</strong> {casa.Nombre}<br />
      <strong>CI/NIT:</strong> {casa['CI/NIT']}<br />
      <strong>Teléfono:</strong> {casa.Telefono}<br />
      <strong>Email:</strong> {casa.Email}<br />
      <strong>Contrato:</strong> {casa.ContratoID}<br />

      {casa.Medidores.length > 1 && (
        <div style={{ marginTop: 8, display: 'flex', gap: 4 }}>
          {casa.Medidores.map((m: any, i: number) => (
            <button
              key={i}
              style={{ fontWeight: i === active ? 'bold' : 'normal' }}
              onClick={() => setActive(i)}
            >
              {m.CodigoMedidor}
            </button>
          ))}
        </div>
      )}

      <hr />
      <strong>Medidor:</strong> {medidor.CodigoMedidor}<br />
      <strong>Modelo:</strong> {medidor.Modelo}<br />
      <strong>Lectura:</strong> {medidor.Lectura} m³<br />
      <strong>Consumo:</strong> {medidor.ConsumoPeriodo} m³<br />
      <strong>Tarifa:</strong> {medidor.TarifaUSD}<br />
      $1

      <div style={{ marginTop: '0.5rem', display: 'flex', justifyContent: 'center', gap: '0.5rem' }}>
        <button
          title="WhatsApp"
          onClick={() => window.open(`http://localhost:3000/enviar?tipo=whatsapp&codigo=${medidor.CodigoMedidor}`, '_blank')}
        >
          📱
        </button>
        <button
          title="SMS"
          onClick={() => window.open(`http://localhost:3000/enviar?tipo=sms&codigo=${medidor.CodigoMedidor}`, '_blank')}
        >
          ✉️
        </button>
        <button
          title="Correo"
          onClick={() => window.open(`http://localhost:3000/enviar?tipo=correo&codigo=${medidor.CodigoMedidor}`, '_blank')}
        >
          📧
        </button>
      </div>
    </div>
  );
};

export default React.memo(MapView);
