import React, { useEffect, useRef, useState, useCallback } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, Polygon, useMap } from 'react-leaflet';
import { HeatmapLayer } from 'react-leaflet-heatmap-layer-v3';
import 'leaflet/dist/leaflet.css';
import { fetchVisiblePoints, fetchMedidorDetailByContratoID, fetchIdentificarPorBusqueda } from '../services/api';
import type { Zone } from '../types';
import { LatLngBounds, Map } from 'leaflet';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point, polygon } from '@turf/helpers';
import './MapView.css';

import { FaWhatsapp } from 'react-icons/fa';
import { MdSms, MdEmail } from 'react-icons/md';

interface Props {
  search: string;
  zone: Zone;
  date: string;
  onSelect: (data: any) => void;
  onSearchExecute: () => void;
  recordLimit?: number;
}

const MapView: React.FC<Props> = ({ search, zone, date, onSelect, onSearchExecute, recordLimit }) => {
  const [popupTargetId, setPopupTargetId] = useState<string | null>(null);
  const [allPoints, setAllPoints] = useState<any[]>([]);
  const [visiblePoints, setVisiblePoints] = useState<any[]>([]);
  const [zoom, setZoom] = useState<number>(12);
  const [districtShapes, setDistrictShapes] = useState<Record<string, [number, number][]>>({});
  const [limit, setLimit] = useState<number>(recordLimit ?? 500);
  const [selectedDetail, setSelectedDetail] = useState<any | null>(null);
  const mapRef = useRef<Map | null>(null);
  const markerRefs = useRef<Record<string, any>>({});
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

  const updateVisiblePoints = useCallback(() => {
    if (!mapRef.current) return;
    if (updateTimeoutRef.current) clearTimeout(updateTimeoutRef.current);

    updateTimeoutRef.current = setTimeout(async () => {
      const currentZoom = mapRef.current!.getZoom();
      setZoom(currentZoom);

      const bounds = mapRef.current!.getBounds();
      const fechaHora = date;

      const lat_min = bounds.getSouthWest().lat;
      const lat_max = bounds.getNorthEast().lat;
      const lon_min = bounds.getSouthWest().lng;
      const lon_max = bounds.getNorthEast().lng;

      const data = await fetchVisiblePoints({
        fecha_hora: fechaHora,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        record_limit: limit,
      });

      const key = zone.distritoId?.toString();
      let filtered = data;
      if (key && districtShapes[key]) {
        const shape = polygon([districtShapes[key].map(([lat, lng]) => [lng, lat])]);
        filtered = data.filter(p => booleanPointInPolygon(point([p.Longitud, p.Latitud]), shape));
      }

      setAllPoints(filtered);
      setVisiblePoints(filtered);
    }, 100);
  }, [date, zone.distritoId, districtShapes, limit]);

  useEffect(() => {
    const handler = async (e: any) => {
      const searchTerm = e.detail?.trim();
      if (!searchTerm) return;

      const fechaHora = date;
      const match = await fetchIdentificarPorBusqueda(searchTerm, fechaHora);
      if (!match) {
        alert('No se encontró ninguna coincidencia');
      } else {
        const { Latitud, Longitud, ContratoID } = match;

        setSelectedDetail(match);
        setPopupTargetId(ContratoID);

        setVisiblePoints(prev => {
          const exists = prev.some(p => p.ContratoID === ContratoID);
          if (!exists) return [...prev, match];
          return prev.map(p => p.ContratoID === ContratoID ? match : p);
        });

        setTimeout(() => {
          mapRef.current?.setView([Latitud, Longitud], 18);
          const ref = markerRefs.current[ContratoID];
          if (ref) ref.openPopup();
        }, 300);
      }
    };

    window.addEventListener('execute-search', handler);
    return () => window.removeEventListener('execute-search', handler);
  }, [search, date]);

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
    <div className="map-container">
      <div className="map-controls">
        <label style={{ marginRight: 6 }}>Máx puntos:</label>
        <select value={limit} onChange={e => setLimit(Number(e.target.value))}>
          <option value={100}>100</option>
          <option value={500}>500</option>
          <option value={1000}>1000</option>
          <option value={5000}>5000</option>
          <option value={10000}>10000</option>
          <option value={30000}>Todos (30 000)</option>
        </select>
      </div>

      <MapContainer center={[-17.39, -66.16]} zoom={zoom} style={{ height: '100%', width: '100%', zIndex: 1 }} preferCanvas={true}>
        <MapZoomWatcher />
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution='&copy; OpenStreetMap contributors' />

        {currentPolygon && (
          <Polygon positions={currentPolygon} pathOptions={{ color: 'green', fillOpacity: 0.2 }} />
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
          />
        )}

        {zoom >= 14 && visiblePoints.map((casa) => (
          <CircleMarker
            key={casa.ContratoID}
            center={[casa.Latitud, casa.Longitud]}
            radius={6}
            fillOpacity={0.8}
            pathOptions={{ color: popupTargetId === casa.ContratoID ? 'red' : 'blue' }}
            ref={(ref) => { if (ref) markerRefs.current[casa.ContratoID] = ref; }}
            eventHandlers={{
              click: async () => {
                const detalle = await fetchMedidorDetailByContratoID(casa.ContratoID, date);
                setSelectedDetail(detalle);
                setPopupTargetId(detalle.ContratoID);
                setVisiblePoints(prev =>
                  prev.map(p => (p.ContratoID === detalle.ContratoID ? detalle : p))
                );
              },
            }}
          >
            <Popup onClose={() => setPopupTargetId(null)}>
              {popupTargetId === casa.ContratoID && selectedDetail?.ContratoID === casa.ContratoID
                ? <MedidorTabs casa={selectedDetail}  fechaHora={date}/>
                : 'Cargando...'}
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </div>
  );
};

const MedidorTabs: React.FC<{ casa: any; fechaHora: string }> = ({ casa, fechaHora }) => {
  const [active, setActive] = useState(0);
  const medidor = casa.Medidores[active];
  if (!medidor || typeof medidor === 'string') return <div>Cargando medidores...</div>;

  return (
    <div className="popup-content">
      <strong>Nombre:</strong> {casa.Nombre}<br />
      <strong>CI/NIT:</strong> {casa.CI_NIT}<br />
      <strong>Teléfono:</strong> {casa.Telefono}<br />
      <strong>Email:</strong> {casa.Email}<br />
      <strong>Contrato:</strong> {casa.ContratoID}<br />

      {Array.isArray(casa.Medidores) && casa.Medidores.length > 1 && (
        <div className="medidor-tabs">
          {casa.Medidores.map((m: any, i: number) => (
            <button
              key={i}
              className={i === active ? 'active' : ''}
              onClick={() => setActive(i)}
            >
              {m.CodigoMedidor || m}
            </button>
          ))}
        </div>
      )}

      {medidor && typeof medidor === 'object' && (
        <>
          <hr />
          <strong>Medidor:</strong> {medidor.CodigoMedidor}<br />
          <strong>Modelo:</strong> {medidor.Modelo}<br />
          <strong>Lectura:</strong> {medidor.Lectura} m³<br />
          <strong>Consumo:</strong> {medidor.ConsumoPeriodo} m³<br />
          <strong>Tarifa:</strong> {medidor.TarifaUSD}<br />

          <div className="contact-buttons">
            <button
              title="WhatsApp"
              onClick={() =>
                window.open(
                  `http://localhost:8001/enviar?tipo=whatsapp&codigo=${medidor.CodigoMedidor}&fecha=${encodeURIComponent(fechaHora)}`,
                  '_blank'
                )
              }
            >
              <FaWhatsapp size={20} color="#25D366" />
            </button>
            <button
              title="SMS"
              onClick={() =>
                window.open(
                  `http://localhost:8001/enviar?tipo=sms&codigo=${medidor.CodigoMedidor}&fecha=${encodeURIComponent(fechaHora)}`,
                  '_blank'
                )
              }
            >
              <MdSms size={20} color="#2196F3" />
            </button>
            <button
              title="Correo"
              onClick={() =>
                window.open(
                  `http://localhost:8001/enviar?tipo=correo&codigo=${medidor.CodigoMedidor}&fecha=${encodeURIComponent(fechaHora)}`,
                  '_blank'
                )
              }
            >
              <MdEmail size={20} color="#F44336" />
            </button>
          </div>

          </>
        )}
    </div>
  );
};

export default React.memo(MapView);
