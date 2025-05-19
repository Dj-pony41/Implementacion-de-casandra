declare module 'react-leaflet-heatmap-layer-v3' {
  import { ComponentType } from 'react';

  export interface HeatmapLayerProps {
    points: any[];
    longitudeExtractor: (p: any) => number;
    latitudeExtractor: (p: any) => number;
    intensityExtractor: (p: any) => number;
    radius?: number;
    blur?: number;
    max?: number;
    fitBoundsOnLoad?: boolean;
    fitBoundsOnUpdate?: boolean;
  }

  const HeatmapLayer: ComponentType<HeatmapLayerProps>;
  export default HeatmapLayer;
}
