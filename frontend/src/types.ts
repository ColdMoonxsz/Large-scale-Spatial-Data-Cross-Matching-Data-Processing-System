export type BBox = { minx: number; miny: number; maxx: number; maxy: number };

export type TaskCreatePayload = {
  dataset_a: string;
  dataset_b: string;
  bbox?: BBox;
  grids?: string[];
};

export type PolygonFeature = {
  id: string | number;
  area: number;
  geometry: {
    type: string;
    coordinates: any;
  };
};

