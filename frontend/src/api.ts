import axios from "axios";
import { BBox, TaskCreatePayload } from "./types";

const client = axios.create({
  baseURL: "/",
});

export async function uploadDataset(prefix: string, file: File, onProgress?: (percent: number) => void) {
  const form = new FormData();
  form.append("prefix", prefix);
  form.append("file", file);
  const res = await client.post("/api/datasets/upload", form, {
    headers: { "Content-Type": "multipart/form-data" },
    onUploadProgress: (evt) => {
      if (!onProgress || !evt.total) return;
      const percent = Math.round((evt.loaded / evt.total) * 100);
      onProgress(percent);
    },
  });
  return res.data;
}

export async function createTask(payload: TaskCreatePayload) {
  const res = await client.post("/api/tasks", payload);
  return res.data;
}

export async function getTask(taskId: string) {
  const res = await client.get(`/api/tasks/${taskId}`);
  return res.data;
}

export async function getResult(taskId: string) {
  const res = await client.get(`/api/results/${taskId}`);
  return res.data;
}

export async function getPolygons(dataset: string, bbox: BBox, limit = 3000) {
  const params = { dataset, limit, ...bbox };
  const res = await client.get("/api/regions/polygons", { params });
  return res.data;
}

export async function getRegionStats(dataset_a: string, dataset_b: string, bbox?: BBox) {
  const params: any = { dataset_a, dataset_b };
  if (bbox) {
    params.minx = bbox.minx;
    params.miny = bbox.miny;
    params.maxx = bbox.maxx;
    params.maxy = bbox.maxy;
  }
  const res = await client.get("/api/regions/stats", { params });
  return res.data;
}

