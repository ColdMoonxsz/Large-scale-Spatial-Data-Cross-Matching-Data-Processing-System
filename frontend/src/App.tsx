import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import {
  Badge,
  Button,
  Card,
  Col,
  Divider,
  Form,
  InputNumber,
  Layout,
  Row,
  Space,
  Statistic,
  Tag,
  Typography,
  Upload,
  message,
} from "antd";
import { UploadOutlined, PlayCircleOutlined, ReloadOutlined, AimOutlined, RocketOutlined } from "@ant-design/icons";
import { DeckGL } from "@deck.gl/react";
import { PolygonLayer } from "@deck.gl/layers";
import { OrthographicView } from "@deck.gl/core";
import { createTask, getPolygons, getRegionStats, getTask, uploadDataset } from "./api";
import { BBox, PolygonFeature } from "./types";

const { Title, Text } = Typography;
const GLOBAL_BOUNDS: BBox = { minx: -120, miny: -120, maxx: 120, maxy: 120 };

type DatasetUploadState = { file?: File; prefix: string };

function CanvasPreview({
  polygonsA,
  polygonsB,
  bbox,
  width = 800,
  height = 520,
}: {
  polygonsA: PolygonFeature[];
  polygonsB: PolygonFeature[];
  bbox: BBox;
  width?: number;
  height?: number;
}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [viewState, setViewState] = useState({ scale: 1, offsetX: 0, offsetY: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const containerRef = useRef<HTMLDivElement | null>(null);

  // 计算数据边界
  const dataBounds = useMemo(() => {
    const collectRings = (features: PolygonFeature[]) => {
      const rings: number[][][] = [];
      features.forEach((f) => {
        const coords = f.geometry.coordinates;
        const ring = Array.isArray(coords[0][0]) ? coords[0] : coords;
        rings.push(ring as number[][]);
      });
      return rings;
    };

    const ringsA = collectRings(polygonsA);
    const ringsB = collectRings(polygonsB);

    let minx = bbox.minx;
    let maxx = bbox.maxx;
    let miny = bbox.miny;
    let maxy = bbox.maxy;
    const extendBounds = (rings: number[][][]) => {
      rings.forEach((ring) =>
        ring.forEach(([x, y]) => {
          minx = Math.min(minx, x);
          maxx = Math.max(maxx, x);
          miny = Math.min(miny, y);
          maxy = Math.max(maxy, y);
        })
      );
    };
    extendBounds(ringsA);
    extendBounds(ringsB);

    return { minx, maxx, miny, maxy, ringsA, ringsB };
  }, [polygonsA, polygonsB, bbox]);

  // 坐标转换函数
  const toPx = useCallback(
    (x: number, y: number) => {
      const pad = 20;
      const { minx, maxx, miny, maxy } = dataBounds;
      const baseScale = Math.min(
        (width - pad * 2) / (maxx - minx || 1),
        (height - pad * 2) / (maxy - miny || 1)
      );
      const scale = baseScale * viewState.scale;
      return [
        pad + (x - minx) * scale + viewState.offsetX,
        height - pad - (y - miny) * scale + viewState.offsetY,
      ];
    },
    [dataBounds, width, height, viewState]
  );

  // 绘制
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#fcfcfc";
    ctx.fillRect(0, 0, width, height);

    const drawRings = (rings: number[][][], stroke: string, fill: string) => {
      rings.forEach((ring) => {
        ctx.beginPath();
        ring.forEach(([x, y], idx) => {
          const [px, py] = toPx(x, y);
          if (idx === 0) ctx.moveTo(px, py);
          else ctx.lineTo(px, py);
        });
        ctx.closePath();
        ctx.fillStyle = fill;
        ctx.strokeStyle = stroke;
        ctx.lineWidth = 1.2 / viewState.scale;
        ctx.fill();
        ctx.stroke();
      });
    };

    drawRings(dataBounds.ringsA, "rgba(220,20,60,0.9)", "rgba(220,20,60,0.25)");
    drawRings(dataBounds.ringsB, "rgba(34,139,34,0.9)", "rgba(34,139,34,0.25)");

    // draw bbox
    ctx.beginPath();
    [
      [bbox.minx, bbox.miny],
      [bbox.maxx, bbox.miny],
      [bbox.maxx, bbox.maxy],
      [bbox.minx, bbox.maxy],
    ].forEach(([x, y], idx) => {
      const [px, py] = toPx(x, y);
      if (idx === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.closePath();
    ctx.strokeStyle = "rgba(64,158,255,0.9)";
    ctx.lineWidth = 2 / viewState.scale;
    ctx.setLineDash([6, 4]);
    ctx.stroke();
    ctx.setLineDash([]);
  }, [dataBounds, bbox, width, height, viewState, toPx]);

  // 鼠标事件处理
  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? 0.9 : 1.1;
    const newScale = Math.max(0.1, Math.min(10, viewState.scale * delta));
    setViewState((prev) => ({ ...prev, scale: newScale }));
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (e.button === 0) {
      setIsDragging(true);
      setDragStart({ x: e.clientX - viewState.offsetX, y: e.clientY - viewState.offsetY });
    }
  };

  const handleMouseMove = (e: React.MouseEvent) => {
    if (isDragging) {
      setViewState((prev) => ({
        ...prev,
        offsetX: e.clientX - dragStart.x,
        offsetY: e.clientY - dragStart.y,
      }));
    }
  };

  const handleMouseUp = () => {
    setIsDragging(false);
  };

  const handleReset = () => {
    setViewState({ scale: 1, offsetX: 0, offsetY: 0 });
  };

  const handleZoomIn = () => {
    setViewState((prev) => ({ ...prev, scale: Math.min(10, prev.scale * 1.2) }));
  };

  const handleZoomOut = () => {
    setViewState((prev) => ({ ...prev, scale: Math.max(0.1, prev.scale / 1.2) }));
  };

  return (
    <div
      ref={containerRef}
      style={{
        position: "relative",
        width: "100%",
        height: "100%",
        overflow: "hidden",
        cursor: isDragging ? "grabbing" : "grab",
      }}
      onWheel={handleWheel}
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
      onMouseLeave={handleMouseUp}
    >
      <canvas
        ref={canvasRef}
        width={width}
        height={height}
        style={{ width: "100%", height: "100%", background: "#fcfcfc", display: "block" }}
      />
      <div
        style={{
          position: "absolute",
          top: 10,
          right: 10,
          display: "flex",
          flexDirection: "column",
          gap: 4,
          zIndex: 10,
        }}
      >
        <Button size="small" onClick={handleZoomIn} title="放大">
          +
        </Button>
        <Button size="small" onClick={handleZoomOut} title="缩小">
          −
        </Button>
        <Button size="small" onClick={handleReset} title="重置">
          ↻
        </Button>
      </div>
    </div>
  );
}

function Thumbnail({ bbox }: { bbox: BBox }) {
  const width = 220;
  const height = 220;
  const pad = 10;
  const scaleX = (width - pad * 2) / (GLOBAL_BOUNDS.maxx - GLOBAL_BOUNDS.minx);
  const scaleY = (height - pad * 2) / (GLOBAL_BOUNDS.maxy - GLOBAL_BOUNDS.miny);
  const toPx = (x: number, y: number) => [
    pad + (x - GLOBAL_BOUNDS.minx) * scaleX,
    height - pad - (y - GLOBAL_BOUNDS.miny) * scaleY,
  ];
  const [x1, y1] = toPx(bbox.minx, bbox.miny);
  const [x2, y2] = toPx(bbox.maxx, bbox.maxy);
  return (
    <svg width={width} height={height} style={{ border: "1px solid #ddd", background: "#fafafa" }}>
      <rect x={pad} y={pad} width={width - pad * 2} height={height - pad * 2} fill="#f5f5f5" stroke="#e0e0e0" />
      <rect
        x={Math.min(x1, x2)}
        y={Math.min(y1, y2)}
        width={Math.abs(x2 - x1)}
        height={Math.abs(y2 - y1)}
        fill="rgba(64,158,255,0.25)"
        stroke="#409eff"
        strokeWidth={2}
      />
      <text x={pad + 6} y={pad + 16} fill="#666" fontSize={12}>
        范围预览
      </text>
    </svg>
  );
}

function detectWebglSupport(): boolean {
  try {
    const canvas = document.createElement("canvas");
    const gl = (canvas.getContext("webgl") ||
      canvas.getContext("experimental-webgl")) as WebGLRenderingContext | null;
    if (!gl || typeof gl.getParameter !== "function") return false;
    // Touch a simple parameter to ensure the context is usable.
    gl.getParameter(gl.ALIASED_LINE_WIDTH_RANGE);
    return true;
  } catch {
    return false;
  }
}

export default function App() {
  const [uploadA, setUploadA] = useState<DatasetUploadState>({ prefix: "data_a" });
  const [uploadB, setUploadB] = useState<DatasetUploadState>({ prefix: "data_b" });
  const [uploading, setUploading] = useState(false);
  const uploadMsgKey = "upload-progress";
  const [bbox, setBbox] = useState<BBox>({ ...GLOBAL_BOUNDS });
  const [taskId, setTaskId] = useState<string>();
  const [taskStatus, setTaskStatus] = useState<string>();
  const [result, setResult] = useState<any>();
  const [statsGlobal, setStatsGlobal] = useState<any>();
  const [statsBBox, setStatsBBox] = useState<any>();
  const [polygonsA, setPolygonsA] = useState<PolygonFeature[]>([]);
  const [polygonsB, setPolygonsB] = useState<PolygonFeature[]>([]);
  const pollingRef = useRef<number>();
  const [glError, setGlError] = useState(() => !detectWebglSupport());
  // 默认使用 Canvas，必要时可切换 WebGL
  const [useCanvas, setUseCanvas] = useState(true);

  useEffect(() => {
    return () => {
      if (pollingRef.current) window.clearInterval(pollingRef.current);
    };
  }, []);

  // 一旦 WebGL 报错，自动切到 Canvas 回退
  useEffect(() => {
    if (glError) setUseCanvas(true);
  }, [glError]);

  const handleUpload = async () => {
    if (!uploadA.file || !uploadB.file) {
      message.error("请同时选择两个数据集文件");
      return;
    }
    setUploading(true);
    message.open({ key: uploadMsgKey, type: "loading", content: "上传中..." });
    try {
      await uploadDataset(uploadA.prefix, uploadA.file, (p) =>
        message.open({ key: uploadMsgKey, type: "loading", content: `A 上传中... ${p}%` })
      );
      await uploadDataset(uploadB.prefix, uploadB.file, (p) =>
        message.open({ key: uploadMsgKey, type: "loading", content: `B 上传中... ${p}%` })
      );
      message.success({ key: uploadMsgKey, content: "上传完成", duration: 2 });
    } catch (e: any) {
      message.error({ key: uploadMsgKey, content: e?.message || "上传失败" });
    } finally {
      setUploading(false);
    }
  };

  const handleCompute = async () => {
    const payload = { dataset_a: uploadA.prefix, dataset_b: uploadB.prefix, bbox };
    const res = await createTask(payload);
    setTaskId(res.task_id);
    setTaskStatus(res.status);
    pollingRef.current = window.setInterval(async () => {
      const r = await getTask(res.task_id);
      setTaskStatus(r.status);
      if (r.status === "DONE") {
        setResult(r.result);
        if (pollingRef.current) window.clearInterval(pollingRef.current);
      }
      if (r.status === "FAILED") {
        if (pollingRef.current) window.clearInterval(pollingRef.current);
      }
    }, 1500);
  };

  const handleStatsGlobal = async () => {
    const hide = message.loading("计算中...");
    try {
      const res = await getRegionStats(uploadA.prefix, uploadB.prefix);
      setStatsGlobal(res);
      message.success("计算完成");
    } catch (e: any) {
      message.error(e?.message || "计算失败");
    } finally {
      hide();
    }
  };

  const handleStatsBBox = async () => {
    const hide = message.loading("计算中...");
    try {
      const res = await getRegionStats(uploadA.prefix, uploadB.prefix, bbox);
      setStatsBBox(res);
      message.success("计算完成");
    } catch (e: any) {
      message.error(e?.message || "计算失败");
    } finally {
      hide();
    }
  };

  const handleLoadPolygons = async () => {
    const hide = message.loading("加载数据...");
    try {
      const [fa, fb] = await Promise.all([
        getPolygons(uploadA.prefix, bbox, 4000),
        getPolygons(uploadB.prefix, bbox, 4000),
      ]);
      setPolygonsA(fa.features);
      setPolygonsB(fb.features);
      message.success(`已加载 A:${fa.count} / B:${fb.count}`);
    } catch (e: any) {
      message.error(e?.message || "加载失败");
    } finally {
      hide();
    }
  };

  const layers = useMemo(() => {
    const toDeck = (features: PolygonFeature[], color: [number, number, number, number]) =>
      new PolygonLayer({
        id: color[0] === 220 ? "dataset-a" : "dataset-b",
        data: features,
        pickable: true,
        getPolygon: (d) => d.geometry.coordinates,
        getFillColor: color,
        getLineColor: [40, 40, 40, 220],
        lineWidthUnits: "pixels",
        lineWidthMinPixels: 1.2,
      });
    const bboxFeature = {
      id: "bbox",
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [bbox.minx, bbox.miny],
            [bbox.maxx, bbox.miny],
            [bbox.maxx, bbox.maxy],
            [bbox.minx, bbox.maxy],
            [bbox.minx, bbox.miny],
          ],
        ],
      },
    };
    const bboxLayer = new PolygonLayer({
      id: "bbox-highlight",
      data: [bboxFeature],
      pickable: false,
      stroked: true,
      filled: false,
      getLineColor: [64, 158, 255, 255],
      lineWidthUnits: "pixels",
      lineWidthMinPixels: 2,
      dashJustified: true,
    });
    return [
      toDeck(polygonsA, [220, 20, 60, 190]),
      toDeck(polygonsB, [34, 139, 34, 190]),
      bboxLayer,
    ];
  }, [polygonsA, polygonsB, bbox]);

  const center = {
    x: (bbox.minx + bbox.maxx) / 2,
    y: (bbox.miny + bbox.maxy) / 2,
  };

  const statusBadge = (
    <Badge
      status={
        taskStatus === "DONE"
          ? "success"
          : taskStatus === "FAILED"
          ? "error"
          : taskStatus === "RUNNING"
          ? "processing"
          : "default"
      }
      text={`${taskStatus || "未开始"}`}
    />
  );

  return (
    <Layout style={{ minHeight: "100vh", background: "var(--page-bg)" }}>
      <Layout.Content style={{ padding: "24px 24px 48px", maxWidth: 1200, margin: "0 auto", width: "100%" }}>
        <Card
          bordered={false}
          style={{ marginBottom: 16, background: "linear-gradient(135deg,#f7f9ff 0%,#f3f7ff 100%)" }}
          styles={{ body: { padding: 16 } }}
        >
          <Space direction="vertical" size={4}>
            <Tag color="blue" style={{ alignSelf: "flex-start" }}>
              本地分析
            </Tag>
            <Title level={3} style={{ margin: 0 }}>
              空间多边形匹配分析
            </Title>
            <Text type="secondary">基于 Jaccard 相似度的空间数据交叉匹配</Text>
          </Space>
        </Card>

        <Row gutter={[16, 16]}>
          <Col span={16}>
            <Card
              title={
                <Space>
                  <AimOutlined />
                  <span>空间视图</span>
                </Space>
              }
              size="small"
              className="glass-card"
              extra={
                <Space>
                  <Button onClick={handleLoadPolygons} icon={<ReloadOutlined />} type="primary" ghost>
                    加载数据
                  </Button>
                  <Button size="small" onClick={() => setUseCanvas((v) => !v)}>
                    {useCanvas ? "WebGL" : "Canvas"}
                  </Button>
                </Space>
              }
            >
              <div style={{ height: 520, border: "1px solid #eee", borderRadius: 10, overflow: "hidden" }}>
                {useCanvas || glError ? (
                  <CanvasPreview polygonsA={polygonsA} polygonsB={polygonsB} bbox={bbox} />
                ) : (
                  <DeckGL
                    controller={true}
                    width="100%"
                    height="100%"
                    style={{ width: "100%", height: "100%" }}
                    parameters={{ clearColor: [0.99, 0.99, 0.99, 1] }}
                    views={new OrthographicView({ id: "ortho" })}
                    initialViewState={{
                      target: [center.x, center.y, 0],
                      zoom: 4,
                    }}
                    glOptions={{ failIfMajorPerformanceCaveat: false }}
                    onWebGLInitialized={(gl) => {
                      if (!gl || typeof gl.getParameter !== "function") {
                        setGlError(true);
                        return;
                      }
                      try {
                        gl.getParameter(gl.MAX_TEXTURE_SIZE);
                      } catch {
                        setGlError(true);
                      }
                    }}
                    onError={() => setGlError(true)}
                    layers={layers}
                    getTooltip={({ object }) =>
                      object && {
                        text: `id: ${object.id}\narea: ${object.area.toFixed(4)}`,
                      }
                    }
                  />
                )}
              </div>
            </Card>

            <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
              <Col span={12}>
                <Card title="分析结果" size="small" className="glass-card" extra={statusBadge}>
                  <div style={{ marginTop: 4 }}>
                    {result ? (
                      <Row gutter={[12, 12]}>
                        <Col span={12}>
                          <Statistic title="Jaccard" value={result.block_jaccard} precision={6} />
                        </Col>
                        <Col span={12}>
                          <Statistic title="交集对数" value={result.intersection_count} />
                        </Col>
                        <Col span={8}>
                          <Statistic title="|A|" value={result.area_a} precision={4} />
                        </Col>
                        <Col span={8}>
                          <Statistic title="|B|" value={result.area_b} precision={4} />
                        </Col>
                        <Col span={8}>
                          <Statistic title="|A∩B|" value={result.area_inter} precision={4} />
                        </Col>
                      </Row>
                    ) : (
                      <Text type="secondary">等待计算完成</Text>
                    )}
                  </div>
                  {(statsGlobal || statsBBox) && (
                    <>
                      <Divider />
                      {statsGlobal && (
                        <div style={{ marginBottom: 12 }}>
                          <Text strong>全域：</Text>
                          <Row gutter={[12, 8]} style={{ marginTop: 6 }}>
                            <Col span={8}>
                              <Statistic title="Jaccard" value={statsGlobal.block_jaccard} precision={6} />
                            </Col>
                            <Col span={8}>
                              <Statistic title="|A|" value={statsGlobal.area_a} precision={4} />
                            </Col>
                            <Col span={8}>
                              <Statistic title="|B|" value={statsGlobal.area_b} precision={4} />
                            </Col>
                            <Col span={12}>
                              <Statistic title="|A∩B|" value={statsGlobal.area_inter} precision={4} />
                            </Col>
                            <Col span={12}>
                              <Statistic title="交集对数" value={statsGlobal.intersection_count} />
                            </Col>
                          </Row>
                        </div>
                      )}
                      {statsBBox && (
                        <div>
                          <Text strong>当前范围：</Text>
                          <Row gutter={[12, 8]} style={{ marginTop: 6 }}>
                            <Col span={8}>
                              <Statistic title="Jaccard" value={statsBBox.block_jaccard} precision={6} />
                            </Col>
                            <Col span={8}>
                              <Statistic title="|A|" value={statsBBox.area_a} precision={4} />
                            </Col>
                            <Col span={8}>
                              <Statistic title="|B|" value={statsBBox.area_b} precision={4} />
                            </Col>
                            <Col span={12}>
                              <Statistic title="|A∩B|" value={statsBBox.area_inter} precision={4} />
                            </Col>
                            <Col span={12}>
                              <Statistic title="交集对数" value={statsBBox.intersection_count} />
                            </Col>
                          </Row>
                        </div>
                      )}
                    </>
                  )}
                </Card>
              </Col>
              <Col span={12}>
                <Card title="操作" size="small" className="glass-card">
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <Button type="primary" icon={<PlayCircleOutlined />} onClick={handleCompute} block>
                      执行计算
                    </Button>
                    <Button icon={<ReloadOutlined />} onClick={handleLoadPolygons} block>
                      刷新视图
                    </Button>
                    <Space>
                      <Button onClick={handleStatsGlobal} block>
                        全域分析
                      </Button>
                      <Button type="dashed" onClick={handleStatsBBox} block>
                        范围分析
                      </Button>
                    </Space>
                    <Button icon={<RocketOutlined />} type="dashed" onClick={handleUpload} block>
                      重新上传
                    </Button>
                  </Space>
                </Card>
              </Col>
            </Row>
          </Col>

          <Col span={8}>
            <Card title="数据上传" size="small" className="glass-card" style={{ marginBottom: 16 }}>
              <Space direction="vertical" style={{ width: "100%" }}>
                <Space>
                  <Text strong>数据集A: {uploadA.prefix}</Text>
                  <Upload
                    beforeUpload={(file) => {
                      setUploadA({ ...uploadA, file });
                      return false;
                    }}
                    maxCount={1}
                  >
                    <Button icon={<UploadOutlined />}>选择文件</Button>
                  </Upload>
                </Space>
                <Space>
                  <Text strong>数据集B: {uploadB.prefix}</Text>
                  <Upload
                    beforeUpload={(file) => {
                      setUploadB({ ...uploadB, file });
                      return false;
                    }}
                    maxCount={1}
                  >
                    <Button icon={<UploadOutlined />}>选择文件</Button>
                  </Upload>
                </Space>
                <Button type="primary" onClick={handleUpload} block loading={uploading} disabled={uploading}>
                  上传
                </Button>
              </Space>
            </Card>

            <Card title="范围设置" size="small" className="glass-card">
              <Form layout="vertical">
                <Form.Item label="minx / miny">
                  <Space>
                    <InputNumber value={bbox.minx} onChange={(v) => setBbox({ ...bbox, minx: v || 0 })} />
                    <InputNumber value={bbox.miny} onChange={(v) => setBbox({ ...bbox, miny: v || 0 })} />
                  </Space>
                </Form.Item>
                <Form.Item label="maxx / maxy">
                  <Space>
                    <InputNumber value={bbox.maxx} onChange={(v) => setBbox({ ...bbox, maxx: v || 0 })} />
                    <InputNumber value={bbox.maxy} onChange={(v) => setBbox({ ...bbox, maxy: v || 0 })} />
                  </Space>
                </Form.Item>
              </Form>
              <Thumbnail bbox={bbox} />
            </Card>
          </Col>
        </Row>
      </Layout.Content>
    </Layout>
  );
}

