# 空间多边形匹配分析系统

基于 Jaccard 相似度的空间数据交叉匹配分析工具，支持大规模多边形数据集的本地计算和可视化。

## 功能特性

- 📊 **空间数据上传与分块**：自动将 CSV 格式的多边形数据按网格分块存储
- 🔍 **区域选择与预览**：支持自定义 bbox 范围，实时预览多边形分布
- 📈 **Jaccard 相似度计算**：计算两个数据集之间的空间交集和相似度
- 🎨 **交互式可视化**：支持 WebGL 和 Canvas 两种渲染模式，支持缩放和平移
- 📋 **统计分析**：支持全域和指定区域的统计分析

## 技术栈

### 后端
- FastAPI
- Shapely (几何计算)
- R-tree (空间索引)
- Pandas (数据处理)

### 前端
- React + TypeScript
- Ant Design
- Deck.gl (WebGL 渲染)
- Vite

## 安装与运行

### 后端

```bash
# 安装依赖
pip install -r requirements.txt

# 运行服务
python main.py
```

后端服务默认运行在 `http://localhost:8000`

### 前端

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端默认运行在 `http://localhost:3000`

## 数据格式

CSV 文件需要包含以下列：
- `id`: 多边形唯一标识
- `geometry`: WKT 格式的多边形几何（如 `POLYGON ((x1 y1, x2 y2, ...))`）

示例：
```csv
id,geometry
A0,"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
A1,"POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))"
```

## API 接口

- `POST /api/datasets/upload` - 上传数据集并分块
- `POST /api/tasks` - 创建计算任务
- `GET /api/tasks/{task_id}` - 查询任务状态
- `GET /api/regions/polygons` - 获取指定区域的多边形
- `GET /api/regions/stats` - 获取统计信息

## 使用说明

1. **上传数据**：在前端选择两个 CSV 文件（数据集 A 和 B），点击上传
2. **设置范围**：调整 bbox 参数选择要分析的区域
3. **预览数据**：点击"加载数据"查看多边形分布
4. **执行计算**：点击"执行计算"开始 Jaccard 相似度计算
5. **查看结果**：在结果面板查看计算统计信息

## 项目结构

```
.
├── main.py                 # FastAPI 主应用
├── parti1_local.py         # 数据分块模块
├── parti2_local.py         # Jaccard 计算模块
├── requirements.txt        # Python 依赖
├── frontend/               # 前端代码
│   ├── src/
│   │   ├── App.tsx        # 主组件
│   │   ├── api.ts         # API 调用
│   │   └── types.ts       # 类型定义
│   └── package.json       # 前端依赖
└── README.md              # 项目说明
```

## 许可证

MIT License

