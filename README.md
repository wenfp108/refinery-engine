# Refinery Engine

> 自动化情报采集 → 清洗 → AI 审计 → 归档的闭环引擎。

## 架构

```
┌─────────────────────────────────────────────────────┐
│                   GitHub Actions                     │
│              (每 2 小时定时触发)                       │
└──────────┬──────────────┬──────────────┬────────────┘
           │              │              │
     ┌─────▼─────┐  ┌────▼────┐  ┌─────▼─────┐
     │ Refinery  │  │ Factory │  │  Masters  │
     │ (清洗入库) │  │ (AI审计) │  │ (审计插件) │
     └─────┬─────┘  └────┬────┘  └───────────┘
           │              │
     ┌─────▼─────┐  ┌────▼────┐
     │ Supabase  │  │  Vault  │
     │ (数据仓库) │  │ (归档库) │
     └───────────┘  └─────────┘
```

## 关联仓库

| 仓库 | 用途 |
|------|------|
| [Refinery-Engine](https://github.com/wenfp108/refinery-erngine) | 本仓库。数据清洗、AI 审计、自动归档 |
| [Masters-Council](https://github.com/wenfp108/Masters-Council) | 审计插件（达里奥、塔勒布、芒格等大师视角） |
| [Central-Bank](https://github.com/wenfp108/Central-Bank) | 原始信号 JSON 存储 + 审计结果归档 |

## 数据流

1. **采集层**（外部）→ 原始 JSON 写入 Central-Bank
2. **Refinery** → 从 Central-Bank 读取 JSON，清洗后写入 Supabase `raw_signals` 表
3. **Factory** → 从 Supabase 筛选精锐信号，调用 AI 进行多大师审计
4. **归档** → 7 天前的数据打包为 Parquet 上传 Central-Bank，然后清理 Supabase

## 信号来源

| 来源 | 去重逻辑 | 保留条数 |
|------|---------|---------|
| GitHub | repo_name | 20 |
| Papers | title | 30 |
| Twitter | 互动评分 + VIP 加权 | 60 |
| Reddit | score × vibe | 30 |
| Polymarket | 策略标签 + 流动性 | 80 |

## 新闻监控

集成 [NewsNow](https://github.com/wenfp108/newsnow) 数据源，每日自动监控：

| 数据源 | 类型 | 更新频率 |
|--------|------|---------|
| 经济学人 | 国际新闻 | 慢 |
| 纽约时报 | 国际新闻 | 中 |
| 金融时报 | 财经新闻 | 中 |
| GitHub Trending | 科技 | 快 |
| Hacker News | 科技 | 中 |
| 华尔街见闻 | 财经 | 快 |
| 金十数据 | 财经 | 快 |

运行新闻监控：
```bash
python news_monitor.py
```

统计数据保存在 `news_stats.json`，保留最近 30 天记录。

## 配置

所有可调参数在 `config.py`，包括：

- AI 模型和 API 地址
- 各来源的筛选配额和拉取上限
- Twitter VIP 列表和评分权重
- 审计批次大小和并发数
- 归档保留天数

## 环境变量

在 GitHub Actions Secrets 中配置：

| 变量 | 用途 |
|------|------|
| `GH_PAT` | GitHub Token（访问 Central-Bank 和 Masters-Council） |
| `SUPABASE_URL` | Supabase 项目地址 |
| `SUPABASE_KEY` | Supabase API Key |
| `SILICON_FLOW_KEY` | AI 模型 API Key |

## 手动触发

GitHub → Actions → 选择 workflow → Run workflow

## 🛠️ Environment

- **Runner**: GitHub Actions (`ubuntu-latest`)
- **Engine**: Python 3.9
- **Mode**: Automated Schedule (每 2 小时)
    * **Last Updated**: 2026-05-08
