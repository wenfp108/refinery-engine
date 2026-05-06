"""
新闻监控模块 - 整合 NewsNow 数据源
"""
import os
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional

class NewsMonitor:
    """监控 NewsNow 数据源"""

    def __init__(self, newsnow_url: str = None):
        self.newsnow_url = newsnow_url or os.environ.get("NEWSNOW_URL", "http://localhost:3000")
        self.stats_file = "news_stats.json"

    def fetch_source(self, source_id: str) -> List[Dict]:
        """从 NewsNow 获取指定数据源"""
        try:
            # NewsNow API 端点
            url = f"{self.newsnow_url}/api/source/{source_id}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️ 获取 {source_id} 失败: {e}")
            return []

    def fetch_all_sources(self) -> Dict[str, List[Dict]]:
        """获取所有配置的数据源"""
        sources = {
            # 国际新闻
            "economist": "经济学人",
            "nytimes": "纽约时报",
            "ft": "金融时报",
            "caixin": "财新网",
            # 科技
            "github": "GitHub Trending",
            "hackernews": "Hacker News",
            # 金融
            "wallstreetcn": "华尔街见闻",
            "jin10": "金十数据",
            "xueqiu": "雪球",
            "cls": "财联社",
            "gelonghui": "格隆汇",
            "fastbull": "法布财经",
        }

        results = {}
        for source_id, name in sources.items():
            print(f"📡 正在获取 {name}...")
            items = self.fetch_source(source_id)
            if items:
                results[source_id] = {
                    "name": name,
                    "count": len(items),
                    "items": items[:10],  # 只保存前10条
                    "fetched_at": datetime.now().isoformat(),
                }
                print(f"  ✅ 获取 {len(items)} 条")
            else:
                print(f"  ⚠️ 无数据")

        return results

    def update_stats(self, data: Dict):
        """更新统计数据"""
        stats = self.load_stats()
        today = datetime.now().strftime("%Y-%m-%d")

        if today not in stats:
            stats[today] = {}

        for source_id, source_data in data.items():
            stats[today][source_id] = {
                "count": source_data["count"],
                "fetched_at": source_data["fetched_at"],
            }

        # 只保留最近30天的数据
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        stats = {k: v for k, v in stats.items() if k >= cutoff}

        with open(self.stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        return stats

    def load_stats(self) -> Dict:
        """加载统计数据"""
        if os.path.exists(self.stats_file):
            with open(self.stats_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def get_daily_summary(self, date: str = None) -> str:
        """获取每日摘要"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        stats = self.load_stats()
        if date not in stats:
            return f"📊 {date} 无数据"

        day_stats = stats[date]
        total = sum(s["count"] for s in day_stats.values())

        output = f"📊 新闻监控统计 ({date})\n"
        output += "=" * 40 + "\n\n"

        sorted_sources = sorted(
            day_stats.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )

        for source_id, data in sorted_sources:
            bar = "█" * min(data["count"] // 5, 20)
            output += f"{source_id:20} {data['count']:4} 条 {bar}\n"

        output += f"\n{'─' * 40}\n"
        output += f"总计: {total} 条数据\n"

        return output

    def run(self):
        """执行监控任务"""
        print("🔥 开始新闻监控...")
        data = self.fetch_all_sources()
        stats = self.update_stats(data)
        summary = self.get_daily_summary()
        print("\n" + summary)
        return data


if __name__ == "__main__":
    monitor = NewsMonitor()
    monitor.run()
