"""
新闻联播 NLP 处理器
流程：加载文本 → jieba分词 → 关键词提取 → 权重打分 → 映射行业
输出：分析结果写入 Central-Bank
"""

import os, sys, json, re, base64
from pathlib import Path
from collections import Counter
from datetime import datetime, timedelta, timezone

import jieba
import jieba.analyse
from github import Github, Auth

# ============================================
# 0. GitHub 连接
# ============================================
GITHUB_TOKEN = os.environ.get("GH_PAT")
if not GITHUB_TOKEN:
    sys.exit("❌ 缺少 GH_PAT 环境变量")

auth = Auth.Token(GITHUB_TOKEN)
gh_client = Github(auth=auth)
news_repo = gh_client.get_repo("wenfp108/xin-wen-lian-bo")
bank_repo = gh_client.get_repo("wenfp108/Central-Bank")

# ============================================
# 1. 关键词 → 行业映射表
# ============================================
KEYWORD_SECTOR_MAP = {
    # 新能源
    "新能源": "新能源", "光伏": "新能源", "风电": "新能源",
    "储能": "新能源", "氢能": "新能源", "碳中和": "新能源",
    "绿色": "新能源", "低碳": "新能源", "清洁能源": "新能源",
    "充电": "新能源", "电池": "新能源", "锂电": "新能源",
    # 半导体
    "半导体": "半导体", "芯片": "半导体", "集成电路": "半导体",
    "自主可控": "半导体", "国产替代": "半导体", "光刻": "半导体",
    # 科技/AI
    "人工智能": "科技", "AI": "科技", "大模型": "科技",
    "算力": "科技", "数字经济": "科技", "数据": "科技",
    "机器人": "科技", "自动驾驶": "科技", "智能终端": "科技",
    "量子": "科技", "区块链": "科技",
    # 军工
    "军工": "军工", "国防": "军工", "航天": "军工",
    "卫星": "军工", "导弹": "军工", "航空母舰": "军工",
    "无人机": "军工", "北斗": "军工",
    # 消费
    "消费": "消费", "内需": "消费", "文旅": "消费",
    "旅游": "消费", "餐饮": "消费", "零售": "消费",
    "免税": "消费", "电商": "消费",
    # 医药
    "医药": "医药", "医疗": "医药", "创新药": "医药",
    "医保": "医药", "中药": "医药", "生物": "医药",
    # 基建
    "基建": "基建", "高铁": "基建", "公路": "基建",
    "水利": "基建", "铁路": "基建", "桥梁": "基建",
    "城市更新": "基建", "新型城镇化": "基建",
    # 金融
    "金融": "金融", "银行": "金融", "证券": "金融",
    "保险": "金融", "资本市场": "金融", "股市": "金融",
    # 农业
    "农业": "农业", "粮食": "农业", "种业": "农业",
    "乡村振兴": "农业", "农机": "农业", "化肥": "农业",
    # 房地产
    "房地产": "房地产", "住房": "房地产", "楼市": "房地产",
    "保障房": "房地产", "公积金": "房地产",
    # 能源
    "能源": "能源", "石油": "能源", "天然气": "能源",
    "煤炭": "能源", "核电": "能源", "电网": "能源",
    # 汽车
    "汽车": "汽车", "新能源汽车": "汽车", "智能网联": "汽车",
    "车联网": "汽车", "充电桩": "汽车",
}

# 信号强度词
SIGNAL_WORDS = {
    "大力推进": 3, "加快发展": 3, "着力": 2, "强化": 2,
    "重点": 2, "突破": 2, "关键": 2, "强调": 3,
    "重要指示": 5, "重要讲话": 5, "亲自": 5,
    "加快": 2, "推进": 1, "深化": 2, "改革": 2,
    "创新": 1, "发展": 1, "建设": 1,
}

NOISE_WORDS = {"央视网", "新闻联播", "视频", "nbsp", "消息", "http", "https", "com", "html", "VIDE", "查看原文"}

# ============================================
# 2. 从 GitHub 加载新闻
# ============================================
def load_news_from_github(date_str):
    """从 xin-wen-lian-bo 仓库加载指定日期的新闻"""
    path = f"news/{date_str}.md"
    try:
        content_file = news_repo.get_contents(path)
        return base64.b64decode(content_file.content).decode('utf-8')
    except Exception:
        return None

def get_latest_news_date():
    """获取最新新闻日期"""
    try:
        contents = news_repo.get_contents("news")
        md_files = [f.name.replace('.md', '') for f in contents if f.name.endswith('.md')]
        return max(md_files) if md_files else None
    except Exception:
        return None

def parse_duration(duration_str):
    """解析时长字符串 '05:32' → 秒数"""
    if not duration_str:
        return 0
    parts = duration_str.split(':')
    if len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    return 0

def parse_news_items(text):
    """解析新闻条目（标题 + 内容 + 序号 + 时长）"""
    items = []
    sections = re.split(r'\n### ', text)
    for i, section in enumerate(sections):
        if i == 0:
            continue
        lines = section.strip().split('\n')
        title = lines[0].strip()
        duration_str = None
        duration_sec = 0
        for line in lines[:5]:
            m = re.search(r'视频时长[:：]\s*(\d{2}:\d{2})', line)
            if m:
                duration_str = m.group(1)
                duration_sec = parse_duration(duration_str)
                break
        content = '\n'.join(lines[1:])
        content = re.sub(r'<[^>]+>', '', content)
        content = re.sub(r'\[查看原文\].*', '', content)
        content = content.strip()
        items.append({
            'index': i,
            'title': title,
            'content': content,
            'duration_str': duration_str,
            'duration_sec': duration_sec,
        })
    return items

# ============================================
# 3. 权重打分
# ============================================
def score_news_item(item):
    """对单条新闻打分"""
    title = item['title']
    content = item['content']
    duration_sec = item.get('duration_sec', 0)
    full_text = title + " " + content

    position_score = max(0, 10 - item['index'])

    if duration_sec >= 300:
        duration_score = 10
    elif duration_sec >= 180:
        duration_score = 6
    elif duration_sec >= 120:
        duration_score = 3
    elif duration_sec >= 60:
        duration_score = 1
    else:
        duration_score = 0

    sector_scores = {}
    for keyword, sector in KEYWORD_SECTOR_MAP.items():
        count = full_text.count(keyword)
        if count > 0:
            sector_scores[sector] = sector_scores.get(sector, 0) + count

    signal_score = 0
    for word, weight in SIGNAL_WORDS.items():
        count = full_text.count(word)
        signal_score += count * weight

    title_bonus = 0
    for keyword in KEYWORD_SECTOR_MAP:
        if keyword in title:
            title_bonus += 5

    return {
        'position_score': position_score,
        'duration_score': duration_score,
        'sector_scores': sector_scores,
        'signal_score': signal_score,
        'title_bonus': title_bonus,
        'total_score': position_score + duration_score + signal_score + title_bonus,
    }

# ============================================
# 4. jieba 关键词提取
# ============================================
def extract_keywords(text, top_n=20):
    """用jieba提取关键词"""
    tfidf_kw = jieba.analyse.extract_tags(text, topK=top_n * 2, withWeight=True)
    tfidf_kw = [(kw, w) for kw, w in tfidf_kw if kw not in NOISE_WORDS and len(kw) > 1][:top_n]
    tr_kw = jieba.analyse.textrank(text, topK=top_n * 2, withWeight=True)
    tr_kw = [(kw, w) for kw, w in tr_kw if kw not in NOISE_WORDS and len(kw) > 1][:top_n]
    return {
        'tfidf': [(kw, round(w, 4)) for kw, w in tfidf_kw],
        'textrank': [(kw, round(w, 4)) for kw, w in tr_kw],
    }

# ============================================
# 5. 处理单日新闻
# ============================================
def process_day(date_str):
    """处理一天的新闻"""
    text = load_news_from_github(date_str)
    if not text:
        return None

    items = parse_news_items(text)
    if not items:
        return None

    scored_items = []
    for item in items:
        scores = score_news_item(item)
        item['scores'] = scores
        scored_items.append(item)

    sector_totals = {}
    for item in scored_items:
        for sector, score in item['scores']['sector_scores'].items():
            sector_totals[sector] = sector_totals.get(sector, 0) + score

    full_text = " ".join(item['content'] for item in items)
    keywords = extract_keywords(full_text)

    return {
        'date': date_str,
        'news_count': len(items),
        'sector_scores': dict(sorted(sector_totals.items(), key=lambda x: -x[1])),
        'top_keywords': keywords['tfidf'][:10],
        'items': [{
            'index': item['index'],
            'title': item['title'],
            'duration': item.get('duration_str', '-'),
            'sectors': item['scores']['sector_scores'],
            'signal_score': item['scores']['signal_score'],
            'duration_score': item['scores']['duration_score'],
            'total_score': item['scores']['total_score'],
        } for item in scored_items],
    }

# ============================================
# 6. 多日趋势分析
# ============================================
def analyze_trends(days=7):
    """分析多日趋势（默认7天）"""
    try:
        contents = news_repo.get_contents("news")
        md_files = sorted([f.name.replace('.md', '') for f in contents if f.name.endswith('.md')])
    except Exception:
        return None

    if not md_files:
        return None

    recent = md_files[-days:]
    sector_trend = {}
    keyword_freq = Counter()

    for date_str in recent:
        result = process_day(date_str)
        if not result:
            continue
        for sector, score in result['sector_scores'].items():
            if sector not in sector_trend:
                sector_trend[sector] = []
            sector_trend[sector].append({'date': date_str, 'score': score})
        for kw, weight in result['top_keywords']:
            keyword_freq[kw] += 1

    return {
        'period': f"{recent[0]} ~ {recent[-1]}",
        'days': len(recent),
        'sector_trend': sector_trend,
        'top_keywords': keyword_freq.most_common(20),
    }

# ============================================
# 7. 生成 Markdown 报告
# ============================================
def generate_report(date_str=None):
    """生成分析报告，返回 (report_content, date_str)"""
    if not date_str:
        date_str = get_latest_news_date()
    if not date_str:
        return None, None

    result = process_day(date_str)
    if not result:
        return None, None

    # 7天趋势
    trends = analyze_trends(7)

    md = f"# 📰 新闻联播政策信号分析 ({result['date']})\n\n"
    md += f"> **新闻数量**: {result['news_count']} 条\n\n"

    # 行业热度
    md += "## 📊 行业热度\n\n"
    md += "| 行业 | 分数 | 热度 |\n|:---|:---|:---|\n"
    for sector, score in result['sector_scores'].items():
        bar = "█" * min(score, 20)
        md += f"| {sector} | {score} | {bar} |\n"
    md += "\n"

    # 重点新闻（按权重排序前5）
    md += "## 🔥 重点新闻\n\n"
    md += "| 分数 | 时长 | 标题 | 行业 |\n|:---|:---|:---|:---|\n"
    sorted_items = sorted(result['items'], key=lambda x: -x['total_score'])
    for item in sorted_items[:5]:
        sectors = ", ".join(item['sectors'].keys()) if item['sectors'] else "-"
        title_short = item['title'][:40]
        md += f"| {item['total_score']} | {item['duration']} | {title_short} | {sectors} |\n"
    md += "\n"

    # 关键词
    md += "## 🔑 关键词\n\n"
    for kw, w in result['top_keywords']:
        md += f"- **{kw}** ({w})\n"
    md += "\n"

    # 7天趋势
    if trends:
        md += f"## 📈 7天趋势 ({trends['period']})\n\n"
        md += "| 行业 | 均值 | 最新 | 趋势 |\n|:---|:---|:---|:---|\n"
        for sector, data in trends['sector_trend'].items():
            avg = sum(d['score'] for d in data) / len(data)
            recent = data[-1]['score'] if data else 0
            trend = "↑" if recent > avg else "↓" if recent < avg else "→"
            md += f"| {sector} | {avg:.1f} | {recent} | {trend} |\n"
        md += "\n"
        md += "### 高频关键词\n\n"
        for kw, count in trends['top_keywords'][:10]:
            md += f"- {kw} ({count}天)\n"

    return md, date_str

# ============================================
# 8. 写入 Central-Bank
# ============================================
def upload_to_bank(report_content, date_str):
    """将报告写入 Central-Bank"""
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:8]
    path = f"reports/policy/{year}/{month}/{day}/新闻联播分析.md"

    try:
        try:
            old = bank_repo.get_contents(path)
            bank_repo.update_file(old.path, f"📰 更新: {date_str} 新闻联播分析", report_content, old.sha)
            print(f"📝 更新: {path}")
        except Exception:
            bank_repo.create_file(path, f"📰 新增: {date_str} 新闻联播分析", report_content)
            print(f"📝 创建: {path}")
    except Exception as e:
        print(f"❌ 写入失败: {e}")

# ============================================
# 9. CLI
# ============================================
if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'run'

    if cmd == 'run':
        # 默认：分析最新一天并上传
        report, date_str = generate_report()
        if report:
            upload_to_bank(report, date_str)
            print(f"✅ 新闻联播分析完成: {date_str}")
        else:
            print("⚠️ 无数据可分析")

    elif cmd == 'date':
        # 指定日期
        date_str = sys.argv[2]
        report, date_str = generate_report(date_str)
        if report:
            upload_to_bank(report, date_str)
            print(f"✅ 新闻联播分析完成: {date_str}")
        else:
            print(f"⚠️ 无数据: {date_str}")

    elif cmd == 'test':
        # 测试模式（不上传）
        report, date_str = generate_report()
        if report:
            print(report)
        else:
            print("⚠️ 无数据")

    else:
        print("用法：")
        print("  python policy_nlp.py run       # 分析最新一天并上传")
        print("  python policy_nlp.py date 20260508  # 指定日期")
        print("  python policy_nlp.py test       # 测试（不上传）")
