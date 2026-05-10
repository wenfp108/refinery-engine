import json
import math
from datetime import datetime, timedelta

TABLE_NAME = "polymarket_logs"
RADAR_TARGET_TOTAL = 50

# 🚫 噪音过滤：猎奇/阴谋论/低质量话题
NOISE_KEYWORDS = [
    "aliens", "alien exist", "ufo", "extraterrestrial",
    "epstein", "suicide note", "conspiracy",
    "flat earth", "illuminati", "new world order",
    "bigfoot", "loch ness", "area 51",
]

def is_noise(item):
    """过滤猎奇/阴谋论内容"""
    text = (str(item.get('title', '')) + " " + str(item.get('question', ''))).lower()
    return any(kw in text for kw in NOISE_KEYWORDS)

# 🎨 美化工具
def fmt_k(num, prefix=""):
    if not num: return "-"
    try: n = float(num)
    except: return "-"
    if n >= 1_000_000_000_000: return f"{prefix}{n/1_000_000_000_000:.1f}T"
    if n >= 1_000_000_000: return f"{prefix}{n/1_000_000_000:.1f}B"
    if n >= 1_000_000: return f"{prefix}{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{prefix}{n/1_000:.1f}K"
    return f"{prefix}{int(n)}"

def to_bj_time(utc_str):
    if not utc_str: return None
    try:
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        return (dt + timedelta(hours=8)).isoformat()
    except: return None

def parse_num(val):
    if not val: return 0
    s = str(val).replace(',', '').replace('$', '').replace('%', '')
    try: return float(s)
    except: return 0

def process(raw_data, path):
    processed_list = []
    engine_type = "sniper" if "sniper" in path.lower() else "radar"
    if isinstance(raw_data, dict) and "items" in raw_data: items = raw_data["items"]
    elif isinstance(raw_data, list): items = raw_data
    else: items = [raw_data]

    # 1. 备用时间（仅当 JSON 里没时间时使用）
    force_now_time = (datetime.utcnow() + timedelta(hours=8)).isoformat()
    
    for item in items:
        # 🔥 2. 核心修改：优先尝试获取原始数据的更新时间
        # Polymarket 原始 JSON 通常带有 updatedAt 字段
        raw_time = item.get('updatedAt') 
        bj_time_final = to_bj_time(raw_time) if raw_time else force_now_time
        
        entry = {
            "bj_time": bj_time_final, # ✅ 现在它是真实的或者是当时入库的时间
            "title": item.get('eventTitle'),
            "slug": item.get('slug'),
            "ticker": item.get('ticker'),
            "question": item.get('question'),
            "prices": str(item.get('prices')),
            "category": item.get('category', 'OTHER'),
            "volume": parse_num(item.get('volume')),
            "liquidity": parse_num(item.get('liquidity')),
            "vol24h": parse_num(item.get('vol24h')),
            "day_change": parse_num(item.get('dayChange')),
            "engine": engine_type,
            "strategy_tags": item.get('strategy_tags', []),
            "raw_json": item
        }
        processed_list.append(entry)
    return processed_list

def calculate_score(item):
    vol24h = float(item.get('vol24h') or 0)
    day_change = abs(float(item.get('dayChange') or item.get('day_change') or 0))
    score = vol24h * (day_change + 1)
    text = (str(item.get('title')) + " " + str(item.get('question'))).lower()

    # 分层狙击：Gold/Fed 优先，Bitcoin 降权
    gold_fed = ["gold", "xau", "fed", "federal reserve"]
    bitcoin = ["bitcoin", "btc"]
    if any(k in text for k in gold_fed) and "warsh" not in text:
        score *= 3
    elif any(k in text for k in bitcoin):
        score *= 1.5  # Bitcoin 不再享受高权重

    tags = item.get('strategy_tags') or []
    if 'TAIL_RISK' in tags:
        score *= 3  # 从 ×50 降到 ×3，防止分数爆炸
    return score


def get_event_group(item):
    """将同类事件聚合，防止 Bitcoin 价格问题刷屏"""
    title = str(item.get('title', '')).lower()
    question = str(item.get('question', '')).lower()
    combined = title + " " + question

    # Bitcoin/BTC 价格类 → 合并为一组
    if any(k in combined for k in ["bitcoin above", "bitcoin price", "bitcoin hit", "bitcoin all time", "btc above", "btc price"]):
        return "crypto_price"
    # Ethereum 价格类
    if any(k in combined for k in ["ethereum price", "eth above", "eth price"]):
        return "crypto_price"
    # Fed 利率类
    if any(k in combined for k in ["fed decision", "fed rate", "rate cut", "federal reserve"]):
        return "fed_rates"
    # Gold 类
    if any(k in combined for k in ["gold hit", "gold settle", "gc hit", "gc settle", "xau"]):
        return "gold"
    # 2028 大选类
    if "2028" in combined and any(k in combined for k in ["president", "nominee", "election"]):
        return "election_2028"
    return item.get('slug', 'other')  # 默认用 slug

# 🔥 修复 f-string 报错
def get_win_rate_str(price_str):
    try:
        if "Yes:" in price_str: 
            val = float(price_str.split('Yes:')[1].split('%')[0])
            return f"Yes {val:.0f}%"
        if "Up:" in price_str: 
            val = float(price_str.split('Up:')[1].split('%')[0])
            return f"Up {val:.0f}%"
        if "{" in price_str:
            clean_json = price_str.replace("'", '"')
            val = float(json.loads(clean_json)) * 100
            return f"{val:.0f}%"
    except: pass
    return str(price_str)[:15]

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_data = res.data if res.data else []
    except Exception as e: return {}
    if not all_data: return {}

    # 🔥 1. 快照去重：只留最新时间戳
    def deduplicate_snapshots(items):
        latest_map = {}
        for item in items:
            unique_key = f"{item['slug']}_{item['question']}"
            if unique_key not in latest_map:
                latest_map[unique_key] = item
            else:
                if item.get('bj_time', '0') > latest_map[unique_key].get('bj_time', '0'):
                    latest_map[unique_key] = item
        return list(latest_map.values())

    clean_data = deduplicate_snapshots(all_data)

    # 过滤噪音内容
    clean_data = [i for i in clean_data if not is_noise(i)]

    sniper_pool = [i for i in clean_data if i.get('engine') == 'sniper']
    radar_pool = [i for i in clean_data if i.get('engine') == 'radar']
    sector_matrix = {}
    global_seen_slugs = set()

    def anti_flood_filter(items):
        grouped = {}
        for i in items:
            group = get_event_group(i)
            if group not in grouped: grouped[group] = []
            grouped[group].append(i)
        final = []
        for group, rows in grouped.items():
            for r in rows: r['_temp_score'] = calculate_score(r)
            rows.sort(key=lambda x: x['_temp_score'], reverse=True)
            final.extend(rows[:2])  # 每组最多 2 条
        return final

    # 🔥 2. 构建 8 列宽表
    def build_markdown(items):
        # 表头保持不变
        header = "| 信号 | 标题 | 问题 | Prices (Yes/No) | Vol | Liq | 24h | Tags |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
        rows = []
        for i in items:
            # 1. 信号保持不变
            signal = fmt_k(i['_temp_score'])
            
            # 2. 修改标题逻辑：放宽限制到 60 字符，防止太短看不清
            # 同时移除换行符，防止破坏表格格式
            raw_title = str(i.get('title', '-')).replace('|', '').replace('\n', ' ')
            title = raw_title[:60] + ('...' if len(raw_title) > 60 else '')
            
            # 3. 问题链接保持不变，稍微放宽长度
            q_text = str(i.get('question', '-')).replace('|', '').replace('\n', ' ')
            q_text_short = q_text[:50] + "..." # 稍微加长一点
            question = f"[{q_text_short}](https://polymarket.com/event/{i['slug']})"
            
            # 4. 🔥 核心修改：价格显示
            # 假设 i['prices'] 是类似 "Yes: 0.5% | No: 99.5%" 的字符串
            # 必须把中间的 '|' 替换掉，否则 Markdown 表格会崩坏
            # 方案 A: 用斜杠 (Yes: 0.5% / No: 99.5%)
            raw_prices = str(i.get('prices', 'N/A'))
            prices = raw_prices.replace('|', '/') 
            
            # 方案 B (可选): 如果支持 HTML，可以用 <br> 换行显示更清晰
            # prices = raw_prices.replace('|', '<br>') 

            # 其他数值保持不变
            vol = fmt_k(i.get('volume', 0), '$')
            liq = fmt_k(i.get('liquidity', 0), '$')
            v24 = fmt_k(i.get('vol24h', 0), '$')
            tags = ", ".join(i.get('strategy_tags', []))[:20] # Tags 也稍微放宽一点

            row = f"| **{signal}** | {title} | {question} | {prices} | {vol} | {liq} | {v24} | {tags} |"
            rows.append(row)
            
            # 记录 slug (保持原逻辑)
            if 'slug' in i:
                global_seen_slugs.add(i['slug'])
                
        return {"header": header, "rows": rows}

    if sniper_pool:
        refined = anti_flood_filter(sniper_pool)
        refined.sort(key=lambda x: x['_temp_score'], reverse=True)
        sector_matrix["🎯 SNIPER (核心监控)"] = build_markdown(refined[:6])  # 最多6条，防止刷屏

    # 🔥 3. 顺序：政治压轴
    SECTORS_LIST = [
        "Geopolitics", "Science", "Climate-Science", "Tech", 
        "Finance", "Crypto", "Economy", "Politics"
    ]
    
    MAP = {
        'POLITICS': 'Politics', 'GEOPOLITICS': 'Geopolitics', 'TECH': 'Tech', 
        'FINANCE': 'Finance', 'CRYPTO': 'Crypto', 'SCIENCE': 'Science', 
        'ECONOMY': 'Economy', 'BUSINESS': 'Economy',
        'CLIMATE': 'Climate-Science', 'GLOBAL WARMING': 'Climate-Science', 'ENVIRONMENT': 'Climate-Science'
    }

    if radar_pool:
        for s in SECTORS_LIST:
            pool = [
                i for i in radar_pool 
                if (MAP.get(i.get('category'), 'Other') == s or i.get('category') == s.upper())
                and i['slug'] not in global_seen_slugs
            ]
            if not pool: continue
            refined = anti_flood_filter(pool)
            refined.sort(key=lambda x: x['_temp_score'], reverse=True)
            quota = max(3, math.ceil((len(pool) / len(radar_pool)) * RADAR_TARGET_TOTAL))
            sector_matrix[s] = build_markdown(refined[:quota])

    return sector_matrix
