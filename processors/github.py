import json
from datetime import datetime, timedelta

TABLE_NAME = "github_logs"

def fmt_k(num):
    if not num: return "-"
    try: n = float(num)
    except: return "-"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# === 1. 数据清洗逻辑 (保持不变) ===
def process(raw_data, path):
    if isinstance(raw_data, dict) and "items" in raw_data:
        items = raw_data["items"]
        meta_time = raw_data.get("meta", {}).get("scanned_at_bj")
    else:
        items = raw_data if isinstance(raw_data, list) else [raw_data]
        meta_time = None
        
    refined_results = []
    for i in items:
        # 如果 JSON 里没带时间，就用当前时间
        bj_time = meta_time if meta_time else datetime.now().isoformat()
        
        row = {
            "bj_time": bj_time,
            "repo_name": i.get('name'),
            "url": i.get('url'),
            "stars": int(i.get('stars', 0)),
            "full_text": i.get('description', ''),  # 项目描述
            # 这里的 tags 其实是 sentinel.js 里的策略标签 (e.g. ['TECH_ACCELERATOR'])
            "topics": i.get('tags', []),
            "raw_json": i
        }
        refined_results.append(row)
    return refined_results

# === 2. 战报生成逻辑 (修改版：单榜单模式) ===
def get_hot_items(supabase, table_name):
    # 只看最近 24 小时
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_repos = res.data if res.data else []
    except Exception as e: return {}
    
    if not all_repos: return {}

    # 1. 去重：同名项目只留 Star 最高的那个记录
    unique_repos = {}
    for r in all_repos:
        name = r.get('repo_name')
        if not name: continue
        if name not in unique_repos or r['stars'] > unique_repos[name]['stars']:
            unique_repos[name] = r

    # 2. 过滤低质量项目：至少 100 stars
    repo_list = [r for r in unique_repos.values() if r.get('stars', 0) >= 100]
    repo_list.sort(key=lambda x: x['stars'], reverse=True)
    final_list = repo_list[:30]

    # 3. 构建单一宽表
    header = "| Stars | 项目 | 描述 | 标签 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
    rows = []

    for r in final_list:
        stars = fmt_k(r['stars'])
        name = r.get('repo_name', 'Unknown')

        # 描述：截断到 60 字符
        desc = str(r.get('full_text', '') or '').replace('|', '').replace('\n', ' ')[:60]
        if not desc: desc = '-'

        # 处理标签显示
        raw_tags = r.get('topics', [])
        if isinstance(raw_tags, str):
            try: raw_tags = json.loads(raw_tags)
            except: raw_tags = []

        tag_str = " ".join([f"`{t}`" for t in raw_tags[:2]]) if raw_tags else "-"

        url = r.get('url', '#')

        rows.append(f"| ⭐ {stars} | **{name}** | {desc} | {tag_str} | [🔗]({url}) |")
        
    return {"🏆 GitHub Trending (Global Top 30)": {"header": header, "rows": rows}}
