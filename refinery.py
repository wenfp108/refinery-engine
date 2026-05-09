import os, json, base64, requests, importlib.util, sys, time
import pandas as pd
import io
from datetime import datetime, timedelta, timezone
from supabase import create_client
from github import Github, Auth


def retry(func, *args, retries=3, delay=5, **kwargs):
    """通用重试包装器"""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"⚠️ {func.__name__} 失败 (第{attempt+1}次): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

# === 🛡️ 1. 核心配置 ===
PRIVATE_BANK_ID = "wenfp108/Central-Bank" 
GITHUB_TOKEN = os.environ.get("GH_PAT") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ [审计异常] 环境变量缺失。")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
auth = Auth.Token(GITHUB_TOKEN)
gh_client = Github(auth=auth)
private_repo = gh_client.get_repo(PRIVATE_BANK_ID)

# === 🧩 2. 插件发现系统 (强制指向 raw_signals) ===
def get_all_processors():
    procs = {}
    proc_dir = "./processors"
    if not os.path.exists(proc_dir): return procs
    for filename in os.listdir(proc_dir):
        if filename.endswith(".py") and not filename.startswith("__"):
            name = filename[:-3]
            try:
                spec = importlib.util.spec_from_file_location(f"mod_{name}", os.path.join(proc_dir, filename))
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                procs[name] = {
                    "module": mod,
                    "source_name": name,  # 记录来源名 (twitter, github...)
                    "table_name": "raw_signals",  # 🔥 强制统一表名
                }
            except Exception as e: print(f"⚠️ 插件 {name} 加载失败: {e}")
    return procs

# === ⏱️ 辅助：检查数据新鲜度 ===
def get_data_freshness(table_name, source_name=None):
    try:
        query = supabase.table(table_name).select("created_at").neq("created_at", "null")
        
        # 如果是 raw_signals，需要按 signal_type 过滤
        if table_name == "raw_signals" and source_name:
            query = query.eq("signal_type", source_name)
            
        res = query.order("created_at", desc=True).limit(1).execute()
        
        if not res.data: return (False, 9999, "无数据")
        
        last_time_str = res.data[0]['created_at']
        if not last_time_str: return (False, 9999, "无时间戳")

        try:
            last_time_str = last_time_str.replace('Z', '+00:00')
            last_time = datetime.fromisoformat(last_time_str)
        except (ValueError, TypeError) as e:
            print(f"   ⚠️ 时间解析失败 '{last_time_str}': {e}")
            return (False, 9999, last_time_str)
        
        now = datetime.now(timezone(timedelta(hours=8)))
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone.utc)
        
        diff = now - last_time
        minutes_ago = int(diff.total_seconds() / 60)
        
        return (minutes_ago <= 65, minutes_ago, last_time.strftime('%H:%M'))
    except Exception as e:
        return (True, 0, "CheckError")

# === 🔥 3. 战报工厂 (辅助生成) ===
def generate_hot_reports(processors_config):
    # 注意：Factory.py 是主战场，Refinery 里的这个函数主要用于简单的 Markdown 归档
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    year = bj_now.strftime('%Y')
    month = bj_now.strftime('%m')
    day = bj_now.strftime('%d')
    hour = bj_now.strftime('%H')
    
    file_name = f"{hour}点战报.md"
    report_path = f"reports/{year}/{month}/{day}/{file_name}"
    
    date_display = bj_now.strftime('%Y-%m-%d %H:%M')
    md_report = f"# 🚀 Architect's Alpha 情报审计 ({date_display})\n\n"
    md_report += "> **机制说明**：全源智能去重 | 资金流向优先 | 自动归档\n\n"

    has_content = False

    for source_name, config in processors_config.items():
        if hasattr(config["module"], "get_hot_items"):
            try:
                table = config["table_name"]
                is_fresh, mins_ago, _ = get_data_freshness(table, source_name)
                
                # 如果数据太老 (超过12小时) 就不写进简报了
                if not is_fresh and mins_ago > 720: 
                    continue 

                sector_data = config["module"].get_hot_items(supabase, table)
                if not sector_data: continue

                has_content = True
                
                freshness_tag = "" if is_fresh else f" (⚠️ 数据滞后 {int(mins_ago/60)}h)"
                md_report += f"## 📡 来源：{source_name.upper()}{freshness_tag}\n"
                
                for sector, data in sector_data.items():
                    md_report += f"### 🏷️ 板块：{sector}\n"
                    if isinstance(data, dict):
                        if "header" in data: md_report += data["header"] + "\n"
                        if "rows" in data and isinstance(data["rows"], list):
                            for row in data["rows"]: md_report += row + "\n"
                    elif isinstance(data, list):
                        md_report += "| 信号 | 内容 | 🔗 |\n| :--- | :--- | :--- |\n"
                        for item in data:
                            md_report += f"| {item.get('score','-')} | {item.get('full_text','-')} | [🔗]({item.get('url','#')}) |\n"
                    md_report += "\n"
            except Exception as e:
                print(f"   ⚠️ 来源 {source_name} 报告生成失败: {e}")

    if not has_content:
        md_report += "\n\n**🛑 本轮扫描全域静默，请查阅历史归档。**"

    def _write_report():
        try:
            old = private_repo.get_contents(report_path)
            private_repo.update_file(old.path, f"📊 Update: {file_name}", md_report, old.sha)
            print(f"📝 战报更新：{report_path}")
        except Exception:
            private_repo.create_file(report_path, f"🚀 New: {file_name}", md_report)
            print(f"📝 战报创建：{report_path}")

    try:
        retry(_write_report)
    except Exception as e:
        print(f"❌ 写入失败（重试 3 次后）: {e}")

    # 清理7天前的旧报告
    try:
        cutoff = bj_now - timedelta(days=7)
        _cleanup_old_reports(cutoff)
    except Exception as e:
        print(f"⚠️ 清理旧报告失败: {e}")


def _cleanup_old_reports(cutoff):
    """删除 reports/ 下超过7天的战报文件（每次只删最老的一天，避免API过载）"""
    try:
        reports_dir = private_repo.get_contents("reports")
    except Exception:
        return

    # 收集所有日期目录
    old_dirs = []
    for year_dir in reports_dir:
        if not year_dir.type == 'dir' or not year_dir.name.isdigit():
            continue
        try:
            month_dirs = private_repo.get_contents(year_dir.path)
        except Exception:
            continue
        for month_dir in month_dirs:
            if not month_dir.type == 'dir':
                continue
            try:
                day_dirs = private_repo.get_contents(month_dir.path)
            except Exception:
                continue
            for day_dir in day_dirs:
                if not day_dir.type == 'dir':
                    continue
                try:
                    date_str = f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
                    dir_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    if dir_date < cutoff.date():
                        old_dirs.append(day_dir)
                except Exception:
                    continue

    if not old_dirs:
        return

    # 只删最老的一天（每次运行删24个文件，不超API限制）
    oldest = old_dirs[0]
    try:
        files = private_repo.get_contents(oldest.path)
        for f in files:
            private_repo.delete_file(f.path, f"🗑️ 清理旧报告: {f.name}", f.sha)
            print(f"🗑️ 删除: {f.path}")
        print(f"✅ 已清理 {oldest.path}")
    except Exception as e:
        print(f"⚠️ 清理 {oldest.path} 失败: {e}")

# === 🚜 4. 滚动收割 (✅ 修正版：只清理 raw_signals) ===
def perform_grand_harvest(processors_config):
    print("⏰ 触发每日滚动收割 (Archive & Purge)...")
    cutoff_date = (datetime.now() - timedelta(days=7)).replace(hour=23, minute=59, second=59)
    cutoff_str = cutoff_date.isoformat()
    date_tag = cutoff_date.strftime('%Y%m%d')

    # ✅ 修正：列表里只有 raw_signals，彻底删除旧表引用
    target_tables = ["raw_signals"] 

    for table in target_tables:
        try:
            # 1. 归档逻辑 (将7天前的数据打包上传 GitHub)
            res = supabase.table(table).select("*").lt("created_at", cutoff_str).execute()
            data = res.data
            
            if data:
                # 转换为 Parquet 上传 GitHub
                df = pd.DataFrame(data)

                # 🔥🔥 [新增修复] 强制统一 raw_json 列类型为字符串，解决 pyarrow 混合类型报错 🔥🔥
                if 'raw_json' in df.columns:
                    df['raw_json'] = df['raw_json'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else str(x))
                
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
                
                year_month = cutoff_date.strftime('%Y/%m')
                # 🔥 修改开始：使用当前时间（精确到秒）作为文件名后缀
                current_run_tag = datetime.now().strftime('%Y%m%d_%H%M%S')
                archive_path = f"archive/{year_month}/{table}_{current_run_tag}.parquet"
                # 🔥 修改结束
                
                try:
                    private_repo.create_file(
                        path=archive_path,
                        message=f"🏛️ Archive: {table} batch",
                        content=buffer.getvalue(),
                        branch="main" 
                    )
                except Exception as upload_e:
                    print(f"   ⚠️ 归档文件上传失败 (可能已存在): {upload_e}")
                    # 🔥 修改开始：添加刹车逻辑
                    print("   🛑以此停止：为防止数据丢失，跳过删除步骤！")
                    return 
                    # 🔥 修改结束
                
                # 2. 清理逻辑 (删除已归档的数据)
                # 使用循环分批删除，防止超时
                ids = [item['id'] for item in data if 'id' in item]
                if ids:
                    batch_size = 500
                    for i in range(0, len(ids), batch_size):
                        batch = ids[i : i + batch_size]
                        supabase.table(table).delete().in_("id", batch).execute()
                    print(f"   🗑️ {table}: 已清理 {len(ids)} 条过期数据")
            else:
                pass # 没有过期数据
                
        except Exception as e:
            # 只有 raw_signals 会走到这里，旧表根本不会报错
            print(f"   ⚠️ [{table}] 收割任务跳过: {e}")

# === 🏦 5. 搬运逻辑 (核心：JSON -> Supabase) ===
def process_and_upload(path, sha, config):
    # 检查哨兵：文件是否处理过
    check = supabase.table("processed_files").select("file_sha").eq("file_sha", sha).execute()
    if check.data: return 0

    try:
        content_file = private_repo.get_contents(path)
        raw_data = json.loads(base64.b64decode(content_file.content).decode('utf-8'))

        # 调用 Processor 清洗数据
        items = config["module"].process(raw_data, path)
        count = len(items) if items else 0

        if items:
            # 🔥 注入核心字段 signal_type
            for item in items:
                item['signal_type'] = config["source_name"]

                # 兼容性处理：确保 raw_json 存在
                if 'raw_json' not in item:
                    item['raw_json'] = item.copy()

            # 分批写入 raw_signals
            for i in range(0, len(items), 500):
                supabase.table("raw_signals").insert(items[i : i+500]).execute()

            # 登记哨兵
            supabase.table("processed_files").upsert({
                "file_sha": sha,
                "file_path": path,
                "engine": config["source_name"],
                "item_count": count
            }).execute()
            return count
    except Exception as e:
        print(f"❌ 处理文件 {path} 失败: {e}")
    return 0

def sync_bank_to_sql(processors_config, full_scan=False):
    current_time = datetime.now().strftime('%H:%M:%S')
    mode_str = "全量补录" if full_scan else "1小时增量"
    print(f"[{current_time}] 🏦 巡检开始: {mode_str}提取")
    stats = {name: 0 for name in processors_config.keys()}
    
    if full_scan:
        try:
            contents = private_repo.get_contents("")
            while contents:
                file_content = contents.pop(0)
                if file_content.type == "dir":
                    contents.extend(private_repo.get_contents(file_content.path))
                elif file_content.name.endswith(".json"):
                    source_key = file_content.path.split('/')[0]
                    if source_key in processors_config:
                        added = process_and_upload(file_content.path, file_content.sha, processors_config[source_key])
                        stats[source_key] += added
        except Exception as e: print(f"❌ Scan Error: {e}")
    else:
        # 增量模式：只检查最近 24 小时以内的 Commit
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        commits = private_repo.get_commits(since=since)
        for commit in commits:
            for f in commit.files:
                if f.filename.endswith('.json'):
                    source_key = f.filename.split('/')[0]
                    if source_key in processors_config:
                        added = process_and_upload(f.filename, f.sha, processors_config[source_key])
                        stats[source_key] += added

    for source, count in stats.items():
        if count > 0: print(f"✅ {source} (+{count}) -> raw_signals")

if __name__ == "__main__":
    all_procs = get_all_processors()
    is_full_scan = (os.environ.get("FORCE_FULL_SCAN") == "true")
    
    sync_bank_to_sql(all_procs, full_scan=is_full_scan)
    generate_hot_reports(all_procs)
    perform_grand_harvest(all_procs)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 审计任务圆满完成。")
