"""
一次性补录脚本：扫描 Central-Bank/twitter/ 下所有未处理的 JSON 文件
只处理 twitter 目录，不影响其他数据源
"""
import os, json, base64, sys
from datetime import datetime, timezone
from supabase import create_client
from github import Github, Auth

# 复用 refinery 的配置和 processor
from processors.twitter import process

# 配置
PRIVATE_BANK_ID = "wenfp108/Central-Bank"
GITHUB_TOKEN = os.environ.get("GH_PAT")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ 环境变量缺失")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
gh = Github(auth=Auth.Token(GITHUB_TOKEN))
repo = gh.get_repo(PRIVATE_BANK_ID)

def main():
    print("🔍 扫描 Central-Bank/twitter/ ...")

    # 1. 预加载已处理 SHA
    res = supabase.table("processed_files").select("file_sha").eq("engine", "twitter").execute()
    processed = set(r["file_sha"] for r in (res.data or []))
    print(f"   已处理: {len(processed)} 个文件")

    # 2. 列出 twitter/ 下所有文件
    try:
        files = repo.get_contents("twitter")
    except Exception as e:
        print(f"❌ 无法读取 twitter/: {e}")
        return

    json_files = [f for f in files if f.name.endswith(".json")]
    unprocessed = [f for f in json_files if f.sha not in processed]
    print(f"   总文件: {len(json_files)} | 未处理: {len(unprocessed)}")

    if not unprocessed:
        print("✅ 全部已处理，无需补录")
        return

    # 3. 逐个处理
    total_items = 0
    for i, f in enumerate(unprocessed, 1):
        try:
            raw = json.loads(base64.b64decode(f.content).decode("utf-8"))
            items = process(raw, f.path)
            if not items:
                print(f"   [{i}/{len(unprocessed)}] {f.name}: 0 条（跳过）")
                continue

            # 注入 signal_type
            for item in items:
                item["signal_type"] = "twitter"
                if "raw_json" not in item:
                    item["raw_json"] = item.copy()

            # 分批写入
            for batch_start in range(0, len(items), 500):
                supabase.table("raw_signals").insert(items[batch_start:batch_start+500]).execute()

            # 登记哨兵
            supabase.table("processed_files").upsert({
                "file_sha": f.sha,
                "file_path": f.path,
                "engine": "twitter",
                "item_count": len(items)
            }).execute()

            total_items += len(items)
            print(f"   [{i}/{len(unprocessed)}] ✅ {f.name}: +{len(items)} 条")

        except Exception as e:
            print(f"   [{i}/{len(unprocessed)}] ❌ {f.name}: {e}")

    print(f"\n🎉 补录完成: {len(unprocessed)} 个文件, +{total_items} 条数据")

if __name__ == "__main__":
    main()
