"""
清理 Central-Bank/twitter/ 根目录下的历史文件
规则：只保留今天和昨天的文件，删除其他（它们已经在子文件夹里了）
"""
import os, sys
from datetime import datetime, timezone, timedelta
from github import Github, Auth

PRIVATE_BANK_ID = "wenfp108/Central-Bank"
GITHUB_TOKEN = os.environ.get("GH_PAT")

if not GITHUB_TOKEN:
    sys.exit("❌ GH_PAT 缺失")

gh = Github(auth=Auth.Token(GITHUB_TOKEN))
repo = gh.get_repo(PRIVATE_BANK_ID)

def main():
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"🔍 清理 twitter/ 根目录（保留 {today} 和 {yesterday}）")

    try:
        files = repo.get_contents("twitter")
    except Exception as e:
        print(f"❌ 读取失败: {e}")
        return

    # 只看根目录下的 .json 文件（不递归进子文件夹）
    root_jsons = [f for f in files if f.name.endswith(".json")]
    print(f"   根目录 JSON 文件: {len(root_jsons)}")

    to_delete = []
    to_keep = []
    for f in root_jsons:
        if today in f.name or yesterday in f.name:
            to_keep.append(f)
        else:
            to_delete.append(f)

    print(f"   保留: {len(to_keep)} | 待删除: {len(to_delete)}")

    if not to_delete:
        print("✅ 无需清理")
        return

    # 二次确认
    print(f"\n⚠️ 即将删除 {len(to_delete)} 个历史文件：")
    for f in to_delete[:5]:
        print(f"   - {f.name}")
    if len(to_delete) > 5:
        print(f"   ... 还有 {len(to_delete)-5} 个")

    # 执行删除
    deleted = 0
    for f in to_delete:
        try:
            repo.delete_file(f.path, f"🗑️ 清理冗余: {f.name}", f.sha)
            deleted += 1
            print(f"   🗑️ {f.name}")
        except Exception as e:
            print(f"   ❌ {f.name}: {e}")

    print(f"\n🎉 完成: 删除 {deleted}/{len(to_delete)} 个文件")
    print(f"   保留: {len(to_keep)} 个今日/昨日文件")

if __name__ == "__main__":
    main()
