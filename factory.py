import pandas as pd
import hashlib, json, os, requests, subprocess, time, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
import importlib.util
import config as cfg


class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = cfg.AI_API_URL
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.v3_model = cfg.AI_MODEL
        self.vault_path = None
        self.memory = {}

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists():
            print(f"⚠️ Masters 目录不存在: {self.masters_path}")
            return masters

        # First load base module and add to sys.modules
        base_path = self.masters_path / "base.py"
        if base_path.exists():
            try:
                spec = importlib.util.spec_from_file_location("base", base_path)
                base_module = importlib.util.module_from_spec(spec)
                sys.modules["base"] = base_module
                spec.loader.exec_module(base_module)
            except Exception as e:
                print(f"⚠️ base.py 加载失败: {e}")

        for file_path in self.masters_path.glob("*.py"):
            if file_path.name.startswith("__") or file_path.name == "base.py":
                continue
            try:
                name = file_path.stem
                spec = importlib.util.spec_from_file_location(name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if hasattr(module, 'audit'):
                    masters[name] = module
                    print(f"✅ 已加载 Master: {name}")
            except Exception as e:
                print(f"⚠️ Master {file_path.name} 加载失败: {e}")
        return masters

    def build_day_memory(self, vault_path):
        day_str = datetime.now().strftime('%Y%m%d')
        instructions_dir = vault_path / "instructions"
        if not instructions_dir.exists():
            return set()

        day_processed_ids = set()
        print(f"🧐 正在加载今日全天记忆...")
        for f in instructions_dir.glob(f"teachings_{day_str}_*.jsonl"):
            try:
                with open(f, 'r', encoding='utf-8') as f_in:
                    for line in f_in:
                        try:
                            data = json.loads(line)
                            tid, m, rid = data.get('topic_id'), data.get('master'), data.get('ref_id')
                            if tid and m:
                                if tid not in self.memory:
                                    self.memory[tid] = {}
                                self.memory[tid][m] = data.get('output', "")
                            if rid:
                                day_processed_ids.add(rid)
                        except (json.JSONDecodeError, TypeError) as e:
                            print(f"   ⚠️ 记忆行解析失败: {e}")
                            continue
            except Exception as e:
                print(f"   ⚠️ 记忆文件读取失败 {f.name}: {e}")
        print(f"✅ 记忆构建：锁定 {len(day_processed_ids)} 个历史哈希")
        return day_processed_ids

    # ─────────────────────────────────────────────
    #  信号筛选：每个来源独立函数 + 统一调度器
    # ─────────────────────────────────────────────

    def _fetch_github(self, supabase):
        raw = supabase.table("raw_signals").select("*") \
            .eq("signal_type", "github") \
            .order("created_at", desc=True).limit(cfg.FETCH_LIMITS["github"]) \
            .execute().data or []
        unique = {}
        for r in raw:
            name = r.get('repo_name')
            if name and name not in unique:
                unique[name] = r
        picks = list(unique.values())[:cfg.SIGNAL_QUOTAS["github"]]
        print(f"✅ GitHub: 获 {len(picks)} 条")
        return picks

    def _fetch_papers(self, supabase):
        raw = supabase.table("raw_signals").select("*") \
            .eq("signal_type", "papers") \
            .order("created_at", desc=True).limit(cfg.FETCH_LIMITS["papers"]) \
            .execute().data or []
        unique = {}
        for r in raw:
            title = r.get('title') or r.get('headline')
            if not title and r.get('full_text'):
                title = r.get('full_text')[:30]
            if title and title not in unique:
                r['title'] = title
                unique[title] = r
        picks = list(unique.values())[:cfg.SIGNAL_QUOTAS["papers"]]
        print(f"✅ Papers: 获 {len(picks)} 条")
        return picks

    def _fetch_twitter(self, supabase):
        raw = supabase.table("raw_signals").select("*") \
            .eq("signal_type", "twitter") \
            .order("created_at", desc=True).limit(cfg.FETCH_LIMITS["twitter"]) \
            .execute().data or []
        vip_names = [v.lower() for v in cfg.TWITTER_VIP_LIST]
        def score(row):
            rt = row.get('retweets', 0)
            bm = row.get('bookmarks', 0)
            like = row.get('likes', 0)
            user = str(row.get('user_name', '')).lower()
            s = (rt * cfg.TWITTER_SCORE_RETWEET) + (bm * cfg.TWITTER_SCORE_BOOKMARK) + like
            if any(v in user for v in vip_names):
                s += cfg.TWITTER_VIP_BONUS_HIGH if (rt > cfg.TWITTER_VIP_HIGH_RT_THRESHOLD or like > cfg.TWITTER_VIP_HIGH_LIKE_THRESHOLD) else cfg.TWITTER_VIP_BONUS_LOW
            return s
        for r in raw:
            r['_rank'] = score(r)
        picks = sorted(raw, key=lambda x: x['_rank'], reverse=True)[:cfg.SIGNAL_QUOTAS["twitter"]]
        print(f"✅ Twitter: 获 {len(picks)} 条")
        return picks

    def _fetch_reddit(self, supabase):
        raw = supabase.table("raw_signals").select("*") \
            .eq("signal_type", "reddit") \
            .order("created_at", desc=True).limit(cfg.FETCH_LIMITS["reddit"]) \
            .execute().data or []
        unique = {r.get('url'): r for r in raw if r.get('url')}
        def score(row):
            return (row.get('score') or 0) * (1 + abs(float(row.get('vibe') or 0)))
        picks = sorted(unique.values(), key=score, reverse=True)[:cfg.SIGNAL_QUOTAS["reddit"]]
        print(f"✅ Reddit: 获 {len(picks)} 条")
        return picks

    def _fetch_polymarket(self, supabase):
        raw = supabase.table("raw_signals").select("*") \
            .eq("signal_type", "polymarket") \
            .order("created_at", desc=True).limit(cfg.FETCH_LIMITS["polymarket"]) \
            .execute().data or []
        unique = {}
        for p in raw:
            raw_json = p.get('raw_json')
            if isinstance(raw_json, str):
                try:
                    raw_json = json.loads(raw_json)
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"   ⚠️ Polymarket raw_json 解析失败: {e}")
                    raw_json = {}
            p['_parsed'] = raw_json
            slug = p.get('slug') or raw_json.get('slug')
            if slug:
                curr_liq = float(p.get('liquidity') or 0)
                if slug not in unique or curr_liq > float(unique[slug].get('liquidity', 0)):
                    unique[slug] = p

        def score(row):
            parsed = row['_parsed']
            liq = float(row.get('liquidity') or 0)
            tags = parsed.get('strategy_tags', [])
            for tag_name, bonus in cfg.POLY_STRATEGY_BONUS.items():
                if tag_name in tags:
                    return bonus + liq
            cat = str(row.get('category', '')).upper()
            if any(kw in cat for kw in cfg.POLY_CATEGORY_KEYWORDS):
                return cfg.POLY_CATEGORY_BONUS + liq
            return cfg.POLY_BASE_SCORE + liq

        picks = sorted(unique.values(), key=score, reverse=True)[:cfg.SIGNAL_QUOTAS["polymarket"]]
        print(f"✅ Polymarket: 获 {len(picks)} 条")
        return picks

    def fetch_elite_signals(self):
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 启动精锐筛选...")
            results = []
            results.extend(self._fetch_github(supabase))
            results.extend(self._fetch_papers(supabase))
            results.extend(self._fetch_twitter(supabase))
            results.extend(self._fetch_reddit(supabase))
            results.extend(self._fetch_polymarket(supabase))
            return results
        except Exception as e:
            print(f"⚠️ 筛选异常: {e}")
            return []

    def audit_process(self, row, processed_ids):
        topic_id = row.get('url') or row.get('slug') or row.get('repo_name') or "unknown"
        source = row.get('signal_type', 'unknown').lower()

        parts = [f"【Source: {source.upper()}】"]
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | Topics: {row.get('topics')}")
            parts.append(f"描述: {row.get('full_text') or '新项目发布'} | Link: {row.get('url')}")
        elif source == 'papers':
            parts.append(f"论文: {row.get('title')} | 期刊: {row.get('journal')}")
            parts.append(f"引用: {row.get('citations')} | 摘要: {row.get('full_text')}")
        elif source in ['twitter', 'reddit']:
            parts.append(f"用户: {row.get('user_name') or row.get('subreddit')} | Score: {row.get('_rank', 0)}")
            parts.append(f"内容: {row.get('full_text') or row.get('title')}")
        else:
            raw = row.get('_parsed') or row.get('raw_json') or {}
            parts.append(f"预测: {row.get('title')} | 问题: {row.get('question')}")
            parts.append(f"价格: {row.get('prices') or raw.get('outcome_prices')} | 流动性: ${raw.get('liquidity')}")

        content = "\n".join(parts)
        ref_id = hashlib.sha256(content.encode()).hexdigest()

        if ref_id in processed_ids:
            return []

        results = []
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought", "").strip(), r.split("### Output")[1].strip()
            return "Audit", r

        for name, mod in self.masters.items():
            prev_opinion = self.memory.get(topic_id, {}).get(name)
            drift_context = f"\n\n[历史记忆]：此前观点：'{prev_opinion}'。数据变动若触发逻辑反转，请在 Output 开头标记 [DRIFT_DETECTED]。" if prev_opinion else ""
            try:
                if hasattr(mod, 'audit'):
                    row['_drift_context'] = drift_context
                    row['full_text_formatted'] = content
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "topic_id": topic_id, "master": name,
                            "drift": "[DRIFT_DETECTED]" in o,
                            "source": source, "input": content, "thought": t, "output": o
                        }, ensure_ascii=False))
            except Exception as e:
                print(f"   ⚠️ Master {name} 审计异常 (topic={topic_id}): {e}")
                continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        (self.vault_path / "instructions").mkdir(parents=True, exist_ok=True)

        processed_ids = self.build_day_memory(self.vault_path)

        now = datetime.now()
        day_str = now.strftime('%Y%m%d')
        hour_str = now.strftime('%H')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}_{hour_str}.jsonl"

        signals = self.fetch_elite_signals()
        if not signals:
            return

        for i in range(0, len(signals), cfg.AUDIT_BATCH_SIZE):
            chunk = signals[i:i + cfg.AUDIT_BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=cfg.AUDIT_WORKERS) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))

            added = []
            for r_list in res:
                if r_list:
                    added.extend(r_list)
                    for r_json in r_list:
                        processed_ids.add(json.loads(r_json).get('ref_id'))

            if added:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write('\n'.join(added) + '\n')

    def call_ai(self, model, sys_prompt, usr_prompt):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": usr_prompt}
            ],
            "temperature": 0.7
        }

        for attempt in range(cfg.AI_MAX_RETRIES):
            try:
                resp = requests.post(self.api_url, json=payload, headers=headers, timeout=cfg.AI_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                content = data['choices'][0]['message']['content']
                return "SUCCESS", content
            except requests.exceptions.Timeout:
                print(f"   ⏳ AI 调用超时 (第 {attempt+1}/{cfg.AI_MAX_RETRIES} 次)")
            except requests.exceptions.HTTPError as e:
                print(f"   ⚠️ AI HTTP 错误 {e.response.status_code} (第 {attempt+1}/{cfg.AI_MAX_RETRIES} 次)")
            except (KeyError, IndexError):
                print(f"   ⚠️ AI 返回格式异常 (第 {attempt+1}/{cfg.AI_MAX_RETRIES} 次)")
                return "ERROR", "AI_FORMAT_ERROR"
            except Exception as e:
                print(f"   ⚠️ AI 调用异常: {e} (第 {attempt+1}/{cfg.AI_MAX_RETRIES} 次)")

            if attempt < cfg.AI_MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)
                print(f"   💤 等待 {wait}s 后重试...")
                time.sleep(wait)

        return "ERROR", "AI_FAIL"

    def git_push_assets(self):
        """防御型推送：解决身份未知、未提交更改以及远程拒绝问题"""
        if not self.vault_path: return
        cwd = self.vault_path
        
        # === 🛡️ 新增：自愈逻辑 ===
        # 检查是否存在僵尸 rebase 锁，如果有，先杀掉
        rebase_dir = cwd / ".git" / "rebase-merge"
        if rebase_dir.exists():
            print("🚑 检测到僵尸 Rebase 锁，正在执行战地急救...")
            subprocess.run(["git", "rebase", "--abort"], cwd=cwd)
            if rebase_dir.exists(): # 如果 abort 失败，直接物理删除
                import shutil
                shutil.rmtree(rebase_dir)
        # =======================

        # 1. 强制注入身份
        subprocess.run(["git", "config", "user.email", "bot@factory.com"], cwd=cwd)
        subprocess.run(["git", "config", "user.name", "Cognitive Bot"], cwd=cwd)
        # 解决 pull 时的 rebase 策略警告
        subprocess.run(["git", "config", "pull.rebase", "true"], cwd=cwd)

        # 2. 【顺序调整】先 add 和 commit，把你的 1000 多条数据存进本地仓库
        subprocess.run(["git", "add", "."], cwd=cwd)
        
        # 检查是否有东西可以 commit
        diff_status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
        if diff_status.returncode == 0:
            print("💤 没有发现新资产，跳过同步。")
            return

        # 3. 执行 Commit
        commit_msg = f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_msg], cwd=cwd)

        # 4. 【同步远程】此时再 pull --rebase，Git 就能顺畅地把远程改动接在你的 commit 之后
        print("🔄 正在通过 rebase 同步远程仓库...")
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd)

        # 5. 最终推送
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=cwd, capture_output=True, text=True)
        
        if push_res.returncode == 0:
            print("🚀 认知资产已成功同步至中央银行。")
        else:
            print(f"❌ 最终推送失败: {push_res.stderr}")

if __name__ == "__main__":
    factory = UniversalFactory()
    factory.process_and_ship()
