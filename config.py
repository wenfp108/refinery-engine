"""
集中配置 — 所有硬编码的魔法数字都在这里。
环境变量不在这里管，继续用 os.environ.get()。
"""

# === AI 调用 ===
AI_API_URL = "https://token-plan-sgp.xiaomimimo.com/v1/chat/completions"
AI_MODEL = "mimo-v2.5-pro"
AI_MAX_RETRIES = 3
AI_TIMEOUT = 60  # 秒

# === 信号筛选配额 (每个来源保留多少条进入审计) ===
SIGNAL_QUOTAS = {
    "github":     20,
    "papers":     30,
    "twitter":    60,
    "reddit":     30,
    "polymarket": 80,
}

# === Supabase 拉取上限 ===
FETCH_LIMITS = {
    "github":     100,
    "papers":     100,
    "twitter":    500,
    "reddit":     500,
    "polymarket": 800,
}

# === Twitter VIP 列表 ===
TWITTER_VIP_LIST = [
    'Karpathy', 'Musk', 'Vitalik', 'LeCun',
    'Dalio', 'Naval', 'Sama', 'PaulG',
]

# === Twitter 评分权重 ===
TWITTER_SCORE_RETWEET = 5
TWITTER_SCORE_BOOKMARK = 10
TWITTER_VIP_BONUS_HIGH = 10000   # VIP 且高互动
TWITTER_VIP_BONUS_LOW = 500      # VIP 但低互动
TWITTER_VIP_HIGH_RT_THRESHOLD = 10
TWITTER_VIP_HIGH_LIKE_THRESHOLD = 50

# === Polymarket 策略标签优先级 ===
POLY_STRATEGY_BONUS = {
    "TAIL_RISK": 10_000_000,
}
POLY_CATEGORY_KEYWORDS = ["ECONOMY", "TECH"]
POLY_CATEGORY_BONUS = 5_000_000
POLY_BASE_SCORE = 1_000_000

# === 审计批次 ===
AUDIT_BATCH_SIZE = 50
AUDIT_WORKERS = 20

# === 归档 ===
ARCHIVE_RETENTION_DAYS = 7
ARCHIVE_DELETE_BATCH_SIZE = 500
