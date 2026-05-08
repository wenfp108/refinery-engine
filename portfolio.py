"""
模拟投资组合追踪器
配置和数据存储在 Central-Bank 仓库
"""

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

# ---- 路径 ----

def get_bank():
    for p in [
        Path(__file__).parent.parent / 'Central-Bank',
        Path.home() / 'Downloads' / 'Central-Bank',
        Path('../Central-Bank'),
    ]:
        if (p / 'data' / 'portfolio' / 'portfolio_config.py').exists():
            return p.resolve()
    print("ERROR: Central-Bank not found")
    sys.exit(1)

BANK = get_bank()
sys.path.insert(0, str(BANK / 'data' / 'portfolio'))
from portfolio_config import ALLOCATION, ASSET_CLASS, US_SYMBOLS, A_SHARE_SYMBOLS, CRYPTO_SYMBOLS, USD_CNY, MONTHLY_INVESTMENT, INITIAL_CAPITAL, CPI_ANNUAL

DATA = BANK / 'data' / 'portfolio'
DATA.mkdir(parents=True, exist_ok=True)
REPORT = BANK / 'reports' / 'portfolio'
REPORT.mkdir(parents=True, exist_ok=True)

# ---- 价格 ----

def fetch_prices():
    all_prices = {}

    # 动态获取 USD/CNY 汇率
    fx = USD_CNY
    try:
        import yfinance as yf
        fx_data = yf.download('CNY=X', period='5d', progress=False)
        fx_close = fx_data['Close']
        if hasattr(fx_close, 'columns'):
            fx_close = fx_close.iloc[:, 0]
        fx_val = fx_close.dropna().iloc[-1]
        fx = round(float(fx_val.item() if hasattr(fx_val, 'item') else fx_val), 4)
        print(f"  USD/CNY: {fx}")
    except Exception as e:
        print(f"  USD/CNY 获取失败，用配置值 {USD_CNY}: {e}")

    # 美股
    try:
        import yfinance as yf
        for sym in US_SYMBOLS:
            try:
                d = yf.download(sym, period='5d', progress=False)
                close = d['Close']
                if hasattr(close, 'columns'):
                    close = close.iloc[:, 0]
                val = float(close.dropna().iloc[-1].item() if hasattr(close.dropna().iloc[-1], 'item') else close.dropna().iloc[-1])
                all_prices[sym] = round(val * fx, 2)
            except Exception as e:
                print(f"  {sym}: {e}")
    except ImportError:
        print("  yfinance not installed")

    # A股
    try:
        import akshare as ak
        df = ak.fund_etf_spot_em()
        for sym in A_SHARE_SYMBOLS:
            row = df[df['代码'] == sym]
            if not row.empty:
                all_prices[sym] = round(float(row.iloc[0]['最新价']), 4)
            else:
                print(f"  {sym}: A股价格未找到")
    except Exception as e:
        print(f"  A股价格获取失败: {e}")

    # BTC
    try:
        ids = ','.join(CRYPTO_SYMBOLS.values())
        r = requests.get(f'https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd', timeout=10)
        for sym, cg_id in CRYPTO_SYMBOLS.items():
            if cg_id in r.json():
                all_prices[sym] = round(r.json()[cg_id]['usd'] * fx, 2)
            else:
                print(f"  {sym}: CoinGecko 未返回数据")
    except Exception as e:
        print(f"  BTC价格获取失败: {e}")

    print(f"Prices: {len(all_prices)} symbols")
    return all_prices

# ---- 持仓（存在 Central-Bank） ----

def load_positions():
    f = DATA / 'positions.json'
    if f.exists():
        return json.loads(f.read_text())
    return {}

def save_positions(positions):
    (DATA / 'positions.json').write_text(json.dumps(positions, indent=2, default=str))

# ---- 定投 ----

def simulate_dca(amount=None, prices=None, trade_date=None):
    if amount is None:
        amount = MONTHLY_INVESTMENT
    if prices is None:
        prices = fetch_prices()
    if trade_date is None:
        trade_date = str(date.today())

    print(f"\nDCA: ¥{amount:,.0f} on {trade_date}")
    positions = load_positions()
    trades = []

    skipped_amount = 0
    for symbol, ratio in ALLOCATION.items():
        if symbol == 'CASH':
            continue
        target = amount * ratio
        price = prices.get(symbol)
        if not price or price <= 0:
            print(f"  SKIP {symbol} (no price, ¥{target:,.0f} → cash)")
            skipped_amount += target
            continue

        shares = target / price
        old = positions.get(symbol, {'shares': 0, 'avg_cost': 0})
        old_s, old_c = float(old['shares']), float(old['avg_cost'])
        new_s = old_s + shares
        new_c = (old_s * old_c + shares * price) / new_s if new_s > 0 else 0

        positions[symbol] = {
            'asset_class': ASSET_CLASS.get(symbol, 'unknown'),
            'shares': round(new_s, 6),
            'avg_cost': round(new_c, 4),
        }
        trades.append({'symbol': symbol, 'shares': round(shares, 6), 'price': round(price, 2), 'amount': round(target, 2), 'date': trade_date})
        print(f"  BUY {symbol}: {shares:.4f} @ ¥{price:.2f} = ¥{target:,.0f}")

    # 现金（含未买到的钱）
    cash = amount * ALLOCATION.get('CASH', 0) + skipped_amount
    old_cash = float(positions.get('CASH', {}).get('shares', 0))
    positions['CASH'] = {'asset_class': 'cash', 'shares': round(old_cash + cash, 2), 'avg_cost': 1.0}

    save_positions(positions)

    # 交易记录追加
    trades_file = DATA / 'trades.jsonl'
    with open(trades_file, 'a') as f:
        for t in trades:
            f.write(json.dumps(t, ensure_ascii=False) + '\n')

    return trades

# ---- 每日快照 + 报告 ----

def daily_snapshot(prices=None):
    if prices is None:
        prices = fetch_prices()

    positions = load_positions()
    if not positions:
        print("No positions. Run: python portfolio.py dca")
        return

    today = str(date.today())
    detail = {}
    total = 0

    # 从最近的快照获取缺失价格的后备数据
    fallback_prices = {}
    for i in range(1, 11):
        prev_file = DATA / f'snapshot_{date.today() - timedelta(days=i)}.json'
        if prev_file.exists():
            prev = json.loads(prev_file.read_text())
            for sym, d in prev.get('positions', {}).items():
                if 'price' in d and sym not in fallback_prices:
                    fallback_prices[sym] = d['price']
            break

    for sym, pos in positions.items():
        shares = float(pos['shares'])
        avg_cost = float(pos.get('avg_cost', 0))
        if sym == 'CASH':
            detail[sym] = {'shares': shares, 'value': shares, 'pnl': 0}
            total += shares
            continue
        price = prices.get(sym)
        if not price:
            # 用前一次快照的价格，避免周末/假期导致资产"消失"
            price = fallback_prices.get(sym)
            if price:
                print(f"  {sym}: 用前次快照价格 ¥{price}")
            else:
                print(f"  {sym}: 无价格数据，跳过")
                continue
        value = shares * price
        cost = shares * avg_cost
        pnl = value - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0
        detail[sym] = {'shares': round(shares, 4), 'price': price, 'value': round(value, 2), 'pnl': round(pnl, 2), 'pnl_pct': round(pnl_pct, 2)}
        total += value

    # 累计收益 = (当前市值 - 总投入) / 总投入
    # 总投入 = 所有持仓的成本 + 现金
    cost_basis = sum(float(p['shares']) * float(p.get('avg_cost', 0)) for s, p in positions.items() if s != 'CASH')
    cash_total = float(positions.get('CASH', {}).get('shares', 0))
    total_deposited = cost_basis + cash_total
    cum_ret = ((total - total_deposited) / total_deposited * 100) if total_deposited > 0 else 0

    # 日收益（向前找最近的快照，最多10天，兼容周末/假期）
    daily_ret = None
    for i in range(1, 11):
        prev_file = DATA / f'snapshot_{date.today() - timedelta(days=i)}.json'
        if prev_file.exists():
            prev = json.loads(prev_file.read_text())
            daily_ret = round((total - prev['total_value']) / prev['total_value'] * 100, 4)
            break

    snapshot = {
        'date': today, 'total_value': round(total, 2),
        'daily_return': daily_ret, 'cumulative_return': round(cum_ret, 4),
        'positions': detail,
    }
    (DATA / f'snapshot_{today}.json').write_text(json.dumps(snapshot, indent=2, default=str))

    # 找今日最佳和最差
    performers = [(s, d['pnl_pct']) for s, d in detail.items() if s != 'CASH' and 'pnl_pct' in d]
    best = max(performers, key=lambda x: x[1]) if performers else ('-', 0)
    worst = min(performers, key=lambda x: x[1]) if performers else ('-', 0)

    # 通胀调整（年化CPI）
    # 找第一天有持仓的日期，计算天数
    trade_dates = []
    trades_f = DATA / 'trades.jsonl'
    if trades_f.exists():
        with open(trades_f) as f:
            for line in f:
                t = json.loads(line)
                trade_dates.append(t['date'])
    first_date = min(trade_dates) if trade_dates else today
    days_held = (date.fromisoformat(today) - date.fromisoformat(first_date)).days
    inflation_rate = CPI_ANNUAL
    inflation_adj = (1 + inflation_rate) ** (days_held / 365) - 1
    real_ret = cum_ret - inflation_adj * 100

    # 追加到报告
    daily_sign = '+' if (daily_ret or 0) >= 0 else ''
    cum_sign = '+' if cum_ret >= 0 else ''
    real_sign = '+' if real_ret >= 0 else ''
    row = f"| {today} | ¥{total:,.0f} | ¥{total_deposited:,.0f} | {cum_sign}{cum_ret:.2f}% | {real_sign}{real_ret:.2f}% | {daily_sign}{daily_ret or 0:.2f}% | {best[0]} {best[1]:+.1f}% | {worst[0]} {worst[1]:+.1f}% |\n"

    report_file = REPORT / 'portfolio.md'
    if report_file.exists():
        content = report_file.read_text()
    else:
        content = "# 模拟组合日报\n\n| 日期 | 总市值 | 投入 | 累计收益 | 扣通胀 | 日收益 | 最佳 | 最差 |\n|------|--------|------|---------|--------|--------|------|------|\n"

    # 插入到表头之后
    lines = content.split('\n')
    header_idx = next((i for i, l in enumerate(lines) if l.startswith('|------')), None)
    if header_idx is not None:
        lines.insert(header_idx + 1, row.rstrip())
    else:
        lines.append(row.rstrip())
    report_file.write_text('\n'.join(lines))

    print(f"\nSnapshot {today}: ¥{total:,.0f} | cum:{cum_sign}{cum_ret:.2f}% | daily:{daily_sign}{daily_ret or 0:.2f}%")
    print(f"  Best: {best[0]} {best[1]:+.1f}% | Worst: {worst[0]} {worst[1]:+.1f}%")

# ---- 回测 ----

def backtest(start_date='2026-03-01'):
    """从指定日期开始回测，每月1号DCA，每日生成快照"""
    import yfinance as yf

    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    today = date.today()

    # 清空持仓和快照，从零开始
    (DATA / 'positions.json').write_text('{}')
    for f in DATA.glob('snapshot_*.json'):
        f.unlink()
    # 清空交易记录
    trades_file = DATA / 'trades.jsonl'
    if trades_file.exists():
        trades_file.unlink()

    print(f"=== 回测从 {start} 到 {today} ===")

    # 批量下载美股历史数据
    print("下载历史价格...")
    # 下载USD/CNY汇率历史
    try:
        fx_hist = yf.download('CNY=X', start=str(start), end=str(today + timedelta(days=1)), progress=False)
        fx_close = fx_hist['Close']
        if hasattr(fx_close, 'columns'):
            fx_close = fx_close.iloc[:, 0]
        fx_close = fx_close.ffill()  # 假期用前一个交易日汇率
    except Exception:
        fx_close = None
    # 美股用原symbol，A股加 .SS 后缀用yfinance
    yf_symbols = list(US_SYMBOLS)
    a_share_map = {}  # '510300.SS' -> '510300'
    for sym in A_SHARE_SYMBOLS:
        yf_sym = f'{sym}.SS'
        yf_symbols.append(yf_sym)
        a_share_map[yf_sym] = sym

    hist = yf.download(yf_symbols, start=str(start), end=str(today + timedelta(days=1)), progress=False)
    close_df = hist['Close']
    # A股假期没数据，用前一个交易日的价格填充（避免快照跳水）
    for yf_sym in a_share_map:
        if yf_sym in close_df.columns:
            close_df[yf_sym] = close_df[yf_sym].ffill()
    trading_dates = sorted(close_df.index)

    # DCA 日期：每月1号（或最近的交易日）
    dca_dates = []
    d = start.replace(day=1)
    while d <= today:
        # 找到 >= d 的最近交易日
        for td in trading_dates:
            if td.date() >= d:
                dca_dates.append(td.date())
                break
        # 下个月
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)

    print(f"DCA 日期: {dca_dates}")
    print(f"交易日数: {len(trading_dates)}")

    def get_fx(target_date):
        """获取某天的USD/CNY汇率"""
        if fx_close is None:
            return USD_CNY
        for td in trading_dates:
            if td.date() >= target_date:
                v = fx_close.loc[td]
                if hasattr(v, 'item'):
                    v = v.item()
                if v == v:  # not NaN
                    return round(float(v), 4)
        return USD_CNY

    # 执行 DCA
    for dca_d in dca_dates:
        # 获取当天价格
        prices = {}
        fx = get_fx(dca_d)
        # 美股 + A股（统一从yfinance获取）
        all_yf = list(US_SYMBOLS) + [f'{s}.SS' for s in A_SHARE_SYMBOLS]
        for yf_sym in all_yf:
            try:
                # 确定实际symbol和是否需要汇率转换
                if yf_sym in a_share_map:
                    sym = a_share_map[yf_sym]
                    use_fx = False  # A股本身是人民币
                else:
                    sym = yf_sym
                    use_fx = True

                if yf_sym not in close_df.columns:
                    continue
                row = close_df[yf_sym]
                # 找最近的交易日
                val = None
                for td in trading_dates:
                    if td.date() >= dca_d:
                        v = row.loc[td]
                        if hasattr(v, 'item'):
                            v = v.item()
                        if not (v != v):  # not NaN
                            val = v
                            break
                if val is None:
                    continue
                prices[sym] = round(float(val) * fx, 2) if use_fx else round(float(val), 4)
            except Exception:
                pass
        # BTC
        try:
            ids = ','.join(CRYPTO_SYMBOLS.values())
            r = requests.get(f'https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&date={dca_d.strftime("%d-%m-%Y")}', timeout=10)
            for sym, cg_id in CRYPTO_SYMBOLS.items():
                if cg_id in r.json():
                    prices[sym] = round(r.json()[cg_id]['usd'] * fx, 2)
        except Exception:
            pass

        # 用 INITIAL_CAPITAL 作为首次，之后用 MONTHLY_INVESTMENT
        amount = INITIAL_CAPITAL if dca_d == dca_dates[0] else MONTHLY_INVESTMENT
        print(f"\n--- DCA {dca_d}: ¥{amount:,.0f} ---")
        simulate_dca(amount=amount, prices=prices, trade_date=str(dca_d))

    # 生成每日快照（用历史收盘价）
    print(f"\n--- 生成每日快照 ---")
    for td in trading_dates:
        td_date = td.date() if hasattr(td, 'date') else td
        if td_date < start:
            continue
        prices = {}
        fx = get_fx(td_date)
        # 美股 + A股
        for yf_sym in all_yf:
            try:
                if yf_sym in a_share_map:
                    sym = a_share_map[yf_sym]
                    use_fx = False
                else:
                    sym = yf_sym
                    use_fx = True
                if yf_sym not in close_df.columns:
                    continue
                val = close_df[yf_sym].loc[td]
                if hasattr(val, 'item'):
                    val = val.item()
                if val != val:  # NaN
                    continue
                prices[sym] = round(float(val) * fx, 2) if use_fx else round(float(val), 4)
            except Exception:
                pass
        # BTC（用当天价格）
        try:
            ids = ','.join(CRYPTO_SYMBOLS.values())
            r = requests.get(f'https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd', timeout=10)
            for sym, cg_id in CRYPTO_SYMBOLS.items():
                if cg_id in r.json():
                    prices[sym] = round(r.json()[cg_id]['usd'] * fx, 2)
        except Exception:
            pass

        # 临时修改 date.today() 不现实，直接传 prices 给 snapshot
        # 需要改造 daily_snapshot 支持指定日期
        _snapshot_for_date(td_date, prices)

    print("\n=== 回测完成 ===")
    # 读最终持仓
    positions = load_positions()
    total = sum(float(p['shares']) * float(p.get('avg_cost', 0)) for s, p in positions.items() if s != 'CASH')
    total += float(positions.get('CASH', {}).get('shares', 0))
    print(f"最终持仓市值: ¥{total:,.0f}")


def _snapshot_for_date(snap_date, prices):
    """为指定日期生成快照（回测用）"""
    positions = load_positions()
    if not positions:
        return

    detail = {}
    total = 0

    # 从最近的快照获取缺失价格的后备数据
    fallback_prices = {}
    for i in range(1, 11):
        pf = DATA / f'snapshot_{snap_date - timedelta(days=i)}.json'
        if pf.exists():
            prev = json.loads(pf.read_text())
            for sym, d in prev.get('positions', {}).items():
                if 'price' in d and sym not in fallback_prices:
                    fallback_prices[sym] = d['price']
            break

    for sym, pos in positions.items():
        shares = float(pos['shares'])
        avg_cost = float(pos.get('avg_cost', 0))
        if sym == 'CASH':
            detail[sym] = {'shares': shares, 'value': shares, 'pnl': 0}
            total += shares
            continue
        price = prices.get(sym)
        if not price:
            price = fallback_prices.get(sym)
            if not price:
                continue
        value = shares * price
        cost = shares * avg_cost
        pnl = value - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0
        detail[sym] = {'shares': round(shares, 4), 'price': price, 'value': round(value, 2), 'pnl': round(pnl, 2), 'pnl_pct': round(pnl_pct, 2)}
        total += value

    cost_basis = sum(float(p['shares']) * float(p.get('avg_cost', 0)) for s, p in positions.items() if s != 'CASH')
    cash_total = float(positions.get('CASH', {}).get('shares', 0))
    total_deposited = cost_basis + cash_total
    cum_ret = ((total - total_deposited) / total_deposited * 100) if total_deposited > 0 else 0

    # 日收益
    daily_ret = None
    prev_file = DATA / f'snapshot_{snap_date - timedelta(days=1)}.json'
    # 向前找最近的快照
    for i in range(1, 10):
        pf = DATA / f'snapshot_{snap_date - timedelta(days=i)}.json'
        if pf.exists():
            prev = json.loads(pf.read_text())
            daily_ret = round((total - prev['total_value']) / prev['total_value'] * 100, 4)
            break

    snapshot = {
        'date': str(snap_date), 'total_value': round(total, 2),
        'daily_return': daily_ret, 'cumulative_return': round(cum_ret, 4),
        'positions': detail,
    }
    (DATA / f'snapshot_{snap_date}.json').write_text(json.dumps(snapshot, indent=2, default=str))

    # 追加到报告
    performers = [(s, d['pnl_pct']) for s, d in detail.items() if s != 'CASH' and 'pnl_pct' in d]
    best = max(performers, key=lambda x: x[1]) if performers else ('-', 0)
    worst = min(performers, key=lambda x: x[1]) if performers else ('-', 0)

    # 通胀调整（年化2%）
    trades_f = DATA / 'trades.jsonl'
    trade_dates = []
    if trades_f.exists():
        with open(trades_f) as f:
            for line in f:
                t = json.loads(line)
                trade_dates.append(t['date'])
    first_date = min(trade_dates) if trade_dates else str(snap_date)
    days_held = (snap_date - date.fromisoformat(first_date)).days
    inflation_adj = (1 + CPI_ANNUAL) ** (days_held / 365) - 1
    real_ret = cum_ret - inflation_adj * 100

    daily_sign = '+' if (daily_ret or 0) >= 0 else ''
    cum_sign = '+' if cum_ret >= 0 else ''
    real_sign = '+' if real_ret >= 0 else ''
    row = f"| {snap_date} | ¥{total:,.0f} | ¥{total_deposited:,.0f} | {cum_sign}{cum_ret:.2f}% | {real_sign}{real_ret:.2f}% | {daily_sign}{daily_ret or 0:.2f}% | {best[0]} {best[1]:+.1f}% | {worst[0]} {worst[1]:+.1f}% |\n"

    report_file = REPORT / 'portfolio.md'
    if report_file.exists():
        content = report_file.read_text()
    else:
        content = "# 模拟组合日报\n\n| 日期 | 总市值 | 投入 | 累计收益 | 扣通胀 | 日收益 | 最佳 | 最差 |\n|------|--------|------|---------|--------|--------|------|------|\n"

    lines = content.split('\n')
    header_idx = next((i for i, l in enumerate(lines) if l.startswith('|------')), None)
    if header_idx is not None:
        lines.insert(header_idx + 1, row.rstrip())
    else:
        lines.append(row.rstrip())
    report_file.write_text('\n'.join(lines))


# ---- CLI ----

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'snapshot':
        daily_snapshot()
    elif cmd == 'init':
        # 初始资金一次性买入
        simulate_dca(amount=INITIAL_CAPITAL, trade_date=str(date.today()))
        daily_snapshot()
    elif cmd == 'dca':
        # 每月定投
        simulate_dca(amount=MONTHLY_INVESTMENT, trade_date=str(date.today()))
        daily_snapshot()
    elif cmd == 'backtest':
        start = sys.argv[2] if len(sys.argv) > 2 else '2026-03-01'
        backtest(start)
    else:
        print("Usage: python portfolio.py [init|dca|snapshot|backtest]")
        print("  init      - 初始资金一次性买入")
        print("  dca       - 每月定投买入")
        print("  snapshot  - 记录当日快照")
        print("  backtest  - 回测（默认从2026-03-01开始）")
