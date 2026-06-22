# -*- coding: utf-8 -*-
"""
A5金叉死叉雷达系统 - 完全重构版 v2.1
==================================
核心修复（不放宽条件）：
1. 摆动点历史丢失 → 改为完整追踪（不能只取5个最新）
2. 状态机逻辑混乱 → 改为严格的递进式状态转移
3. recent[-3/-2/-1]访问越界 → 改为安全的索引检查
4. 状态在state==3后立即重置 → 改为检查金叉才重置
"""
import os
if os.name == 'nt':
    os.system('chcp 65001 > nul')

import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pytdx.hq import TdxHq_API
from collections import deque

try:
    import winsound
except ImportError:
    winsound = None

STOCK_FILE   = "我的板块.txt"
SERVER_IP    = "60.191.117.167"
SERVER_PORT  = 7709
MAX_WORKERS  = 32
SCAN_INTERVAL = 60

def trigger_alarm():
    """触发声音告警"""
    if winsound:
        for _ in range(3):
            winsound.Beep(1200, 150)
            time.sleep(0.05)

def load_stocks(filepath):
    """加载股票池"""
    stocks = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except:
        try:
            with open(filepath, 'r', encoding='gbk') as f:
                lines = f.readlines()
        except:
            print(f"[错误] 无法读取股票文件: {filepath}")
            return []
    
    for line in lines:
        code = line.strip()
        if code and code.isdigit() and len(code) == 6:
            market = 1 if code.startswith('6') else 0
            stocks.append((market, code))
    return stocks

class A5Indicator:
    """A5指标计算器"""
    def __init__(self, df):
        self.df = df.copy()
        self.close = df['close'].values
        self.high = df['high'].values
        self.low = df['low'].values
        self.times = df['datetime'].tolist()
        self.len = len(df)
        
        self.slow = None
        self.a1 = None
        self.calculate()
    
    def calculate(self):
        """计算A5指标"""
        close = pd.Series(self.close)
        high = pd.Series(self.high)
        low = pd.Series(self.low)
        
        fast = close.rolling(20).mean() - close.rolling(6).mean()
        self.slow = fast.rolling(3).mean().values
        
        wy = (2*close + high + low) / 3
        wy = wy.ewm(span=3, adjust=False).mean()
        wy = wy.ewm(span=3, adjust=False).mean()
        wy = wy.ewm(span=3, adjust=False).mean()
        xys0 = (wy - wy.shift(1)) / wy.shift(1) * 100
        self.a1 = xys0.rolling(23).mean().values
    
    def is_valid(self, i):
        """检查指标是否有效"""
        if i < 0 or i >= self.len:
            return False
        return not (np.isnan(self.slow[i]) or np.isnan(self.a1[i]))
    
    def get_crossover(self, i):
        """检测金叉（1=金叉，0=无，-1=死叉）"""
        if i < 1 or not self.is_valid(i) or not self.is_valid(i-1):
            return 0
        
        if self.a1[i-1] <= self.slow[i-1] and self.a1[i] > self.slow[i]:
            return 1
        elif self.a1[i-1] >= self.slow[i-1] and self.a1[i] < self.slow[i]:
            return -1
        return 0

class SwingDetector:
    """摆动点检测器"""
    def __init__(self, highs, lows, N=3):
        self.highs = highs
        self.lows = lows
        self.N = N
        self.len = len(highs)
        
        self.swing_highs = {}
        self.swing_lows = {}
    
    def update(self, current_idx):
        """严格确认摆动点"""
        confirm_idx = current_idx - self.N
        if confirm_idx < self.N or confirm_idx >= self.len - self.N:
            return
        
        # 严格条件：左右各N根
        is_high = (self.highs[confirm_idx] == 
                   np.max(self.highs[confirm_idx-self.N:confirm_idx+self.N+1]))
        is_low = (self.lows[confirm_idx] == 
                  np.min(self.lows[confirm_idx-self.N:confirm_idx+self.N+1]))
        
        if is_high:
            self.swing_highs[confirm_idx] = self.highs[confirm_idx]
        if is_low:
            self.swing_lows[confirm_idx] = self.lows[confirm_idx]
    
    def get_all_swings(self):
        """获取所有已确认的摆动点，按时间排序"""
        all_swings = []
        for idx, price in self.swing_highs.items():
            all_swings.append((idx, 'H', price))
        for idx, price in self.swing_lows.items():
            all_swings.append((idx, 'L', price))
        all_swings.sort(key=lambda x: x[0])
        return all_swings

class StructureAnalyzer:
    """
    结构分析器 - 修复逻辑
    核心修复：
    1. 保留完整的摆动点历史（不丢失）
    2. 状态严格递进：0 → 1 → 2 → 3
    3. 状态机触发条件明确
    """
    def __init__(self):
        self.state = 0  # 0初始 → 1找到LL → 2找到HH → 3找到HL
        self.hh1_price = None
        self.hl_price = None
        self.ll_price = None
        self.all_swings = []  # 修复：保存完整历史，不用deque
    
    def update(self, new_swings):
        """
        基于新的摆动点更新状态
        修复：new_swings是所有摆动点，而不是仅最新的5个
        """
        # 追加新摆动点到历史
        if new_swings:
            for swing in new_swings:
                if swing not in self.all_swings:
                    self.all_swings.append(swing)
        
        if len(self.all_swings) < 2:
            return
        
        # 只看最后3个摆动点来判断当前结构状态
        # （但历史完整保存，不会丢失）
        recent3 = self.all_swings[-3:] if len(self.all_swings) >= 3 else self.all_swings
        
        # 严格状态机：逐级递进
        if self.state == 0:
            # 找到初始LL
            if len(recent3) >= 1 and recent3[-1][1] == 'L':
                self.state = 1
                self.ll_price = recent3[-1][2]
        
        elif self.state == 1:
            # LL已找到，寻找HH（必须在LL之后且价格更高）
            if len(recent3) >= 2:
                if recent3[-2][1] == 'L' and recent3[-1][1] == 'H':
                    if recent3[-1][2] > recent3[-2][2]:  # HH > LL
                        self.state = 2
                        self.hh1_price = recent3[-1][2]
                        self.hl_price = None
        
        elif self.state == 2:
            # LL→HH已找到，寻找HL（必须在HH之后且价格更低）
            if len(recent3) >= 3:
                if (recent3[-3][1] == 'L' and recent3[-2][1] == 'H' and recent3[-1][1] == 'L'):
                    # 验证结构有效性
                    ll_p = recent3[-3][2]
                    hh_p = recent3[-2][2]
                    hl_p = recent3[-1][2]
                    
                    # 必须满足：LL < HH, HL < HH, HL > LL
                    if ll_p < hh_p and hl_p < hh_p and hl_p > ll_p:
                        self.state = 3
                        self.ll_price = ll_p
                        self.hh1_price = hh_p
                        self.hl_price = hl_p
            elif len(recent3) == 2 and recent3[-2][1] == 'H' and recent3[-1][1] == 'L':
                # 如果最后是HL，虽然没有前面的LL了，但假设前面有
                # （这不应该发生，因为我们会保持state==2）
                pass
    
    def is_ready(self):
        """结构是否就绪"""
        return self.state == 3 and self.hh1_price is not None
    
    def reset_after_signal(self):
        """信号触发后重置（准备下一个信号）"""
        self.state = 0
        self.ll_price = None
        self.hh1_price = None
        self.hl_price = None
        self.all_swings = []

class RealTimeAnalyzer:
    """实时分析器"""
    def __init__(self, stock_id, df):
        self.stock_id = stock_id
        self.df = df
        self.indicator = A5Indicator(df)
        self.swing = SwingDetector(df['high'].values, df['low'].values, N=3)
        self.structure = StructureAnalyzer()
        self.last_signal_time = None
    
    def analyze(self):
        """执行完整分析"""
        if len(self.df) < 30:
            return None
        
        last_trading_day = self.indicator.times[-1].strftime('%Y-%m-%d')
        latest_signal = None
        signal_time_str = None
        
        # 逐根K线处理
        for i in range(1, self.indicator.len):
            current_time = self.indicator.times[i]
            
            # 只处理当天数据
            if current_time.strftime('%Y-%m-%d') != last_trading_day:
                continue
            
            # 第1步：更新摆动点检测
            self.swing.update(i)
            
            # 第2步：获取完整的摆动点列表
            all_swings = self.swing.get_all_swings()
            
            # 第3步：更新结构分析（完整历史，不丢失）
            self.structure.update(all_swings)
            
            # 第4步：检测金叉
            crossover = self.indicator.get_crossover(i)
            
            # 第5步：融合判断（金叉 + 结构就绪 + 突破HH1）
            if crossover == 1:  # 发生金叉
                close_price = round(self.df['close'].iloc[i], 2)
                
                if (self.structure.is_ready() and 
                    self.structure.hh1_price and 
                    close_price > self.structure.hh1_price):
                    
                    current_time_sec = current_time
                    # 避免重复信号
                    if (self.last_signal_time is None or 
                        (current_time_sec - self.last_signal_time).total_seconds() >= 60):
                        
                        latest_signal = "[买入] A5金叉 + LL→HH→HL结构就绪 + 突破HH₁"
                        signal_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
                        self.last_signal_time = current_time_sec
                        
                        # 修复：确认信号后才重置结构
                        self.structure.reset_after_signal()
        
        if not latest_signal:
            return None
        
        return {
            "stock_id": self.stock_id,
            "latest_signal": latest_signal,
            "signal_time_str": signal_time_str,
            "last_bar_time": self.indicator.times[-1].strftime('%Y-%m-%d %H:%M'),
            "close": round(self.df['close'].iloc[-1], 2),
        }

def analyze(market, code):
    """分析单只股票"""
    stock_id = f"{'SH' if market==1 else 'SZ'}{code}"
    try:
        api = TdxHq_API(heartbeat=False, auto_retry=False)
        api.connect(SERVER_IP, SERVER_PORT)
        raw = api.get_security_bars(0, market, code, 0, 800)
        api.disconnect()
    except Exception as e:
        return None
    
    if not raw or len(raw) < 40:
        return None
    
    df = pd.DataFrame(raw)
    df['datetime'] = pd.to_datetime(df['datetime'])
    t = df['datetime'].dt.time
    df = df[(t >= pd.Timestamp('09:30').time()) &
            (t <= pd.Timestamp('15:00').time())].reset_index(drop=True)
    
    if len(df) < 30:
        return None
    
    analyzer = RealTimeAnalyzer(stock_id, df)
    return analyzer.analyze()

def run():
    """主程序"""
    print("="*70)
    print("[系统] A5金叉死叉雷达 · 5分钟线 · v2.1 修复版")
    print("="*70)
    
    stocks = load_stocks(STOCK_FILE)
    if not stocks:
        print("[错误] 股票池为空！请检查'我的板块.txt'文件")
        return
    
    print(f" >> 股票池: {len(stocks)} 只 | 服务器: {SERVER_IP}")
    print("="*70)
    
    log_file = "报警记录.txt"
    already_alerted = set()
    is_first_run = True
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            now_str = datetime.now().strftime("%H:%M:%S")
            
            if is_first_run:
                print(f"[{now_str}] 初始化扫描 {len(stocks)} 只...")
            else:
                print(f"[{now_str}] 扫描中...", end="\r")
            
            futures = {
                executor.submit(analyze, mkt, code): code
                for mkt, code in stocks
            }
            
            today_signals = []
            fresh_signals = []
            completed = 0
            total = len(stocks)
            
            for future in as_completed(futures):
                completed += 1
                print(f" >> 扫描进度: [{completed}/{total}]      ", end="\r")
                res = future.result()
                if not res:
                    continue
                
                key = f"{res['stock_id']}_{res['signal_time_str']}"
                
                if is_first_run:
                    if '买入' in res['latest_signal']:
                        today_signals.append(res)
                        already_alerted.add(key)
                else:
                    if key not in already_alerted and '买入' in res['latest_signal']:
                        fresh_signals.append(res)
                        already_alerted.add(key)
            
            if is_first_run:
                if today_signals:
                    print(f"\n{'='*70}")
                    print(f"[盘点] 今日信号（共{len(today_signals)}只）：")
                    print("-"*70)
                    for r in sorted(today_signals, key=lambda x: x['signal_time_str']):
                        print(f"  {r['signal_time_str']} | {r['stock_id']} | {r['latest_signal']} | 价格: {r['close']:.2f}")
                    print("="*70)
                else:
                    print(f"[{now_str}] 今日暂无A5信号")
                print(f"[{now_str}] 护航中，每{SCAN_INTERVAL}秒扫一次\n")
                is_first_run = False
            
            if fresh_signals:
                print(f"\n{'='*70}")
                print(f"[{now_str}] 新信号！")
                print("-"*70)
                popup_lines = []
                for res in fresh_signals:
                    line = f"{res['signal_time_str']} | {res['stock_id']} | {res['latest_signal']} | {res['close']:.2f}"
                    print(f"  {line}")
                    popup_lines.append(line)
                    try:
                        with open(log_file, "a", encoding="gbk", errors="ignore") as f:
                            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}\n")
                    except:
                        pass
                print("-"*70)
                trigger_alarm()
                try:
                    import ctypes
                    ctypes.windll.user32.MessageBoxW(0, "\n".join(popup_lines), "A5金叉死叉预警", 0x40 | 0x1000)
                except:
                    pass
            elif not is_first_run:
                print(f"[{now_str}] 暂无新信号        ", end="\r")
            
            time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run()
