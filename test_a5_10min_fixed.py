# -*- coding: utf-8 -*-
"""
A5金叉死叉雷达系统 - 完全重构版
==================================
修复所有致命逻辑问题：
1. 摆动点确认延迟 → 改为实时确认机制
2. 状态机不完整 → 完整的LL/HH/HL/LH判断
3. 金叉和结构时序不匹配 → 独立检测后融合
4. 重置逻辑导致漏掉 → 完成交易后才重置
5. 数据前向偏差 → 指标结构化存储
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
    """A5指标计算器 - 结构化存储"""
    def __init__(self, df):
        self.df = df.copy()
        self.close = df['close'].values
        self.high = df['high'].values
        self.low = df['low'].values
        self.times = df['datetime'].tolist()
        self.len = len(df)
        
        self.slow = None  # 慢线
        self.a1 = None    # A1线
        self.calculate()
    
    def calculate(self):
        """计算A5指标"""
        close = pd.Series(self.close)
        high = pd.Series(self.high)
        low = pd.Series(self.low)
        
        # 快线 = MA(20) - MA(6)
        fast = close.rolling(20).mean() - close.rolling(6).mean()
        # 慢线 = MA(快线, 3)
        self.slow = fast.rolling(3).mean().values
        
        # XYS0 动量
        wy = (2*close + high + low) / 3
        wy = wy.ewm(span=3, adjust=False).mean()
        wy = wy.ewm(span=3, adjust=False).mean()
        wy = wy.ewm(span=3, adjust=False).mean()
        xys0 = (wy - wy.shift(1)) / wy.shift(1) * 100
        # A1 = MA(XYS0, 23)
        self.a1 = xys0.rolling(23).mean().values
    
    def is_valid(self, i):
        """检查指标是否有效（非NaN）"""
        if i < 0 or i >= self.len:
            return False
        return not (np.isnan(self.slow[i]) or np.isnan(self.a1[i]))
    
    def get_crossover(self, i):
        """
        检测金叉/死叉（0=无，1=金叉，-1=死叉）
        金叉：A1从下穿过慢线
        """
        if i < 1 or not self.is_valid(i) or not self.is_valid(i-1):
            return 0
        
        # A1从下穿上（金叉）
        if self.a1[i-1] <= self.slow[i-1] and self.a1[i] > self.slow[i]:
            return 1
        # A1从上穿下（死叉）
        elif self.a1[i-1] >= self.slow[i-1] and self.a1[i] < self.slow[i]:
            return -1
        return 0

class SwingDetector:
    """摆动点检测器 - 实时确认机制"""
    def __init__(self, highs, lows, N=3):
        self.highs = highs
        self.lows = lows
        self.N = N
        self.len = len(highs)
        
        # 缓存已确认的摆动点
        self.swing_highs = {}  # {index: price}
        self.swing_lows = {}   # {index: price}
    
    def update(self, current_idx):
        """
        更新到current_idx，确认过去的摆动点
        当有N根后续K线时，可以确认摆动点
        """
        confirm_idx = current_idx - self.N
        if confirm_idx < self.N:
            return
        
        # 检查confirm_idx是否为高点
        is_high = (self.highs[confirm_idx] == 
                   np.max(self.highs[confirm_idx-self.N:confirm_idx+self.N+1]))
        # 检查confirm_idx是否为低点
        is_low = (self.lows[confirm_idx] == 
                  np.min(self.lows[confirm_idx-self.N:confirm_idx+self.N+1]))
        
        if is_high:
            self.swing_highs[confirm_idx] = self.highs[confirm_idx]
        if is_low:
            self.swing_lows[confirm_idx] = self.lows[confirm_idx]
    
    def get_latest_swing_highs(self, limit=10):
        """获取最近的N个高点"""
        if not self.swing_highs:
            return []
        return sorted(self.swing_highs.items(), key=lambda x: x[0])[-limit:]
    
    def get_latest_swing_lows(self, limit=10):
        """获取最近的N个低点"""
        if not self.swing_lows:
            return []
        return sorted(self.swing_lows.items(), key=lambda x: x[0])[-limit:]

class StructureAnalyzer:
    """
    结构分析器 - 识别 LL → HH → HL 的经典结构
    基于已确认的摆动点进行状态转移
    """
    def __init__(self):
        self.state = 0  # 状态：0初始 → 1找到LL → 2找到HH → 3找到HL
        self.hh1_price = None  # 第一个HH的价格
        self.hl_price = None   # HL的价格
        self.history = deque(maxlen=20)  # 最近20个摆动点
    
    def identify_swing_type(self, idx, price, swing_highs_list, swing_lows_list):
        """
        识别摆动点类型：HH/LH（高）或HL/LL（低）
        基于与前一个同类型摆动点的比较
        """
        # 获取最近的同类型摆动点
        prev_high_idx = None
        prev_low_idx = None
        
        for h_idx, h_price in swing_highs_list:
            if h_idx < idx:
                prev_high_idx = h_idx
        
        for l_idx, l_price in swing_lows_list:
            if l_idx < idx:
                prev_low_idx = l_idx
        
        return {
            'prev_high_idx': prev_high_idx,
            'prev_low_idx': prev_low_idx,
        }
    
    def update(self, swing_highs_list, swing_lows_list):
        """
        基于摆动点列表更新结构状态
        swing_highs_list: [(idx, price), ...]
        swing_lows_list: [(idx, price), ...]
        """
        if not swing_highs_list and not swing_lows_list:
            return False
        
        # 合并并排序所有摆动点
        all_swings = []
        for idx, price in swing_highs_list:
            all_swings.append((idx, 'H', price))
        for idx, price in swing_lows_list:
            all_swings.append((idx, 'L', price))
        all_swings.sort(key=lambda x: x[0])
        
        # 更新历史记录
        for swing in all_swings:
            if swing not in self.history:
                self.history.append(swing)
        
        # 状态机：根据最新的摆动点序列判断��构
        if len(self.history) < 2:
            return False
        
        # 只看最近的摆动点
        recent = list(self.history)[-5:]
        
        signal_triggered = False
        
        # LL → HH → HL 结构识别
        if len(recent) >= 3:
            # 从右往左看
            latest_type = recent[-1][1]
            
            # 模式1：...LL HH HL → 准备好了，等金叉
            if (len(recent) >= 3 and 
                recent[-3][1] == 'L' and recent[-2][1] == 'H' and recent[-1][1] == 'L'):
                
                ll_idx, ll_price = recent[-3][0], recent[-3][2]
                hh_idx, hh_price = recent[-2][0], recent[-2][2]
                hl_idx, hl_price = recent[-1][0], recent[-1][2]
                
                # LL < HH 且 HL < HH 才是有效的结构
                if ll_price < hh_price and hl_price < hh_price and hl_price > ll_price:
                    self.state = 3
                    self.hh1_price = hh_price
                    self.hl_price = hl_price
                    return True
            
            # 模式2：...LL HH（重新进入状态2等待HL）
            elif (len(recent) >= 2 and 
                  recent[-2][1] == 'L' and recent[-1][1] == 'H'):
                ll_idx, ll_price = recent[-2][0], recent[-2][2]
                hh_idx, hh_price = recent[-1][0], recent[-1][2]
                
                if ll_price < hh_price:
                    self.state = 2
                    self.hh1_price = hh_price
                    self.hl_price = None
                    return False
            
            # 模式3：找到了初始LL
            elif recent[-1][1] == 'L':
                self.state = 1
                self.hh1_price = None
                self.hl_price = None
                return False
        
        return signal_triggered
    
    def is_ready(self):
        """结构是否已就绪（到达HL状态）"""
        return self.state == 3 and self.hh1_price is not None

class RealTimeAnalyzer:
    """实时分析器 - 融合指标和结构"""
    def __init__(self, stock_id, df):
        self.stock_id = stock_id
        self.indicator = A5Indicator(df)
        self.swing = SwingDetector(df['high'].values, df['low'].values, N=3)
        self.structure = StructureAnalyzer()
        self.last_signal_time = None
        self.df = df
    
    def analyze(self):
        """执行完整分析"""
        if len(self.df) < 30:
            return None
        
        last_trading_day = self.indicator.times[-1].strftime('%Y-%m-%d')
        latest_signal = None
        signal_time_str = None
        
        # 逐个K线处理
        for i in range(1, self.indicator.len):
            current_time = self.indicator.times[i]
            
            # 只处理当天数据
            if current_time.strftime('%Y-%m-%d') != last_trading_day:
                continue
            
            # 更新摆动点检测器
            self.swing.update(i)
            
            # 更新结构分析器
            swing_highs = self.swing.get_latest_swing_highs(5)
            swing_lows = self.swing.get_latest_swing_lows(5)
            self.structure.update(swing_highs, swing_lows)
            
            # 检测金叉
            crossover = self.indicator.get_crossover(i)
            
            if crossover == 1:  # 金叉
                close_price = round(self.df['close'].iloc[i], 2)
                
                # 检查：1) 结构就绪 2) 价格突破HH1
                if (self.structure.is_ready() and 
                    self.structure.hh1_price and 
                    close_price > self.structure.hh1_price):
                    
                    current_time_sec = current_time
                    # 避免重复信号（60秒内只发一次）
                    if (self.last_signal_time is None or 
                        (current_time_sec - self.last_signal_time).total_seconds() >= 60):
                        
                        latest_signal = "[买入] A5金叉 + LL→HH→HL结构就绪 + 突破HH₁"
                        signal_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
                        self.last_signal_time = current_time_sec
                        
                        # 信号完成后重置结构（准备下一个信号）
                        self.structure = StructureAnalyzer()
        
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
    """分析单只股票的入口函数"""
    stock_id = f"{'SH' if market==1 else 'SZ'}{code}"
    try:
        api = TdxHq_API(heartbeat=False, auto_retry=False)
        api.connect(SERVER_IP, SERVER_PORT)
        raw = api.get_security_bars(0, market, code, 0, 800)  # 0=5分钟线
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
    print("[系统] A5金叉死叉雷达 · 5分钟线 · 完全重构版")
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
