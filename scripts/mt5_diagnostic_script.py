"""
Diagnose why MT5 is returning identical bid/ask prices
"""

import MetaTrader5 as mt5
from datetime import datetime
import pytz


def diagnose_mt5():
    """Complete MT5 diagnostic"""

    print("\n" + "=" * 60)
    print("MT5 DIAGNOSTIC REPORT")
    print("=" * 60)

    # Initialize MT5
    if not mt5.initialize():
        print("❌ Failed to initialize MT5")
        return

    # 1. Terminal Info
    terminal = mt5.terminal_info()
    print("\n1. TERMINAL INFO:")
    print(f"   Connected: {terminal.connected}")
    print(f"   Trade allowed: {terminal.trade_allowed}")
    print(f"   Name: {terminal.name}")
    print(f"   Company: {terminal.company}")
    print(f"   Path: {terminal.path}")

    # 2. Account Info
    account = mt5.account_info()
    print("\n2. ACCOUNT INFO:")
    print(f"   Login: {account.login}")
    print(f"   Server: {account.server}")
    print(f"   Balance: {account.balance}")
    print(f"   Account type: {'DEMO' if account.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO else 'REAL'}")
    print(f"   Trade allowed: {account.trade_allowed}")
    print(f"   Trade expert: {account.trade_expert}")

    # 3. Current Time
    print("\n3. TIME INFO:")
    local_time = datetime.now()
    utc_time = datetime.now(pytz.UTC)
    print(f"   Local time: {local_time}")
    print(f"   UTC time: {utc_time}")
    print(f"   Day of week: {local_time.strftime('%A')}")

    # 4. Symbol Info for EURUSD
    symbol = "EURUSD"
    print(f"\n4. SYMBOL INFO ({symbol}):")

    symbol_info = mt5.symbol_info(symbol)
    if symbol_info:
        print(f"   Visible: {symbol_info.visible}")
        print(f"   Selected: {symbol_info.select}")
        print(f"   Session deals: {symbol_info.session_deals}")
        print(f"   Session buy orders: {symbol_info.session_buy_orders}")
        print(f"   Session sell orders: {symbol_info.session_sell_orders}")
        print(f"   Volume: {symbol_info.volume}")
        print(f"   Bid: {symbol_info.bid}")
        print(f"   Ask: {symbol_info.ask}")
        print(f"   Point: {symbol_info.point}")
        print(f"   Spread: {symbol_info.spread}")
        print(f"   Trade mode: {symbol_info.trade_mode}")

        # Check if market is open
        if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            print("   ⚠️ MARKET IS CLOSED (TRADE_MODE_DISABLED)")
        elif symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_CLOSEONLY:
            print("   ⚠️ MARKET IS CLOSE-ONLY")
        elif symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            print("   ✅ MARKET IS OPEN (FULL TRADING)")
    else:
        print(f"   ❌ Symbol {symbol} not found")

    # 5. Try to get tick with different methods
    print(f"\n5. TICK DATA METHODS:")

    # Method 1: symbol_info_tick
    tick = mt5.symbol_info_tick(symbol)
    if tick:
        print(f"   symbol_info_tick:")
        print(f"      Bid: {tick.bid}")
        print(f"      Ask: {tick.ask}")
        print(f"      Last: {tick.last}")
        print(f"      Volume: {tick.volume}")
        print(f"      Time: {datetime.fromtimestamp(tick.time)}")
        print(f"      Flags: {tick.flags}")

    # Method 2: copy_ticks_from
    ticks = mt5.copy_ticks_from(symbol, datetime.now(), 1, mt5.COPY_TICKS_INFO)
    if ticks is not None and len(ticks) > 0:
        latest = ticks[-1]
        print(f"   copy_ticks_from:")
        print(f"      Bid: {latest['bid']}")
        print(f"      Ask: {latest['ask']}")
        print(f"      Time: {datetime.fromtimestamp(latest['time'])}")

    # Method 3: copy_rates_from
    rates = mt5.copy_rates_from(symbol, mt5.TIMEFRAME_M1, datetime.now(), 1)
    if rates is not None and len(rates) > 0:
        latest = rates[-1]
        print(f"   copy_rates_from (M1):")
        print(f"      Open: {latest['open']}")
        print(f"      High: {latest['high']}")
        print(f"      Low: {latest['low']}")
        print(f"      Close: {latest['close']}")
        print(f"      Time: {datetime.fromtimestamp(latest['time'])}")

    # 6. Check multiple symbols
    print("\n6. MULTIPLE SYMBOLS CHECK:")
    symbols_to_check = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "GOLD", "BTCUSD"]

    for sym in symbols_to_check:
        tick = mt5.symbol_info_tick(sym)
        if tick:
            spread_pips = (tick.ask - tick.bid) * 10000 if "JPY" not in sym else (tick.ask - tick.bid) * 100
            print(f"   {sym:8} Bid: {tick.bid:.5f}, Ask: {tick.ask:.5f}, Spread: {spread_pips:.1f} pips")
        else:
            # Try with suffix
            for suffix in ['.a', '_m', '.r', '']:
                test_sym = f"{sym}{suffix}"
                tick = mt5.symbol_info_tick(test_sym)
                if tick:
                    spread_pips = (tick.ask - tick.bid) * 10000
                    print(f"   {test_sym:8} Bid: {tick.bid:.5f}, Ask: {tick.ask:.5f}, Spread: {spread_pips:.1f} pips")
                    break
            else:
                print(f"   {sym:8} - Not found")

    # 7. Market Watch
    print("\n7. MARKET WATCH SYMBOLS:")
    symbols = mt5.symbols_get()
    if symbols:
        print(f"   Total symbols available: {len(symbols)}")

        # Check first 10 visible symbols
        visible_symbols = [s for s in symbols if s.visible][:10]
        print(f"   Visible symbols (first 10):")
        for s in visible_symbols:
            print(f"      {s.name}")

    mt5.shutdown()

    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)

    # Recommendations
    print("\n📋 RECOMMENDATIONS:")
    print("1. If market is closed, wait for market open (Sunday evening EST)")
    print("2. If using demo account, spreads might be 0 during off-hours")
    print("3. Try adding symbols to Market Watch in MT5 terminal")
    print("4. Ensure 'Enable algorithmic trading' is checked in MT5")
    print("5. Try reconnecting to a different server if available")


if __name__ == "__main__":
    diagnose_mt5()