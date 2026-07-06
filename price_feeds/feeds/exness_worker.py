"""
Exness MT5 price poller — runs in a child process.

Communicates with the parent process via JSON lines:
  stdin  (parent → child): subscribe/unsubscribe/shutdown commands
  stdout (child → parent): price updates and status messages

This process holds its own MetaTrader5 connection, isolated from the
ICMarkets connection in the main process.
"""

import json
import sys
import threading
import time

import MetaTrader5 as mt5

POLL_INTERVAL = 0.1  # 100ms


def _send(obj: dict):
    print(json.dumps(obj), flush=True)


def _read_commands(subscribed: set, lock: threading.Lock, shutdown_event: threading.Event):
    """Read JSON commands from stdin in a background thread.

    Returns (setting shutdown_event) either on an explicit shutdown command or
    when stdin reaches EOF — the latter means the parent process is gone. In
    that case the worker must release its MT5 terminal and exit rather than
    linger as an orphan; a relaunched parent spawns a fresh worker, and two
    workers cannot share the same Exness terminal.
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            cmd = json.loads(line)
        except json.JSONDecodeError:
            continue

        action = cmd.get("cmd")
        if action == "subscribe":
            symbol = cmd.get("symbol")
            if symbol:
                mt5.symbol_select(symbol, True)
                with lock:
                    subscribed.add(symbol)
                _send({"ack": "subscribe", "symbol": symbol})
        elif action == "unsubscribe":
            symbol = cmd.get("symbol")
            if symbol:
                with lock:
                    subscribed.discard(symbol)
                _send({"ack": "unsubscribe", "symbol": symbol})
        elif action == "shutdown":
            shutdown_event.set()
            return

    # stdin closed — parent exited. Stop polling so the terminal is released.
    shutdown_event.set()


def main():
    if len(sys.argv) < 5:
        _send({"error": "Usage: exness_worker.py <mt5_path> <login> <password> <server>"})
        sys.exit(1)

    mt5_path = sys.argv[1]
    login = int(sys.argv[2])
    password = sys.argv[3]
    server = sys.argv[4]

    if not mt5.initialize(mt5_path, login=login, password=password, server=server):
        error = mt5.last_error()
        _send({"error": f"MT5 init failed: {error}"})
        sys.exit(1)

    terminal_info = mt5.terminal_info()
    terminal_name = terminal_info.name if terminal_info else "unknown"
    _send({"status": "connected", "terminal": terminal_name})

    subscribed: set = set()
    lock = threading.Lock()
    shutdown_event = threading.Event()

    reader = threading.Thread(
        target=_read_commands, args=(subscribed, lock, shutdown_event), daemon=True
    )
    reader.start()

    last_prices: dict = {}

    while not shutdown_event.is_set():
        with lock:
            symbols = list(subscribed)

        for symbol in symbols:
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                continue

            key = (tick.bid, tick.ask)
            if last_prices.get(symbol) != key:
                last_prices[symbol] = key
                _send({"s": symbol, "b": tick.bid, "a": tick.ask, "t": tick.time})

        time.sleep(POLL_INTERVAL)

    mt5.shutdown()
    _send({"status": "shutdown"})


if __name__ == "__main__":
    main()
