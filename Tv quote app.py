# TradingView Quote — Render Service
# Connects TradingView WS, gets snapshot for all symbols, returns data
#
# Deploy on Render as a new Web Service (separate from yahoo-proxy)
# Start command: python app.py

from flask import Flask, request, jsonify
from flask_cors import CORS
import websocket
import json
import threading
import time
from datetime import datetime, timezone

app = Flask(__name__)
CORS(app)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

FIELDS = [
    'lp', 'ch', 'chp', 'prev_close_price',
    'open_price', 'high_price', 'low_price', 'volume',
    'rch', 'rchp', 'rtc', 'rtc_time',
    'current_session', 'update_mode',
    'short_name', 'exchange', 'currency_code',
    'type', 'typespecs', 'timezone', 'country_code',
    'pricescale', 'is_tradable',
]

TV_HEADERS = {
    'Origin': 'https://in.tradingview.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36',
}

SESSION = 'qs_py1'

def tv_msg(obj):
    msg = json.dumps(obj, separators=(',', ':'))
    return f'~m~{len(msg)}~m~{msg}'

def parse_messages(raw):
    import re
    results = []
    for match in re.finditer(r'~m~\d+~m~(\{.*?\})(?=~m~|$)', raw):
        try:
            results.append(json.loads(match.group(1)))
        except Exception:
            pass
    return results

def fetch_tv_quotes(symbols, timeout=8):
    data = {}
    completed = set()
    done = threading.Event()

    def on_open(ws):
        ws.send(tv_msg({'m': 'quote_create_session', 'p': [SESSION]}))
        ws.send(tv_msg({'m': 'quote_set_fields', 'p': [SESSION] + FIELDS}))
        for sym in symbols:
            ws.send(tv_msg({'m': 'quote_add_symbols', 'p': [SESSION, sym]}))

    def on_message(ws, raw):
        # Reply to heartbeat
        if '~h~' in raw:
            import re
            match = re.search(r'~h~\d+', raw)
            if match:
                hb = match.group(0)
                ws.send(f'~m~{len(hb)}~m~{hb}')

        for msg in parse_messages(raw):
            if msg.get('m') == 'qsd' and len(msg.get('p', [])) > 1:
                n = msg['p'][1].get('n')
                v = msg['p'][1].get('v')
                if n and v:
                    data[n] = {**data.get(n, {}), **v}

            if msg.get('m') == 'quote_completed' and len(msg.get('p', [])) > 1:
                completed.add(msg['p'][1])
                if all(s in completed for s in symbols):
                    ws.close()
                    done.set()

    def on_error(ws, error):
        done.set()

    def on_close(ws, code, reason):
        done.set()

    date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    ws_url = f'wss://data.tradingview.com/socket.io/websocket?from=markets%2Fusa%2F&date={date}&auth=sessionid'

    ws = websocket.WebSocketApp(
        ws_url,
        header=[f'{k}: {v}' for k, v in TV_HEADERS.items()],
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    thread = threading.Thread(target=ws.run_forever)
    thread.daemon = True
    thread.start()

    done.wait(timeout=timeout)

    return data


@app.route('/quote')
def quote():
    symbols_param = request.args.get('symbols')
    if not symbols_param:
        return jsonify({'error': 'symbols param required. e.g. ?symbols=NASDAQ:NVDA,NYSE:NOW'}), 400

    symbols = [s.strip() for s in symbols_param.split(',') if s.strip()]

    start = datetime.now(timezone.utc)
    try:
        data = fetch_tv_quotes(symbols, timeout=8)
        elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        return jsonify({
            'fetchedAt': start.isoformat(),
            'fetchTimeMs': elapsed_ms,
            'data': data,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/ping')
def ping():
    return 'ok', 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
