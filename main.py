from fastapi import FastAPI
import pyupbit
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd

app = FastAPI()

telegram_bot_token = "8170040373:AAFaEM789kB8aemN69BWwSjZ74HEVOQXP5s"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

access = "QBJxf9YKWDotc63BFbBg2lkwZ9FHpgoBu3vzjeoS"
secret = "MZqMcGFaZkj7CarqgtIxyoxDcX1xUDB80BAljbWk"
upbit = pyupbit.Upbit(access, secret)

logging.basicConfig(level=logging.INFO)

def send_telegram_message(message):
    max_retries = 10
    retry_delay = 5
    for retry_count in range(1, max_retries + 1):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공: %s", message)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/%d): %s", retry_count, max_retries, str(e))
            time.sleep(retry_delay)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

def retry_request(func, *args, **kwargs):
    max_retries = 10
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                logging.warning("⚠️ 429 Too Many Requests - 대기 후 재시도")
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/{max_retries}): {str(e)}")
            time.sleep(retry_delay)
    return None

def calculate_ema(close, period):
    if len(close) < period:
        return None
    close_series = pd.Series(close)
    ema = close_series.ewm(span=period, adjust=False).mean().iloc[-1]
    return ema

def get_ema_with_retry(close, period):
    for _ in range(5):
        result = calculate_ema(close, period)
        if result is not None:
            return result
        time.sleep(0.5)
    return None

def get_okx_perpetual_symbols():
    try:
        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        response = retry_request(requests.get, url)
        if response is None:
            return []
        data = response.json()
        usdt_pairs = [
            item['instId'].replace("-USDT-SWAP", "") 
            for item in data.get('data', []) 
            if item.get('instId', "").endswith("-USDT-SWAP")
        ]
        return usdt_pairs
    except Exception as e:
        logging.error("OKX 선물 코인 조회 실패: %s", str(e))
        return []

def get_common_upbit_okx_tickers():
    okx_symbols = get_okx_perpetual_symbols()
    upbit_krw_tickers = pyupbit.get_tickers(fiat="KRW")
    matched = []
    for ticker in upbit_krw_tickers:
        symbol = ticker.replace("KRW-", "")
        if symbol in okx_symbols:
            matched.append(ticker)
    return matched

def calculate_trade_price(coins):
    url = "https://api.upbit.com/v1/candles/minutes/10"
    total_trade_price = {}
    for coin in coins:
        querystring = {"market": coin, "count": 145}
        response = retry_request(requests.get, url, params=querystring)
        if response is None:
            continue
        try:
            data = response.json()
            if not data:
                continue
            trade_volume = sum([candle.get('candle_acc_trade_volume', 0) for candle in data])
            current_price = data[0].get("trade_price", None)
            if current_price is None:
                continue
            trade_price = float(current_price) * trade_volume
            trade_price_billion = trade_price / 100000000
            total_trade_price[coin] = round(trade_price_billion)
        except Exception as e:
            logging.error("거래대금 계산 실패 (%s): %s", coin, str(e))
        time.sleep(0.1)
    return dict(sorted(total_trade_price.items(), key=lambda x: x[1], reverse=True)[:30])

def calculate_price_change_percentage(coin):
    for _ in range(10):
        try:
            ohlcv_data = pyupbit.get_ohlcv(coin, interval="day", count=2)
            if ohlcv_data is not None and len(ohlcv_data) >= 2:
                current_close_price = ohlcv_data['close'][-1]
                previous_close_price = ohlcv_data['close'][-2]
                if previous_close_price != 0:
                    return ((current_close_price - previous_close_price) / previous_close_price) * 100
        except Exception as e:
            logging.error("가격 변동률 계산 에러 (%s): %s", coin, str(e))
        time.sleep(1)
    return None

def get_ohlcv_with_retry(coin, interval, count):
    for _ in range(10):
        try:
            df = pyupbit.get_ohlcv(coin, interval=interval, count=count)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logging.error("OHLCV 데이터 요청 실패 (%s): %s", coin, str(e))
        time.sleep(1)
    return None

def format_trade_price_billion(trade_price_billion):
    if trade_price_billion >= 10000:
        trillion = trade_price_billion // 10000
        billion = trade_price_billion % 10000
        return f"{trillion}조 {billion}억" if billion > 0 else f"{trillion}조"
    return f"{trade_price_billion}억"

# ---------------------------- EMA 상태 조회 ----------------------------
def get_ema_status(coin):
    tf_results = []
    tf_data = {}

    timeframes = {
        "1D":  "day",
        "4h":  "minute240",
        "1h":  "minute60",
        "15m": "minute15"
    }

    for tf_label, tf_api in timeframes.items():
        df = get_ohlcv_with_retry(coin, interval=tf_api, count=200)
        if df is None:
            tf_results.append(f"{tf_label}: ❌")
            tf_data[tf_label] = None
            continue

        close = df['close'].values

        ema_5 = get_ema_with_retry(close, 5)
        ema_20 = get_ema_with_retry(close, 20)
        ema_50 = get_ema_with_retry(close, 50)
        ema_200 = get_ema_with_retry(close, 200)

        if None in [ema_5, ema_20, ema_50, ema_200]:
            tf_results.append(f"{tf_label}: ❌")
            tf_data[tf_label] = None
            continue

        tf_data[tf_label] = {
            "ema_5": ema_5,
            "ema_20": ema_20,
            "ema_50": ema_50,
            "ema_200": ema_200
        }

    for tf_label in timeframes:
        emas = tf_data.get(tf_label)
        if not emas:
            continue

        ema_20 = emas["ema_20"]
        ema_50 = emas["ema_50"]
        ema_200 = emas["ema_200"]

        t50 = "✅️" if ema_20 > ema_50 else "🟥"
        f200 = "✅" if ema_50 > ema_200 else "🟥"
        rocket = ""

        # 🚀 조건: 1h, 4h 정배열일 때만 로켓
        if tf_label == "15m":
            emas_1h = tf_data.get("1h")
            emas_4h = tf_data.get("4h")

            cond_1h = emas_1h and emas_1h["ema_20"] > emas_1h["ema_50"] > emas_1h["ema_200"]
            cond_4h = emas_4h and emas_4h["ema_20"] > emas_4h["ema_50"] > emas_4h["ema_200"]

            if cond_1h and cond_4h:
                rocket = " 🚀🚀🚀"

        tf_results.append(f"{tf_label}: {t50}{f200}{rocket}")

    return tf_results
# ----------------------------------------------------------------------

def send_filtered_top_volume_message(top_volume_coins):
    if not top_volume_coins:
        send_telegram_message("🔴 현재 1000억 이상의 거래대금을 가진 코인이 없습니다.\n\n업비트 상태 확인 완료.")
        return

    message_lines = []
    message_lines.append("*업비트 거래대금 1위 + 비트*")
    message_lines.append("━━━━━━━━━━━━━━━━━━━")

    btc_ticker = "KRW-BTC"
    btc_trade_price = top_volume_coins.get(btc_ticker, None)
    btc_price_change = calculate_price_change_percentage(btc_ticker)

    if btc_trade_price is not None and btc_price_change is not None:
        message_lines.append(f"📊 BTC | 💰 {format_trade_price_billion(btc_trade_price)} | 📈 {btc_price_change:+.2f}%")
        for tf_result in get_ema_status(btc_ticker):
            message_lines.append(f"    └ {tf_result}")
        message_lines.append("───────────────────")

    filtered_items = [(coin, price) for coin, price in sorted(top_volume_coins.items(), key=lambda x: x[1], reverse=True)
                      if coin != btc_ticker]

    idx = 1
    rocket_found = False

    for coin, trade_price in filtered_items:
        price_change = calculate_price_change_percentage(coin)
        if price_change is None or price_change <= -100:
            continue

        tf_results = get_ema_status(coin)

        if any("🚀" in line for line in tf_results):  # 🚀 조건 필터링
            rocket_found = True
            message_lines.append(f"📊 {idx}. {coin.replace('KRW-', '')} | 💰 {format_trade_price_billion(trade_price)} | 📈 {price_change:+.2f}%")
            for tf_result in tf_results:
                message_lines.append(f"    └ {tf_result}")
            message_lines.append("───────────────────")
            idx += 1
            if idx > 5:
                break

    if not rocket_found:
        send_telegram_message("🔴 현재 🚀 조건을 만족하는 코인이 없습니다.\n🔴 업비트 상태 확인 완료.")
        return

    message_lines.append("🧭 *매매 원칙*")
    message_lines.append("✅ 추격금지 / ✅ 비중조절 / ✅ 반익절 \n  4h: ✅✅️  \n  1h: ✅✅️   \n15m:✅️✅️  \n───────────────────\n📈 1000:1 정배열만하자  ")
    message_lines.append("━━━━━━━━━━━━━━━━━━━━") 
    final_message = "\n".join(message_lines)
    send_telegram_message(final_message)

def main():
    filtered_tickers = get_common_upbit_okx_tickers()
    top_volume_coins = calculate_trade_price(filtered_tickers)
    filtered_coins = {coin: volume for coin, volume in top_volume_coins.items() if volume >= 1}
    send_filtered_top_volume_message(filtered_coins)

@app.on_event("startup")
def start_scheduler():
    schedule.every(1).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
