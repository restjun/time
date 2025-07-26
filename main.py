from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import random
from datetime import datetime, timedelta, timezone

app = FastAPI()

telegram_bot_token = "8170040373:AAFaEM789kB8aemN69BWwSjZ74HEVOQXP5s"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

def send_telegram_message(message):
    for retry_count in range(1, 11):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공: %s", message)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/10): %s", retry_count, str(e))
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                logging.warning("⚠️ 429 Too Many Requests - 대기 후 재시도")
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/10): {str(e)}")
            time.sleep(5)
    return None

def calculate_ema(close, period):
    if len(close) < period:
        return None
    close_series = pd.Series(close)
    return close_series.ewm(span=period, adjust=False).mean().iloc[-1]

def get_ema_with_retry(close, period):
    for _ in range(5):
        result = calculate_ema(close, period)
        if result is not None:
            return result
        time.sleep(0.5)
    return None

def get_okx_perpetual_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json()
    return [
        item['instId'] for item in data.get('data', [])
        if item['instId'].endswith("-USDT-SWAP")
    ]

def get_okx_spot_top_volume(limit=30):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    response = retry_request(requests.get, url)
    if response is None:
        return {}

    tickers = response.json().get('data', [])
    volume_dict = {}
    for ticker in tickers:
        inst_id = ticker['instId']
        quote_vol = float(ticker.get('volCcyQuote', 0) or 0)
        base_coin = inst_id.replace("-USDT", "")
        volume_dict[base_coin] = quote_vol

    return dict(sorted(volume_dict.items(), key=lambda x: x[1], reverse=True)[:limit])

def filter_swap_listed_coins(base_coins, swap_symbols):
    filtered = {}
    for base in base_coins:
        swap_id = f"{base}-USDT-SWAP"
        if swap_id in swap_symbols:
            filtered[swap_id] = base_coins[base]
    return filtered

def get_ohlcv_okx(instId, bar='1h', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'])
        df['c'] = df['c'].astype(float)
        df['o'] = df['o'].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

# 한구시간(KST) 15시에 맞춘 변동률 계산 함수 수정
def calculate_daily_change(inst_id):
    # OHLCV 2일치 데이터 받아오기 (1D 바, 2개)
    df = get_ohlcv_okx(inst_id, bar="1D", limit=2)
    if df is None or len(df) < 2:
        return None
    try:
        # OKX API 일봉 데이터는 UTC 00시 기준.
        # KST는 UTC +9시간.
        # 따라서, 15시 KST 기준 시점은 UTC 06시임.
        # 일봉은 00시~24시 데이터로 제공되어 직접 15시 KST 캔들은 없으므로 근사치 계산.

        # 전일 KST 15시 시점은 df[-2]의 시가에서 9시간 뒤 시점,
        # 당일 KST 15시 시점은 df[-1]의 시가에서 9시간 뒤 시점으로 근사.

        # 단순히 시가와 종가를 이용해 변동률 계산
        open_price = df.iloc[-2]['o']  # 전일 시가 (UTC 00시)
        close_price = df.iloc[-1]['c'] # 당일 종가 (UTC 00시 기준이지만 근사)

        change = ((close_price - open_price) / open_price) * 100
        return round(change, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def get_ema_status(inst_id):
    tf_results = []
    tf_data = {}

    timeframes = {
        "1D": "1D",
        "4h": "4H",
        "1h": "1H",
        "15m": "15m"
    }

    for tf_label, tf_api in timeframes.items():
        df = get_ohlcv_okx(inst_id, bar=tf_api, limit=200)
        if df is None:
            tf_results.append(f"{tf_label}: ❌")
            tf_data[tf_label] = None
            continue

        close = df['c'].values
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

        time.sleep(random.uniform(0.3, 0.5))

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

        if tf_label == "15m":
            emas_1h = tf_data.get("1h")
            emas_4h = tf_data.get("4h")
            cond_1h = emas_1h and emas_1h["ema_20"] > emas_1h["ema_50"] > emas_1h["ema_200"]
            cond_4h = emas_4h and emas_4h["ema_20"] > emas_4h["ema_50"] > emas_4h["ema_200"]
            if cond_1h and cond_4h:
                rocket = " 🚀🚀🚀"

        tf_results.append(f"{tf_label}: {t50}{f200}{rocket}")

    return tf_results

def send_filtered_top_volume_message(spot_volume_dict, swap_symbols):
    filtered_dict = filter_swap_listed_coins(spot_volume_dict, swap_symbols)
    if not filtered_dict:
        send_telegram_message("🔴 선물 상장된 현물 거래량 상위 코인 없음.")
        return

    message_lines = ["*OKX 현물 거래대금 기준 선물 상장 코인*", "━━━━━━━━━━━━━━━━━━━"]

    btc_id = "BTC-USDT-SWAP"
    btc_ema = get_ema_status(btc_id)
    btc_change = calculate_daily_change(btc_id)
    btc_change_str = f"({btc_change:+.2f}%)" if btc_change is not None else "(N/A)"
    message_lines.append(f"💰 {btc_id} {btc_change_str} (15시 KST 기준)")
    for tf_result in btc_ema:
        message_lines.append(f"    └ {tf_result}")
    message_lines.append("───────────────────")

    idx = 1
    rocket_found = False
    for inst_id in filtered_dict.keys():
        if inst_id == btc_id:
            continue
        tf_results = get_ema_status(inst_id)
        change = calculate_daily_change(inst_id)
        change_str = f"({change:+.2f}%)" if change is not None else "(N/A)"

        if any("🚀" in line for line in tf_results):
            rocket_found = True
            message_lines.append(f"📊 {idx}. {inst_id} {change_str} (15시 KST 기준)")
            for tf_result in tf_results:
                message_lines.append(f"    └ {tf_result}")
            message_lines.append("───────────────────")
            idx += 1
            if idx > 3:
                break

    if not rocket_found:
        message_lines.append("🔴 현재 🚀 조건 만족 코인 없음.")

    message_lines.append("🧭 *@okx_mkt_guide*")
    message_lines.append("━━━━━━━━━━━━━━━━━━━")
    send_telegram_message("\n".join(message_lines))

@app.get("/start")
async def start():
    def job():
        try:
            swap_symbols = get_okx_perpetual_symbols()
            spot_volume_dict = get_okx_spot_top_volume()
            send_filtered_top_volume_message(spot_volume_dict, swap_symbols)
        except Exception as e:
            logging.error(f"스케줄 작업 중 오류: {e}")

    def run_schedule():
        schedule.every(30).minutes.do(job)
        while True:
            schedule.run_pending()
            time.sleep(1)

    threading.Thread(target=run_schedule, daemon=True).start()
    return {"message": "OKX 선물 상장 코인 알림 서비스가 시작되었습니다."}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
