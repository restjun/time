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

    # 거래대금 순서로 정렬 후 limit 개수만 반환
    sorted_volume = dict(sorted(volume_dict.items(), key=lambda x: x[1], reverse=True)[:limit])
    return sorted_volume


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
        df = pd.DataFrame(response.json()['data'], columns=['ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
        df['c'] = df['c'].astype(float)
        df['o'] = df['o'].astype(float)
        df['ts'] = pd.to_datetime(df['ts'].astype(float), unit='ms')
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None


def calculate_daily_change_kst(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df = df.set_index('ts')
        df.index = df.index.tz_localize('UTC').tz_convert('Asia/Seoul')

        today_9am = pd.Timestamp.now(tz='Asia/Seoul').replace(hour=9, minute=0, second=0, microsecond=0)
        if today_9am not in df.index:
            nearest_time = df.index[df.index.get_indexer([today_9am], method='nearest')[0]]
        else:
            nearest_time = today_9am

        open_price = df.loc[nearest_time]['o']
        if isinstance(open_price, pd.Series):
            open_price = open_price.iloc[0]

        latest_close = df['c'].iloc[-1]
        change = ((latest_close - open_price) / open_price) * 100
        return round(change, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류 (KST 09시 기준): {e}")
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


def format_volume_to_million(volume):
    """핫크립토 스타일: 소수점 없이 정수만 m 단위로 변환하여 $291m 형식으로 표시"""
    return f"${int(volume / 1_000_000)}m"


def send_filtered_top_volume_message(spot_volume_dict, swap_symbols):
    filtered_dict = filter_swap_listed_coins(spot_volume_dict, swap_symbols)
    if not filtered_dict:
        send_telegram_message("🔴 선물 상장된 현물 거래량 상위 코인 없음.")
        return

    # 거래대금 내림차순 정렬 (이미 정렬됐을 가능성 있으나 다시 정렬 보장)
    filtered_sorted = dict(sorted(filtered_dict.items(), key=lambda x: x[1], reverse=True))

    message_lines = ["*OKX 현물 거래대금 기준 선물 상장 코인*", "━━━━━━━━━━━━━━━━━━━"]

    btc_id = "BTC-USDT-SWAP"
    if btc_id in filtered_sorted:
        btc_volume = filtered_sorted.pop(btc_id)
        btc_volume_formatted = format_volume_to_million(btc_volume)
        message_lines.append(f"1. BTC : {btc_volume_formatted}")

    for idx, (inst_id, vol) in enumerate(filtered_sorted.items(), start=2):
        vol_formatted = format_volume_to_million(vol)
        base_coin = inst_id.replace("-USDT-SWAP", "")
        message_lines.append(f"{idx}. {base_coin} : {vol_formatted}")

    message_lines.append("━━━━━━━━━━━━━━━━━━━")
    message = "\n".join(message_lines)

    send_telegram_message(message)


def job():
    logging.info("작업 시작")
    spot_volume = get_okx_spot_top_volume(limit=30)
    swap_symbols = get_okx_perpetual_symbols()
    send_filtered_top_volume_message(spot_volume, swap_symbols)
    logging.info("작업 완료")


def run_scheduler():
    schedule.every(60).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.get("/")
def read_root():
    return {"message": "OKX 거래대금 필터링 및 텔레그램 알림 API입니다."}


if __name__ == "__main__":
    threading.Thread(target=run_scheduler, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
