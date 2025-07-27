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

def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

def get_ohlcv_okx(instId, bar='1H', limit=200):
    logging.info(f"📊 {instId} - {bar} 캔들 데이터 요청 중...")
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'])
        df['c'] = df['c'].astype(float)
        df['o'] = df['o'].astype(float)
        df['vol'] = df['vol'].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

def is_ema_bullish(df):
    close = df['c'].values
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    if None in [ema_20, ema_50, ema_200]:
        return False
    return ema_20 > ema_50 > ema_200

def filter_by_1h_and_4h_ema_alignment(inst_ids):
    bullish_ids = []
    for inst_id in inst_ids:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=200)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=200)
        if df_1h is None or df_4h is None:
            continue
        if is_ema_bullish(df_1h) and is_ema_bullish(df_4h):
            bullish_ids.append(inst_id)
        time.sleep(random.uniform(0.2, 0.4))
    return bullish_ids

def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    df["quote_volume"] = df["c"] * df["vol"]
    return df["quote_volume"].sum()

def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=2)
    if df is None or len(df) < 2:
        return None
    try:
        open_price = df.iloc[-1]['o']
        close_price = df.iloc[-1]['c']
        change = ((close_price - open_price) / open_price) * 100
        return round(change, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def format_volume_in_eok(volume):
    try:
        eok = int(volume // 100_000_000)
        return f"{eok}억"
    except:
        return "N/A"

def get_ema_status_text(df, timeframe="15m"):
    close = df['c'].values
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)

    if None in [ema_20, ema_50, ema_200]:
        return f"[{timeframe}] EMA 상태: 계산불가"

    def arrow(a, b):
        return "🔼" if a > b else "🔽"

    return f"[{timeframe}] EMA 상태: {arrow(ema_20, ema_50)}20 {('>' if ema_20 > ema_50 else '<')} " \
           f"{arrow(ema_50, ema_200)}50 {('>' if ema_50 > ema_200 else '<')} " \
           f"{arrow(ema_20, ema_200)}200"

def send_ranked_volume_message(bullish_ids):
    volume_data = {}

    btc_id = "BTC-USDT-SWAP"
    btc_df_15m = get_ohlcv_okx(btc_id, bar="15m", limit=200)
    btc_ema_status = get_ema_status_text(btc_df_15m, timeframe="15m") if btc_df_15m is not None else "[15m] EMA 상태: 불러오기 실패"
    btc_change = calculate_daily_change(btc_id)
    btc_change_str = f"({btc_change:+.2f}%)" if btc_change is not None else "(N/A)"
    btc_volume = calculate_1h_volume(btc_id)
    btc_name = btc_id.replace("-USDT-SWAP", "")

    for inst_id in bullish_ids:
        vol = calculate_1h_volume(inst_id)
        volume_data[inst_id] = vol
        time.sleep(random.uniform(0.2, 0.4))

    sorted_data = sorted(volume_data.items(), key=lambda x: x[1], reverse=True)

    message_lines = ["📊 *1H + 4H 정배열 & 거래대금 랭킹*", "━━━━━━━━━━━━━━━━━━━"]

    message_lines.append(f"📌 *{btc_name}* {btc_change_str}")
    message_lines.append(f"   {btc_ema_status}")
    message_lines.append("━━━━━━━━━━━━━━━━━━━")

    for rank, (inst_id, vol) in enumerate(sorted_data[:10], start=1):
        change = calculate_daily_change(inst_id)
        change_str = f"({change:+.2f}%)" if change is not None else "(N/A)"

        df_15m = get_ohlcv_okx(inst_id, bar="15m", limit=200)
        ema_status = get_ema_status_text(df_15m, timeframe="15m") if df_15m is not None else "[15m] EMA 상태: 불러오기 실패"

        name = inst_id.replace("-USDT-SWAP", "")
        volume_text = format_volume_in_eok(vol)

        message_lines.append(f"{rank}. {name} {change_str} - 거래대금: {volume_text}")
        message_lines.append(f"   {ema_status}")

    message_lines.append("━━━━━━━━━━━━━━━━━━━")
    send_telegram_message("\n".join(message_lines))

def main():
    logging.info("📥 전체 종목 기준 정배열 + 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    bullish_ids = filter_by_1h_and_4h_ema_alignment(all_ids)
    if not bullish_ids:
        send_telegram_message("🔴 1H + 4H 정배열 종목 없음.")
        return
    send_ranked_volume_message(bullish_ids)

@app.on_event("startup")
def start_scheduler():
    schedule.every(3).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
