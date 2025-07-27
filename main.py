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

def check_ema_alignment(df):
    if df is None or len(df) < 200:
        return None
    close = df['c'].values
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    if None in [ema_20, ema_50, ema_200]:
        return None
    return ema_20 > ema_50 > ema_200

def calculate_1d_change(inst_id):
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

def main():
    logging.info("📡 선물 코인 분석을 시작합니다...")
    all_ids = get_all_okx_swap_symbols()

    results = []
    for inst_id in all_ids:
        # 각 타임프레임 데이터 호출
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=200)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=200)
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=200)
        df_15m = get_ohlcv_okx(inst_id, bar='15m', limit=200)

        # EMA 정배열 여부 체크, 계산 불가시 None
        align_1d = check_ema_alignment(df_1d)
        align_4h = check_ema_alignment(df_4h)
        align_1h = check_ema_alignment(df_1h)
        align_15m = check_ema_alignment(df_15m)

        change = calculate_1d_change(inst_id)

        results.append({
            "inst_id": inst_id,
            "change": change,
            "1d": align_1d,
            "4h": align_4h,
            "1h": align_1h,
            "15m": align_15m
        })
        time.sleep(random.uniform(0.3, 0.6))

    # 🚀 조건: 1D, 4H, 1H 모두 정배열(즉 True)
    rocket_coins = [r for r in results if  r["4h"] and r["1h"]]

    # 메시지 생성
    message_lines = ["📡 선물 코인 분석을 시작합니다..."]
    if rocket_coins:
        for i, coin in enumerate(rocket_coins[:10], start=1):
            def emoji_status(val):
                if val is None:
                    return "❌"
                return "✅️" if val else "🟥"

            msg = f"💰 {coin['inst_id']} (+{coin['change'] if coin['change'] is not None else 'N/A'}%)\n" \
                  f"    └ 1D: {emoji_status(coin['1d'])}\n" \
                  f"    └ 4h: {emoji_status(coin['4h'])}\n" \
                  f"    └ 1h: {emoji_status(coin['1h'])}\n" \
                  f"    └ 15m: {emoji_status(coin['15m'])} 🚀🚀🚀\n" \
                  "───────────────────"
            message_lines.append(msg)
    else:
        message_lines.append("🔴 현재 🚀 조건 만족 코인 없음.")

    message_lines.append("\n🧭 *매매 원칙*")
    message_lines.append("✅ 추격금지 / ✅ 비중조절 / ✅ 반익절 ")
    message_lines.append("  4h: ✅✅️  ")
    message_lines.append("  1h: ✅✅️   ")
    message_lines.append("15m:✅️✅️  ")
    message_lines.append("───────────────────")

    send_telegram_message("\n".join(message_lines))

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
