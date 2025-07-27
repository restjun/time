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

def analyze_symbols_with_detail(inst_ids):
    results = []
    for inst_id in inst_ids:
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=200)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=200)
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=200)
        df_15m = get_ohlcv_okx(inst_id, bar='15m', limit=200)
        
        if None in [df_1d, df_4h, df_1h, df_15m]:
            continue
        
        bullish_1d = is_ema_bullish(df_1d)
        bullish_4h = is_ema_bullish(df_4h)
        bullish_1h = is_ema_bullish(df_1h)
        bullish_15m = is_ema_bullish(df_15m)
        
        daily_change = calculate_daily_change(inst_id)
        
        results.append({
            "inst_id": inst_id,
            "daily_change": daily_change,
            "bullish_1d": bullish_1d,
            "bullish_4h": bullish_4h,
            "bullish_1h": bullish_1h,
            "bullish_15m": bullish_15m
        })
        time.sleep(random.uniform(0.2, 0.4))
    return results

def format_analysis_message(results):
    # 메인 타이틀
    message_lines = ["📡 선물 코인 분석을 시작합니다..."]
    
    # 상승률 내림차순 정렬
    results = sorted(results, key=lambda x: (x["daily_change"] if x["daily_change"] is not None else -9999), reverse=True)
    
    # BTC는 최상단 고정 처리 (있으면)
    btc = next((r for r in results if r["inst_id"].startswith("BTC-")), None)
    if btc:
        results.remove(btc)
        btc_change = f"(+{btc['daily_change']:.2f}%)" if btc['daily_change'] is not None else ""
        message_lines.append(f"💰 BTC: {btc['inst_id']} {btc_change}")
        message_lines.append(f"    └ 1D: {'✅️✅' if btc['bullish_1d'] else '🟥️🟥'}")
        message_lines.append(f"    └ 4h: {'✅️✅' if btc['bullish_4h'] else '🟥️🟥'}")
        message_lines.append(f"    └ 1h: {'✅️✅' if btc['bullish_1h'] else '🟥️🟥'}")
        message_lines.append(f"    └ 15m: {'✅️✅ 🚀🚀🚀' if btc['bullish_15m'] else '🟥️🟥'}")
        message_lines.append("───────────────────")
    
    # 나머지 코인 최대 10개 출력
    count = 1
    has_rocket = False
    for r in results[:10]:
        change_str = f"(+{r['daily_change']:.2f}%)" if r['daily_change'] is not None else "(N/A)"
        message_lines.append(f"📊 {count}. {r['inst_id']} {change_str}")
        message_lines.append(f"    └ 1D: {'✅️✅' if r['bullish_1d'] else '🟥️🟥'}")
        message_lines.append(f"    └ 4h: {'✅️✅' if r['bullish_4h'] else '🟥️🟥'}")
        message_lines.append(f"    └ 1h: {'✅️✅' if r['bullish_1h'] else '🟥️🟥'}")
        if r['bullish_15m']:
            message_lines.append(f"    └ 15m: ✅️✅ 🚀🚀🚀")
            has_rocket = True
        else:
            message_lines.append(f"    └ 15m: 🟥️🟥")
        message_lines.append("───────────────────")
        count += 1
    
    if not has_rocket:
        message_lines.append("🔴 현재 🚀 조건 만족 코인 없음.")
    message_lines.append("")
    message_lines.append("🧭 *매매 원칙*")
    message_lines.append("✅ 추격금지 / ✅ 비중조절 / ✅ 반익절")
    message_lines.append("  4h: ✅✅️  ")
    message_lines.append("  1h: ✅✅️   ")
    message_lines.append("15m:✅️✅️  ")
    message_lines.append("───────────────────")
    return "\n".join(message_lines)

def main():
    logging.info("📥 전체 종목 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    if not all_ids:
        send_telegram_message("⚠️ OKX 선물 코인 리스트를 불러올 수 없습니다.")
        return
    
    results = analyze_symbols_with_detail(all_ids)
    if not results:
        send_telegram_message("⚠️ 분석 가능한 코인 데이터가 없습니다.")
        return
    
    message = format_analysis_message(results)
    send_telegram_message(message)

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
