from fastapi import FastAPI
import pyupbit
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
from datetime import datetime
import pytz
import numpy as np

app = FastAPI()

telegram_bot_token = "6389499820:AAFrQ5rwLUD98CFiPJjoVOdSMoEFDcHNMHk"
telegram_user_id = 6596886700

bot = telepot.Bot(telegram_bot_token)

access = "1JiZBNdcwGp5RZF7WwNPs3esjEF0v7aXlxoKjOkU"
secret = "vpCpZu3xs5s2pyVOZgMcxlnMtzP0bZgNEAsUjuAz"
upbit = pyupbit.Upbit(access, secret)

krw_tickers = pyupbit.get_tickers(fiat="KRW")

previous_sent_coins = []
previous_trade_prices = {}

logging.basicConfig(level=logging.DEBUG)

def send_telegram_message(message):
    max_retries = 3
    retry_delay = 5
    for retry_count in range(max_retries):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message)
            logging.info("텔레그램 메시지 전송 성공: %s", message)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/%d): %s", retry_count + 1, max_retries, str(e))
            time.sleep(retry_delay)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

def calculate_vwma(close_prices, volumes, period):
    if len(close_prices) < period:
        return None
    weighted_price = close_prices[-period:] * volumes[-period:]
    total_volume = volumes[-period:].sum()
    if total_volume == 0:
        return None
    return weighted_price.sum() / total_volume

def check_vwma_relations(close_prices, volumes):
    vwma_5 = calculate_vwma(close_prices, volumes, 5)
    vwma_20 = calculate_vwma(close_prices, volumes, 20)
    vwma_50 = calculate_vwma(close_prices, volumes, 50)
    vwma_200 = calculate_vwma(close_prices, volumes, 200)

    if None in (vwma_5, vwma_20, vwma_50, vwma_200):
        return None

    return {
        '5_20': vwma_5 > vwma_20,
        '20_50': vwma_20 > vwma_50,
        '50_200': vwma_50 > vwma_200
    }

def find_golden_cross_coins(tickers, interval, count):
    golden_cross_coins = []
    vwma_states = dict()
    for ticker in tickers:
        df = retry_request(pyupbit.get_ohlcv, ticker, interval=interval, count=count)
        if df is not None and len(df) >= 200:
            close_prices = df['close'].values
            volumes = df['volume'].values
            vwma_relation = check_vwma_relations(close_prices, volumes)
            if vwma_relation and all(vwma_relation.values()):
                golden_cross_coins.append(ticker)
                vwma_states[ticker] = vwma_relation
    return golden_cross_coins, vwma_states

def calculate_trade_price(coins):
    url = "https://api.upbit.com/v1/candles/minutes/10"
    total_trade_price = dict()
    kr_tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(kr_tz)

    if now.hour >= 9:
        total_trade_price = dict()

    for coin in coins:
        querystring = {"market": coin, "count": 144}
        response = retry_request(requests.get, url, params=querystring)
        if response is None:
            continue
        data = response.json()
        try:
            trade_volume = sum([candle['candle_acc_trade_volume'] for candle in data])
            trade_price = float(data[0]["trade_price"]) * trade_volume
            trade_price_billion = trade_price / 100000000
            if trade_price_billion >= 100000:
                total_trade_price[coin] = round(trade_price_billion / 100000)
            else:
                total_trade_price[coin] = round(trade_price_billion)
            time.sleep(0.2)
        except Exception as e:
            logging.error("Error processing data for coin: %s", coin)
            logging.error(str(e))
    time.sleep(0.1)
    return dict(sorted(total_trade_price.items(), key=lambda x: x[1], reverse=True)[:10])

def calculate_price_change_percentage(coin):
    time.sleep(0.2)
    ohlcv_data = retry_request(pyupbit.get_ohlcv, coin, interval="day", count=2)
    if ohlcv_data is not None and len(ohlcv_data) >= 2:
        current_close_price = ohlcv_data['close'][-1]
        previous_close_price = ohlcv_data['close'][-2]
        try:
            if previous_close_price != 0:
                change_percentage = ((current_close_price - previous_close_price) / previous_close_price) * 100
                return change_percentage
            else:
                logging.error("이전 종가가 0입니다: %s", coin)
                return None
        except Exception as e:
            logging.error("가격 변동률 계산 에러 (%s): %s", coin, str(e))
            return None
    else:
        logging.error("캔들 데이터 부족으로 가격 변동률 계산 실패: %s", coin)
        return None

def send_golden_cross_message(golden_cross_coins, vwma_states):
    golden_trade_price_result = calculate_trade_price(golden_cross_coins)
    golden_trade_price_result = {coin: trade_price for coin, trade_price in golden_trade_price_result.items() if trade_price >= 300}

    if not golden_trade_price_result:
        message = "🔴 현재 300억 이상의 거래대금을 가진 코인이 없습니다.\n\n업비트 상태 확인 완료."
        send_telegram_message(message)
        return

    new_golden_coins = [coin for coin in golden_cross_coins if coin not in previous_sent_coins]

    message_lines = []
    message_lines.append("----------------------------------")
    message_lines.append("🟩 5-20 / 20-50 / 50-200 정배열 (VWMA)")
    message_lines.append("----------------------------------")

    for idx, (coin, trade_price) in enumerate(sorted(golden_trade_price_result.items(), key=lambda x: x[1], reverse=True)[:10], start=1):
        price_change_percentage = calculate_price_change_percentage(coin)
        if price_change_percentage is not None and price_change_percentage > -10:
            is_new_coin = coin in new_golden_coins

            vwma = vwma_states.get(coin, {'5_20': False, '20_50': False, '50_200': False})

            vwma_5_20 = "✅" if vwma['5_20'] else "❌"
            vwma_20_50 = "✅" if vwma['20_50'] else "❌"
            vwma_50_200 = "✅" if vwma['50_200'] else "❌"

            message_lines.append(
                f"{idx}. 🟩 {coin.replace('KRW-', '')}: {trade_price}억 ({price_change_percentage:+.2f}%) {'🚀' if is_new_coin else ''}\n"
                f"[VWMA] 5>20{vwma_5_20} 20>50{vwma_20_50} 50>200{vwma_50_200}"
            )

    message_lines.append("----------------------------------")

    full_message = "\n".join(message_lines)
    send_telegram_message(full_message)

    previous_sent_coins.extend(new_golden_coins)

def retry_request(func, *args, **kwargs):
    max_retries = 3
    delay = 2
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.warning("요청 실패, 재시도 중... (%d/%d) 에러: %s", i + 1, max_retries, str(e))
            time.sleep(delay)
    logging.error("요청 실패: 최대 재시도 횟수 초과")
    return None

def main_job():
    try:
        golden_cross_coins, vwma_states = find_golden_cross_coins(krw_tickers, "minute10", 200)
        send_golden_cross_message(golden_cross_coins, vwma_states)
    except Exception as e:
        logging.error("메인 작업 중 오류 발생: %s", str(e))

schedule.every(10).minutes.do(main_job)

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
async def startup_event():
    thread = threading.Thread(target=run_schedule, daemon=True)
    thread.start()

@app.get("/")
def read_root():
    return {"status": "running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
