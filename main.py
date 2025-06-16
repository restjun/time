from fastapi import FastAPI
import pyupbit
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
from datetime import datetime, timedelta
import pandas as pd
import pytz

app = FastAPI()

# 텔레그램 봇 토큰과 사용자 ID 설정
telegram_bot_token = "8170040373:AAFaEM789kB8aemN69BWwSjZ74HEVOQXP5s"
telegram_user_id = 6596886700

bot = telepot.Bot(telegram_bot_token)

# 업비트 로그인 계정2 2026.05.31 만료
access = "QBJxf9YKWDotc63BFbBg2lkwZ9FHpgoBu3vzjeoS"
secret = "MZqMcGFaZkj7CarqgtIxyoxDcX1xUDB80BAljbWk"
upbit = pyupbit.Upbit(access, secret)

# KRW로 거래되는 모든 코인 조회
krw_tickers = pyupbit.get_tickers(fiat="KRW")


# 이전에 발송한 코인 목록 및 거래대금 초기화
previous_sent_coins = []
previous_trade_prices = {}

# 로깅 설정
logging.basicConfig(level=logging.DEBUG)

import time
import logging
import requests
from datetime import datetime
import pytz
import pyupbit

# 텔레그램 메시지 전송 함수
def send_telegram_message(message, btc_status_1h, btc_status_4h, is_new_coin=False, btc_price_change_percentage=0.0):
    max_retries = 3
    retry_delay = 5

    for retry_count in range(1, max_retries + 1):
        try:
            message_with_status = f"{message}\n(BTC-[일봉]){' 🟩 [양봉정렬] 🟩🟩🟩🟩 ' if btc_status_1h else ' 🟥 '}\n(BTC-[분봉]){' 🟩 [캔들] 20개이상 추세유지 🔁' if btc_status_4h else ' 🟥 '}"
            if is_new_coin:
                message_with_status += ""
            bot.sendMessage(chat_id=telegram_user_id, text=message_with_status)
            logging.info("텔레그램 메시지 전송 성공: %s", message_with_status)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/%d): %s", retry_count, max_retries, str(e))
            time.sleep(retry_delay)

    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")


# VWMA 계산
def calculate_vwma(data, volume, period):
    if len(data) < period:
        return None
    weighted_price = data[-period:] * volume[-period:]
    total_volume = volume[-period:].sum()
    return weighted_price.sum() / total_volume if total_volume != 0 else None


# 연속 정배열/역배열 지속 횟수 계산
def count_consecutive_vwma_condition(df, short_period, long_period):
    closes = df['close'].values
    volumes = df['volume'].values
    count = 0
    for i in range(len(closes) - 1, long_period - 2, -1):
        short_vwma = calculate_vwma(closes[:i+1], volumes[:i+1], short_period)
        long_vwma = calculate_vwma(closes[:i+1], volumes[:i+1], long_period)
        if short_vwma is None or long_vwma is None:
            break
        if short_vwma > long_vwma:
            count += 1
        else:
            break
    return count


# 비트코인 상태 확인
def check_bitcoin_status():
    btc_ticker = "KRW-BTC"
    btc_df = retry_request(pyupbit.get_ohlcv, btc_ticker, interval="minute1440", count=200)
    if btc_df is not None and len(btc_df) >= 200:
        btc_vwma_1 = calculate_vwma(btc_df['close'].values, btc_df['volume'].values, 1)
        btc_vwma_2 = calculate_vwma(btc_df['close'].values, btc_df['volume'].values, 2)
        btc_status_1h = 1 if btc_vwma_1 and btc_vwma_2 and btc_vwma_1 > btc_vwma_2 else 0

        btc_df_4h = retry_request(pyupbit.get_ohlcv, btc_ticker, interval="minute60", count=200)
        if btc_df_4h is not None and len(btc_df_4h) >= 200:
            btc_vwma_1_4h = calculate_vwma(btc_df_4h['close'].values, btc_df_4h['volume'].values, 20)
            btc_vwma_2_4h = calculate_vwma(btc_df_4h['close'].values, btc_df_4h['volume'].values, 60)
            btc_status_4h = 1 if btc_vwma_1_4h and btc_vwma_2_4h and btc_vwma_1_4h > btc_vwma_2_4h else 0
        else:
            logging.error("비트코인 4시간 데이터를 불러올 수 없습니다.")
            btc_status_4h = None

        return btc_status_1h, btc_status_4h
    else:
        logging.error("비트코인 데이터를 불러올 수 없습니다.")
        return None, None


# 정배열 돌파 종목 찾기
def find_golden_cross_coins(tickers, interval, count):
    golden_cross_coins = []
    for ticker in tickers:
        df = retry_request(pyupbit.get_ohlcv, ticker, interval=interval, count=count)
        if df is not None and len(df) >= 2:
            vwma_1 = calculate_vwma(df['close'].values, df['volume'].values, 1)
            vwma_2 = calculate_vwma(df['close'].values, df['volume'].values, 2)
            if vwma_1 and vwma_2 and vwma_1 > vwma_2:
                golden_cross_coins.append(ticker)
    return golden_cross_coins


# 가격 변동률 계산
def calculate_price_change_percentage(coin):
    time.sleep(0.2)
    ohlcv_data = retry_request(pyupbit.get_ohlcv, coin, interval="day", count=2)
    if ohlcv_data is not None and len(ohlcv_data) >= 2:
        current = ohlcv_data['close'][-1]
        previous = ohlcv_data['close'][-2]
        try:
            return ((current - previous) / previous) * 100 if previous != 0 else None
        except Exception as e:
            logging.error("가격 변동률 계산 에러 (%s): %s", coin, str(e))
            return None
    return None


# 거래대금 계산
def calculate_trade_price(coins):
    url = "https://api.upbit.com/v1/candles/minutes/10"
    total_trade_price = {}
    for coin in coins:
        querystring = {"market": coin, "count": 144}
        response = retry_request(requests.get, url, params=querystring)
        data = response.json()
        try:
            trade_volume = sum(c["candle_acc_trade_volume"] for c in data)
            trade_price = float(data[0]["trade_price"]) * trade_volume
            total_trade_price[coin] = round(trade_price / 100000000)
            time.sleep(0.2)
        except Exception as e:
            logging.error("Error processing data for coin: %s", coin)
            logging.error(str(e))
    return dict(sorted(total_trade_price.items(), key=lambda x: x[1], reverse=True)[:10])


# 정배열 메시지 전송

import pyupbit

def calculate_vwma(closes, volumes, window):
    if len(closes) < window or len(volumes) < window:
        return None
    closes = closes[-window:]
    volumes = volumes[-window:]
    return (closes * volumes).sum() / volumes.sum()

def count_consecutive_vwma_condition(df, short_window, long_window):
    count = 0
    closes = df['close'].values
    volumes = df['volume'].values

    for i in range(len(df) - long_window):
        vwma_short = calculate_vwma(closes[i:i+short_window], volumes[i:i+short_window], short_window)
        vwma_long = calculate_vwma(closes[i:i+long_window], volumes[i:i+long_window], long_window)

        if vwma_short > vwma_long:
            count += 1
        else:
            count = 0  # 리셋
    return count

def count_consecutive_reverse_vwma_condition(df, short_window, long_window):
    count = 0
    closes = df['close'].values
    volumes = df['volume'].values

    for i in range(len(df) - long_window):
        vwma_short = calculate_vwma(closes[i:i+short_window], volumes[i:i+short_window], short_window)
        vwma_long = calculate_vwma(closes[i:i+long_window], volumes[i:i+long_window], long_window)

        if vwma_short < vwma_long:
            count += 1
        else:
            count = 0  # 리셋
    return count

def send_golden_cross_message(golden_cross_coins, btc_status_1h, btc_status_4h, btc_price_change_percentage):
    golden_trade_price_result = calculate_trade_price(golden_cross_coins)
    golden_trade_price_result = {coin: val for coin, val in golden_trade_price_result.items() if val >= 300}

    if not golden_trade_price_result:
        send_telegram_message("🔴 현재 300억 이상의 거래대금을 가진 코인이 없습니다.\n\n업비트 상태 확인 완료.", btc_status_1h, btc_status_4h)
        return

    message_lines = ["LONG-----------------------------"]

    for idx, (coin, trade_price) in enumerate(golden_trade_price_result.items(), start=1):
        price_change = calculate_price_change_percentage(coin)
        price_change_str = f"{price_change:+.2f}%" if price_change is not None else "N/A"

        df = retry_request(pyupbit.get_ohlcv, coin, interval="minute60", count=200)
        if df is None or len(df) < 120:
            continue

        vwma_5 = calculate_vwma(df['close'].values, df['volume'].values, 5)
        vwma_20 = calculate_vwma(df['close'].values, df['volume'].values, 20)
        vwma_60 = calculate_vwma(df['close'].values, df['volume'].values, 60)
        vwma_120 = calculate_vwma(df['close'].values, df['volume'].values, 120)
 
        cnt_5_20 = count_consecutive_vwma_condition(df, 5, 20)
        cnt_5_20_reverse = count_consecutive_reverse_vwma_condition(df, 5, 20)
        cnt_20_60 = count_consecutive_vwma_condition(df, 20, 60)
        cnt_20_60_reverse = count_consecutive_reverse_vwma_condition(df, 20, 60)        
        cnt_60_120 = count_consecutive_vwma_condition(df, 60, 120)
        cnt_60_120_reverse = count_consecutive_reverse_vwma_condition(df, 60, 120)
        
        # VWMA 역배열 지속 횟수 계산 함수
def count_consecutive_reverse_vwma_condition(df, short_period, long_period):
    closes = df['close'].values
    volumes = df['volume'].values
    count = 0
    for i in range(len(closes) - 1, long_period - 2, -1):
        short_vwma = calculate_vwma(closes[:i+1], volumes[:i+1], short_period)
        long_vwma = calculate_vwma(closes[:i+1], volumes[:i+1], long_period)
        if short_vwma is None or long_vwma is None:
            break
        if short_vwma < long_vwma:
            count += 1
        else:
            break
    return count

        five_twenty = f"🟩({str(cnt_5_20).zfill(2)})" if vwma_5 and vwma_20 and vwma_5 > vwma_20 else f"🅾️({str(cnt_5_20_reverse).zfill(2)})"
        twenty_sixty = f"✅️({str(cnt_20_60).zfill(2)})" if vwma_20 and vwma_60 and vwma_20 > vwma_60 else f"🟥({str(cnt_20_60_reverse).zfill(2)})"
        sixty_hundredtwenty = f"🟩({str(cnt_60_120).zfill(2)})" if vwma_60 and vwma_120 and vwma_60 > vwma_120 else f"🅾️({str(cnt_60_120_reverse).zfill(2)})"

        message_lines.append(f"{str(idx).rjust(2)}.{five_twenty}{twenty_sixty}{sixty_hundredtwenty} {coin.replace('KRW-', '')}:{trade_price}억({price_change_str})")

    message_lines.append("----------------------------------")
    message_lines.append("(BTC-[일봉]) 🟩 [ 3️⃣ ] 🅾️-✅️-🅾️")
    message_lines.append("(BTC-[분봉]) 🟩 [ 5️⃣ ] 🅾️-✅️-✅️")

    send_telegram_message("\n".join(message_lines), btc_status_1h, btc_status_4h)

# 메인 실행
def main():
    btc_status_1h, btc_status_4h = check_bitcoin_status()
    golden_cross_coins = find_golden_cross_coins(krw_tickers, interval="minute1440", count=200)
    send_golden_cross_message(golden_cross_coins, btc_status_1h, btc_status_4h, btc_price_change_percentage=0.0)

# 재시도 로직이 포함된 API 호출 래퍼
def retry_request(func, *args, **kwargs):
    max_retries = 3
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            if result is not None:
                return result
        except Exception as e:
            logging.error(f"API 호출 실패, 재시도 {attempt+1}/{max_retries}: {str(e)}")
            time.sleep(retry_delay)
    return None

# 스케줄러 설정
schedule.every(1).minutes.do(main)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
