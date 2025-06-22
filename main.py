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

# 메시지 전송 함수
def send_telegram_message(message, btc_status_1h, btc_status_4h, is_new_coin=False, btc_price_change_percentage=0.0):
    max_retries = 3
    retry_delay = 5  # 재시도 간격 (초)

    for retry_count in range(1, max_retries + 1):
        try:
            # 메시지와 BTC 상태를 함께 보내기
            message_with_status = f"{message}\n(비트-[일봉]){' 🟩 ' if btc_status_1h else ' 🟥 '}\n(비트-[분봉]){' 🟩 ' if btc_status_4h else ' 🟥 '}"
            if is_new_coin:
                message_with_status += ""
            bot.sendMessage(chat_id=telegram_user_id, text=message_with_status)
            logging.info("텔레그램 메시지 전송 성공: %s", message_with_status)
            return  # 메시지 전송 성공 시 함수 종료
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/%d): %s", retry_count, max_retries, str(e))
            time.sleep(retry_delay)

    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

# 거래량 가중 이동평균선(VWMA) 계산 함수
def calculate_vwma(data, volume, period):
    if len(data) < period:
        return None  # 데이터가 충분하지 않으면 None 반환
    weighted_price = data[-period:] * volume[-period:]
    total_volume = volume[-period:].sum()
    
    if total_volume == 0:
        return None  # 거래량이 0이면 계산 불가
    return weighted_price.sum() / total_volume  # VWMA 계산

# 비트코인 상태 확인 함수 업데이트
def check_bitcoin_status():
    btc_ticker = "KRW-BTC"
    btc_df = retry_request(pyupbit.get_ohlcv, btc_ticker, interval="minute1440", count=200)
    if btc_df is not None and len(btc_df) >= 200:
        btc_vwma_1 = calculate_vwma(btc_df['close'].values, btc_df['volume'].values, 1)
        btc_vwma_2 = calculate_vwma(btc_df['close'].values, btc_df['volume'].values, 2)
        btc_status_1h = 1 if btc_vwma_1 is not None and btc_vwma_2 is not None and btc_vwma_1 > btc_vwma_2 else 0

        btc_df_4h = retry_request(pyupbit.get_ohlcv, btc_ticker, interval="minute60", count=200)
        if btc_df_4h is not None and len(btc_df_4h) >= 200:
            btc_vwma_1_4h = calculate_vwma(btc_df_4h['close'].values, btc_df_4h['volume'].values, 1)
            btc_vwma_2_4h = calculate_vwma(btc_df_4h['close'].values, btc_df_4h['volume'].values, 50)
            btc_status_4h = 1 if btc_vwma_1_4h is not None and btc_vwma_2_4h is not None and btc_vwma_1_4h > btc_vwma_2_4h else 0
        else:
            logging.error("비트코인 4시간 데이터를 불러올 수 없습니다.")
            btc_status_4h = None

        return btc_status_1h, btc_status_4h
    else:
        logging.error("비트코인 데이터를 불러올 수 없습니다.")
        return None, None

# 정배열 돌파 코인 확인 함수 (캔들 수 부족해도 최소 2개 이상 있으면 분석)
def find_golden_cross_coins(tickers, interval, count):
    golden_cross_coins = []

    for ticker in tickers:
        df = retry_request(pyupbit.get_ohlcv, ticker, interval=interval, count=count)
        if df is not None and len(df) >= 2:
            vwma_1 = calculate_vwma(df['close'].values, df['volume'].values, 1)
            vwma_2 = calculate_vwma(df['close'].values, df['volume'].values, 200)
            if vwma_1 is not None and vwma_2 is not None and vwma_1 > vwma_2:
                golden_cross_coins.append(ticker)

    return golden_cross_coins

# 메인 함수
def main():
    btc_status_1h, btc_status_4h = check_bitcoin_status()
    golden_cross_coins = find_golden_cross_coins(krw_tickers, interval="minute60", count=200)
    send_golden_cross_message(golden_cross_coins, btc_status_1h, btc_status_4h, btc_price_change_percentage=0.0)

# 거래대금을 계산하는 함수 (상위 10개 코인만)
def calculate_trade_price(coins):
    url = "https://api.upbit.com/v1/candles/minutes/10"
    total_trade_price = dict()

    # 한국 시간대 설정
    kr_tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(kr_tz)

    if now.hour >= 9:
        total_trade_price = dict()

    for coin in coins:
        querystring = {"market": coin, "count": 144}
        response = retry_request(requests.get, url, params=querystring)
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

# 가격 변동률을 계산하는 함수 (캔들 수가 2개 이상이면 진행)
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

# 정배열 돌파 코인 메시지 전송
def send_golden_cross_message(golden_cross_coins, btc_status_1h, btc_status_4h, btc_price_change_percentage):
    golden_trade_price_result = calculate_trade_price(golden_cross_coins)
    golden_trade_price_result = {coin: trade_price for coin, trade_price in golden_trade_price_result.items() if trade_price >= 200}

    if not golden_trade_price_result:
        message = "🔴 현재 200억 이상의 거래대금을 가진 코인이 없습니다.\n\n업비트 상태 확인 완료."
        send_telegram_message(message, btc_status_1h, btc_status_4h)
        return

    message_lines = []

    message_lines.append("LONG2️⃣---------------------------")

    for idx, (coin, trade_price) in enumerate(sorted(golden_trade_price_result.items(), key=lambda x: x[1], reverse=True), start=1):
        price_change = calculate_price_change_percentage(coin)
        price_change_str = f"{price_change:+.2f}%" if price_change is not None else "N/A"

        # VWMA 상태 계산
        df = retry_request(pyupbit.get_ohlcv, coin, interval="minute60", count=200)
        vwma_1 = calculate_vwma(df['close'].values, df['volume'].values, 1) if df is not None else None
        vwma_50 = calculate_vwma(df['close'].values, df['volume'].values, 50) if df is not None else None
        vwma_200 = calculate_vwma(df['close'].values, df['volume'].values, 200) if df is not None else None
        vwma_100 = calculate_vwma(df['close'].values, df['volume'].values, 100) if df is not None else None


        five_twenty = " ✅️" if vwma_1 is not None and vwma_50 is not None and vwma_1 > vwma_50 else " 🅾️"
        twenty_fifty = "✅️" if vwma_1 is not None and vwma_200 is not None and vwma_1 > vwma_200 else "🅾️"
        fifty_two_hundred = "✅️" if vwma_50 is not None and vwma_200 is not None and vwma_50 > vwma_200 else "🅾️"

        # 줄바꿈 추가 및 랭크 번호 포함
        message_lines.append(
            f"{idx}.{five_twenty}-{twenty_fifty}-{fifty_two_hundred}  {coin.replace('KRW-', '')} : {trade_price}억 ({price_change_str}) ")

    message_lines.append("----------------------------------")
    message_lines.append("(알트-[집중]) 🟩 [ 1️⃣ ] ✅️-🅾️-🅾️")
    final_message = "\n".join(message_lines)
    send_telegram_message(final_message, btc_status_1h, btc_status_4h)


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
