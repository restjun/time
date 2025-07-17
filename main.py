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

logging.basicConfig(level=logging.DEBUG)

def send_telegram_message(message):
    max_retries = 10
    retry_delay = 5
    for retry_count in range(1, max_retries + 1):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message)
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

def calculate_vwma(close, volume, period):
    if len(close) < period or len(volume) < period:
        return None
    close_series = pd.Series(close)
    volume_series = pd.Series(volume)
    vwma = (close_series[-period:] * volume_series[-period:]).sum() / volume_series[-period:].sum()
    return vwma

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
    return dict(sorted(total_trade_price.items(), key=lambda x: x[1], reverse=True)[:10])

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

def get_vwma_with_retry(close, volume, period):
    for _ in range(5):
        result = calculate_vwma(close, volume, period)
        if result is not None:
            return result
        time.sleep(0.5)
    return None

def send_filtered_top_volume_message(top_volume_coins):
    if not top_volume_coins:
        send_telegram_message("🔴 현재 1000억 이상의 거래대금을 가진 코인이 없습니다.\n\n업비트 상태 확인 완료.")
        return

    message_lines = []
    message_lines.append("🚀 *업비트 거래대금 TOP10 + VWMA 정배열 상태*")
    message_lines.append("━━━━━━━━━━━━━━━━━━━━━━")

    timeframes = {
        "1D": "day",
        "4h": "minute240",
        "1h": "minute60",
        "15m": "minute15"
    }

    idx = 1
    for coin, trade_price in sorted(top_volume_coins.items(), key=lambda x: x[1], reverse=True):
        price_change = calculate_price_change_percentage(coin)
        if price_change is None or price_change <= 0:
            continue

        price_change_str = f"{price_change:+.2f}%"
        all_tf_results = []

        for tf_label, tf_api in timeframes.items():
            df = get_ohlcv_with_retry(coin, interval=tf_api, count=200)
            if df is None:
                all_tf_results.append(f"{tf_label}: ❌")
                continue

            try:
                close = df['close'].values
                volume = df['volume'].values

                vwma_10 = get_vwma_with_retry(close, volume, 10)
                vwma_20 = get_vwma_with_retry(close, volume, 20)
                vwma_50 = get_vwma_with_retry(close, volume, 50)
                vwma_200 = get_vwma_with_retry(close, volume, 200)

                if None in [vwma_10, vwma_20, vwma_50, vwma_200]:
                    all_tf_results.append(f"{tf_label}: ❌")
                    continue

                f20 = "✅" if vwma_10 > vwma_20 else "🟥"
                t50 = "✅️" if vwma_20 > vwma_50 else "🟥"
                f200 = "✅" if vwma_50 > vwma_200 else "🟥"

                all_tf_results.append(f"{tf_label}: {f20}{t50}{f200}")
            except Exception as e:
                logging.error("VWMA 계산 실패 (%s %s): %s", coin, tf_label, str(e))
                all_tf_results.append(f"{tf_label}: ❌")
            time.sleep(0.3)

        message_lines.append(f"📊 {idx}. {coin.replace('KRW-', '')} | 💰 {trade_price}억 | 📈 {price_change_str}")
        for tf_result in all_tf_results:
            message_lines.append(f"    └ {tf_result}")
        message_lines.append("📉─────────────────")
        idx += 1

    if idx == 1:
        send_telegram_message("🔴 현재 조건을 만족하는 코인이 없습니다.\n🔴 업비트 상태 확인 완료.")
        return

    message_lines.append("🧭 *매매 원칙*")
    message_lines.append("✅ 추격매수 금지 / ✅ 분할매수 / ✅ 반드시 반익절 / ✅ 거래대금 1000억 이상")
    message_lines.append("━━━━━━━━━━━━━━━━━━━━")
    final_message = "\n".join(message_lines)
    send_telegram_message(final_message)

def main():
    filtered_tickers = get_common_upbit_okx_tickers()
    top_volume_coins = calculate_trade_price(filtered_tickers)
    filtered_coins = {coin: volume for coin, volume in top_volume_coins.items() if volume >= 500}
    send_filtered_top_volume_message(filtered_coins)

schedule.every(1).minutes.do(main)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
