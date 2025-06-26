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

