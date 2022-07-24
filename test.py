import os
import time

# load the environment variables for this setup
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

LOG_LEVEL = os.environ.get("LOG_LEVEL") or "INFO"

# setup logging infrastructure
import logging
logger = logging.getLogger(__name__)

logging.getLogger("urllib3").setLevel(logging.WARN)

FORMAT  = os.environ.get("LOGGER_FORMAT", "%(message)s")
DATEFMT = "%Y-%m-%d %H:%M:%S %z"

logging.basicConfig(level=LOG_LEVEL, format=FORMAT, datefmt=DATEFMT)
formatter = logging.Formatter(FORMAT, DATEFMT)
logging.getLogger().handlers[0].setFormatter(formatter)

import random
random.seed()

from postoffice import init, subscribe, send, sync, pending, status

from multiprocessing.dummy import Process

init()

SUBSCRIBERS = [ f"cb{idx}" for idx in range(3) ]
MESSAGES    = 100

# setup subscriptions
for name in SUBSCRIBERS:
  subscribe(name, f"http://localhost:8000/{name}")

def generate_messages():
  for _ in range(MESSAGES):
    sub = random.choice(SUBSCRIBERS)
    send(sub, { "hello" : sub })
    time.sleep(random.random())

g = Process(target=generate_messages)
g.start()

generating = True

def continue_syncing():
  while generating or pending():
    sync()
    time.sleep(1)
    s = status()
    min_age     = round(s["min_age"] or 0)
    avg_age     = round(s["avg_age"] or 0)
    max_age     = round(s["max_age"] or 0)
    min_retries = round(s["min_retries"] or 0)
    avg_retries = round(s["avg_retries"] or 0)
    max_retries = round(s["max_retries"] or 0)
    logger.info(f"🔈 pending: {pending()}")
    logger.info(f"   age: {min_age}/{avg_age}/{max_age}")
    logger.info(f"   retries: {min_retries}/{avg_retries}/{max_retries}")

s = Process(target=continue_syncing)
s.start()

# wait for generation
g.join()

# wait for syncing
generating = False
s.join()
