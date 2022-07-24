import os
import sys

# load the environment variables for this setup
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

LOG_LEVEL = os.environ.get("LOG_LEVEL") or "DEBUG"

# setup logging infrastructure
import logging
logger = logging.getLogger(__name__)

# logging.getLogger("urllib3").setLevel(logging.WARN)

FORMAT  = os.environ.get("LOGGER_FORMAT", "%(message)s")
DATEFMT = "%Y-%m-%d %H:%M:%S %z"

logging.basicConfig(level=LOG_LEVEL, format=FORMAT, datefmt=DATEFMT)
formatter = logging.Formatter(FORMAT, DATEFMT)
logging.getLogger().handlers[0].setFormatter(formatter)

from flask import Flask, request, make_response, abort
import flask_restful
from flask_restful import Resource

import json

import random
random.seed()

import time

# setup callback endpoints

class Encoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, datetime):
      return o.isoformat()
    if isinstance(o, set):
      return list(o)
    return super().default(o)

server = Flask(__name__)  
api = flask_restful.Api(server)
server.config['RESTFUL_JSON'] =  {
  "indent" : 2,
  "cls"    : Encoder
}

FAIL_RATE = 0.50

class Callback(Resource):
  def post(self, name):
    msg = request.get_json()
    time.sleep(random.random())
    if random.random() <= FAIL_RATE:
      logger.warn(f"[{name}] failing to receive message")
      return make_response("Fail", 500)
    else:
      logger.info(f"[{name}] received message: {msg}")
      return make_response("Accepted", 202)

api.add_resource(Callback, "/<name>")
