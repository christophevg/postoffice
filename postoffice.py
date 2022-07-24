import logging
logger = logging.getLogger(__name__)

import os

from datetime import datetime, timedelta

import pymongo
from pymongo import MongoClient

from multiprocessing.dummy import Pool

import requests

# retry strategy
# TODO: make configurable, e.g. add factor,...
retries = [
  timedelta(seconds=s) for s in [ 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256 ]
]

# setup mongo persistency backend
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URL)
db = client.postoffice

def init():
  db.messages.drop()
  db.subscriptions.drop()

# setup a subscription with a name mapping to a callback
# TODO: extend callback to complete delivery setup, e.g. including auth
def subscribe(name, callback):
  logger.info(f"📪 new subscription: {name} -> {callback}")
  return db.subscriptions.insert_one({
    "name"     : name,
    "callback" : callback
  }).inserted_id

# send a message by looking up the subscription and creating a message for it
def send(to, msg):
  logger.info(f"✉️  sending message to {to}: {msg}")
  box = db.subscriptions.find_one({"name" : to})
  if not box:
    raise ValueError(f"unknown addressee: {to}")

  now = datetime.utcnow()
  return db.messages.insert_one({
    "callback" : box["callback"],
    "msg"      : msg,
    "when"     : now,
    "start"    : now,
    "retry"    : 0
  })

# sync can be called periodically to fetch messages that need sending and do so
# TODO: ensure no two syncs can be happening at the same time ;-)
def sync():
  logger.debug("♻️  synching...")
  messages = list(db.messages.find({
    "status" : { "$exists" : False },
    "when"   : { "$lt" : datetime.utcnow() }
  }))
  logger.debug(f"  👉 handling {len(messages)} messages...")
  pool = Pool(processes=10)
  pool.map_async(unwrap_and_deliver, messages)
  pool.close()
  pool.join()

def unwrap_and_deliver(message):
  deliver(**message)

# deliver a message and in case of failure, update it's "when" time backing off
# TODO: setup delivery session
# TODO: centralize message update
def deliver(_id=None, msg=None, start=None, age=None, when=None, callback=None, retry=0, status=None):
  logger.debug(f"  👉 attempting delivery to {callback} ({retry} tries)")
  try:
    response = requests.post(callback, json=msg)
    response.raise_for_status()
    logger.info(f"  ✅ delivery to {callback} succeeded after {retry + 1} tries")
    now = datetime.utcnow()
    # delivery succeeded, ensure that message isn't retried
    db.messages.update_one({
      "_id" : _id
    },{
      "$set" : {
        "status": "delivered",
        "end"   : now,
        "age"   : (now - start).total_seconds()
      }
    })
  except:
    now = datetime.utcnow()
    logger.info(f"  ⚠️  delivery to {callback} failed")
    if retry < len(retries):
      when += retries[retry]
      if retry + 1 < len(retries):
        logger.debug(f"    👉 rescheduling message delivery for retry {retry + 1} on {when}")
        db.messages.update_one({
          "_id" : _id
        },{
          "$set" : {
            "when" : when,
            "age"  : (now - start).total_seconds()
          },
          "$inc" : { "retry" : 1 }
        })
      else:
        logger.debug(f"    🚨 maximum attempts ({retry + 1}) reached, marking as failed")
        db.messages.update_one({
          "_id" : _id
        },{
          "$set" : {
            "status" : "failed",
            "age"    : (start - now).total_seconds()
          }
        })


# TODO: incorporate into status
def pending():
  messages = list(db.messages.find({
    "status" : { "$exists" : False }
  }))
  return len(messages)

# TODO: make more useful ;-)
def status():
  status = list(db.messages.aggregate([
    { "$group": {
      "_id"         : None,
      "avg_age"     : { "$avg" : "$age"   },
      "min_age"     : { "$min" : "$age"   },
      "max_age"     : { "$max" : "$age"   },
      "avg_retries" : { "$avg" : "$retry" },
      "min_retries" : { "$min" : "$retry" },
      "max_retries" : { "$max" : "$retry" }
    }}
  ]))[0]
  return status
