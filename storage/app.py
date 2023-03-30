import datetime
import json

import connexion
from connexion import NoContent
import swagger_ui_bundle

import mysql.connector 
import pymysql
import yaml
import logging
import logging.config

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from buy import Buy
from sell import Sell

import pykafka
from pykafka import KafkaClient
from pykafka.common import OffsetType

import threading
from threading import Thread

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():

    client = KafkaClient(hosts="kafka:9092")
    client.topics
    topic = client.topics[f"{app_config['kafka']['topic']}"]
    
    messages = topic.get_simple_consumer( 
        reset_offset_on_start = False, 
        auto_offset_reset = OffsetType.LATEST
    )

    for msg in messages:
        # msg_str = msg.decode(encoding='UTF-8', errors='strict')
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        payload = msg["payload"]
        msg_type = msg["type"]

        session = DB_SESSION()

        logger.debug("CONSUMER::storing buy event")
        #logger.debug(msg)

        if msg_type == 'buy':
            obj = Buy(
                payload['buy_id'],
                payload['item_name'],
                payload['item_price'],
                payload['buy_qty'],
                payload['trace_id']
            )
        else:
            obj = Sell(
                payload['sell_id'],
                payload['item_name'],
                payload['item_price'],
                payload['sell_qty'],
                payload['trace_id']
            )

        session.add(obj)
        session.commit()
        session.close()

    messages.commit_offsets()

    # TODO: call messages.commit_offsets() to store the new read position

# Endpoints
def buy(body):
    session = DB_SESSION()

    buy = Buy(
        body['buy_id'],
        body['item_name'],
        body['item_price'],
        body['buy_qty'],
        body['trace_id']
    )

    session.add(buy)
    session.commit()
    session.close()

    return NoContent, 201

def get_buys(timestamp):
    session = DB_SESSION()

    rows = session.query(Buy).filter(Buy.date_created >= timestamp)

    data = []
        
    for row in rows:
        data.append(row.to_dict())
    
    session.close()

    logger.debug(f"Reeturning {len(data)} buy events >= {timestamp}")

    return data, 200

def sell(body):
    session = DB_SESSION()

    sell = Sell(
        body['sell_id'],
        body['item_name'],
        body['item_price'],
        body['sell_qty'],
        body['trace_id']
    )

    session.add(sell)
    session.commit()
    session.close()

    return NoContent, 201

def get_sells(timestamp):
    session = DB_SESSION()

    rows = session.query(Sell).filter(Sell.date_created >= timestamp)

    data = []
        
    for row in rows:
        data.append(row.to_dict())
    
    session.close()

    logger.debug(f"Reeturning {len(data)} sell events >= {timestamp}")

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', base_path="/storage", strict_validation=True, validate_responses=True)

with open('log_conf.yml', encoding='utf-8') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    tl = Thread(target=process_messages)
    tl.daemon = True
    tl.start()
    app.run(port=8090)