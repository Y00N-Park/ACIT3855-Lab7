import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
import pykafka
from pykafka import KafkaClient
import requests
import uuid
import yaml 

def process_event(event, endpoint):
    trace_id = str(uuid.uuid4())
    event['trace_id'] = trace_id

    logger.debug(f'Received {endpoint} event with trace id {trace_id}')

    client = KafkaClient(hosts="kafka:9092")
    client.topics
    topic = client.topics[f"{app_config['events']['topic']}"]
    #topic = client.topics['events']
    producer = topic.get_sync_producer()

    timestamp = datetime.datetime.now().strftime('%U-%m-%d %H:%M:%S')

    producer_dict = {
        "type": endpoint,
        "datetime": timestamp,
        "payload": event
    }

    json_object = json.dumps(producer_dict)
    #.encode(encoding='UTF-8', errors='strict')

    producer.produce(json_object.encode('utf-8'))

    logger.debug(f"PRODUCER::producing {endpoint} event")
    logger.debug(json_object)    
    # TODO: log "PRODUCER::producing x event" where x is the actual event type
    # TODO: log the json string

    return NoContent, 201

# Endpoints
def buy(body):
    process_event(body, 'buy')
    return NoContent, 201

def sell(body):
    process_event(body, 'sell')
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", base_path="/receiver", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8080)