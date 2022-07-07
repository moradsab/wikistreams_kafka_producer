from ast import parse
import json
import argparse
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

def create_kafka_producer(bootstrap_server):
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'),max_in_flight_requests_per_connection=1)
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)

def construct_recenntchange_event(event_data):
    # use dictionary to change assign namespace value and catch any unknown namespaces (like ns 104)
    try:
        user_types = {True: 'bot', False: 'human'}
        user_type = user_types[event_data['bot']]
        event = {"title": event_data['title'],
            "domain": event_data['meta']['domain'],
            "timestamp": event_data['meta']['dt'],
            "user_name": event_data['user'],
            "user_type": user_type,}
            
    except KeyError:
        return None
    except ValueError:
        return None
    else:
        return event
        

def construct_page_create_event(event_data):
    try:
        user_types = {True: 'bot', False: 'human'}
        user_type = user_types[event_data['performer']['user_is_bot']]
        event = {"title": event_data['page_title'],
            "domain": event_data['meta']['domain'],
            "timestamp": event_data['meta']['dt'],
            "user_name": event_data['performer']['user_text'],
            "user_type": user_type,
            "edit_count":event_data['performer']['user_edit_count'],}
    except KeyError:
        return None
    except ValueError:
        return None
    else:
        return event
        
def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')
    parser.add_argument('--bootstrap_server', default='localhost:9092', help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int, default=10000)

    return parser.parse_args()

def get_time(input_time):
    try:
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        past = datetime.strptime(input_time, fmt)
        curr= datetime.now()
        diff=curr-past
        hours=diff.total_seconds()/3600
        if hours <=730.484398 and hours>168:
            return 'month'
        elif hours<=168 and hours>24:
            return 'week'
        elif hours<=24 and hours>1:
            return 'day'
        elif hours<=1:
            return 'hour'
        return 0
    except ValueError:
        return 0

def time_test(input_time):
    try:
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        past = datetime.strptime(input_time, fmt)
        curr= datetime.now()
        diff=curr-past
        if diff.total_seconds()/3600<4.1:
            return 1
        return 0
    except ValueError:
        pass

def iswiki(domain):
    if domain!='':
        dom=domain.split('.')[1]
        if dom=='wikipedia':
            return 1
    return 0

def page_edit_send_message(producer,event):
    try:
        event_data = json.loads(event.data)
    except ValueError:
        return 0
    else:
        event_to_send = construct_recenntchange_event(event_data)
        if event_to_send and iswiki(event_to_send['domain']) and get_time(event_to_send.get('timestamp'))!=0 and event_data.get('type')=='edit':
            print(event_to_send,'re')
            producer.send('page-edit', value=event_to_send)
        else:
            return 0
        return 1

def page_create_send_message(producer,event):
    try:
        event_data = json.loads(event.data)
    except ValueError:
        return 0
    else:
        event_to_send = construct_page_create_event(event_data)
        if event_to_send and get_time(event_to_send['timestamp'])!=0 and iswiki(event_to_send['domain']):
            print(event_to_send,'page')
            producer.send('page-create', value=event_to_send)
        else:
            return 0
        return 1

def revision_create_send_message(producer,event):
    try:
        event_data = json.loads(event.data)
    except ValueError:
        return 0
    else:
        event_to_send = construct_page_create_event(event_data)
        if event_to_send and get_time(event_to_send['timestamp'])!=0 and iswiki(event_to_send['domain']):
            print(event_to_send,'revision')
            producer.send('revision-create', value=event_to_send)
        else:
            return 0
        return 1

def produce(page_edit_producer,page_create_producer,revision_create_producer,events_to_produce):
    # consume websocket
    page_create_url = 'https://stream.wikimedia.org/v2/stream/page-create'
    page_edit_url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    revision_create_url = 'https://stream.wikimedia.org/v2/stream/revision-create'
    
    print('Messages are being published to Kafka topic')
    messages_count = 0
    for page_edit_event,page_create_event,revision_create_event in zip(EventSource(page_edit_url),EventSource(page_create_url),EventSource(revision_create_url)):
        if page_create_event.event == 'message':
            messages_count+=page_create_send_message(page_create_producer,page_create_event)
        if page_edit_event.event == 'message':
            messages_count+=page_edit_send_message(page_edit_producer,page_edit_event)
        if revision_create_event.event=='message':
            messages_count+=revision_create_send_message(revision_create_producer,revision_create_event)
        if messages_count >= events_to_produce:
            page_edit_producer.close()
            page_create_producer.close()
            revision_create_producer.close()
            print('Producer will be killed as {} events were producted'.format(events_to_produce))
            exit(0)

if __name__ == "__main__":
    # parse command line arguments
    args = parse_command_line_arguments()

    # init producers
    page_create_producer = create_kafka_producer(args.bootstrap_server)
    page_edit_producer = create_kafka_producer(args.bootstrap_server)
    revision_create_producer = create_kafka_producer(args.bootstrap_server)
    produce(page_edit_producer,page_create_producer,revision_create_producer,args.events_to_produce)