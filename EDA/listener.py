""" CustomerBehavior Service Domain - Operations handler """

import time
import json
import argparse
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.resources.queue import Queue
from solace.messaging.config.missing_resources_creation_configuration import MissingResourcesCreationStrategy
from solace.messaging.receiver.message_receiver import (InboundMessage,MessageHandler)
from messaging_util import connect_to_broker


# Define the handlers for each insight type
def handle_demographics(json_payload):
    print("Handling Demographics insight")

def handle_engagement(json_payload):
    print("Handling Engagement insight")

def handle_journey(json_payload):
    print("Handling Journey insight")

def handle_psycographics(json_payload):
    print("Handling Psycographics insight")

def handle_retention(json_payload):
    print("Handling Retention insight")

def handle_satisfaction(json_payload):
    print("Handling Satisfaction insight")

def handle_default(json_payload):
    print("Handling default case")

## Callback for received messages
class MessageHandlerImpl(MessageHandler):
    def __init__(self, persistent_receiver: PersistentMessageReceiver):
        self.receiver: PersistentMessageReceiver = persistent_receiver
   
    """ Handles messages received from the broker. """ 
    def on_message(self, message: InboundMessage):
         
        topic = message.get_destination_name()
        assert topic.startswith('BIAN/12.0.0/CustBehIns')
        payload = message.get_payload_as_string() 
        print("\n" +f"Message received. Topic: {topic}") 
        print(f"Message Payload: {payload}")
        # print("\n" + f"Message dump: {message} \n")

        ## BIAN Code
        # check message label to validate expected payload 
        assert topic.startswith('BIAN/12.0.0')
        
        #confirm message received
        self.receiver.ack(message)
        
        # Parse the JSON data
        json_data = json.loads(payload)

        # Extract the CustomerInsightType
        
        insight_type = json_data.get("NotificationCustomerBehaviorAnalysis", {})  \
                                    .get("CustomerBehaviorAnalysis", {})\
                                    .get("CustomerInsightType")

        if insight_type == "Demographics":
            handle_demographics(json_data)
        elif insight_type == "Engagement":
            handle_engagement(json_data)
        elif insight_type == "Journey":
            handle_journey(json_data)
        elif insight_type == "Psycographics":
            handle_psycographics(json_data)
        elif insight_type == "Retention":
            handle_retention(json_data)
        elif insight_type == "Satisfaction":
            handle_satisfaction(json_data)
        else:
            handle_default(json_data)
        
def main():
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Listener to BIAN events')
    parser.add_argument('--queue', help='Queue name')
    args = parser.parse_args()

    queue_name = args.queue or "bian-cust-insights"
 
    # attempts to connect to BIAN cloud broker
    messaging_service = connect_to_broker() 

    durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)

    # Build a receiver and bind it to the durable exclusive queue
    persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder()\
                .with_missing_resources_creation_strategy(MissingResourcesCreationStrategy.CREATE_ON_START)\
                .build(durable_exclusive_queue)
    persistent_receiver.start()
    
    # Build a direct receiver with the given topics and start it
    # direct_receiver = messaging_service.create_direct_message_receiver_builder()\
    #                        .with_subscriptions(topics_sub)\
    #                        .build()
    # direct_receiver.start()
    #print(f'Direct Receiver is running? {direct_receiver.is_running()}')

    try:
        # Callback for received messages
        persistent_receiver.receive_async(MessageHandlerImpl(persistent_receiver))
        print(f'Persistent receiver started, bound to Queue [{durable_exclusive_queue.get_name()}]')
        try: 
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print('\nDisconnecting Messaging Service')
    finally:
        print('\nTerminating receiver')
        persistent_receiver.terminate()
        print('\nDisconnecting Messaging Service')
        messaging_service.disconnect()


main()
    