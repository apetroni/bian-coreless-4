""" CustomerBehavior Service Domain - Operations handler """

import time
import json
import argparse

from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.resources.queue import Queue
from solace.messaging.config.missing_resources_creation_configuration import MissingResourcesCreationStrategy
from solace.messaging.receiver.message_receiver import (InboundMessage,MessageHandler)
from messaging_util import connect_to_broker
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher, MessagePublishReceiptListener

class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"): 
        print("on_failed_publish")
class PublisherMessageReceiptListener(MessagePublishReceiptListener):
    def on_publish_receipt(self, publish_receipt: 'PublishReceipt'):
        pass
        #print("on publish, sends the publish receipt")
class MessagePublisher():
    def __init__(self, messaging_service):        
        # Create a persistent message publisher and start it
        self.publisher: PersistentMessagePublisher = messaging_service.create_persistent_message_publisher_builder().build()
        # build the full topic for the BQ Insight 
        self.topic = Topic.of(f"BIAN/12.0.0/CustEligibilityRating/Updates_2")
        self.outbound_msg_builder = messaging_service.message_builder()    
        self.publisher.start()
        # set a message delivery listener to the publisher
        self.publisher.set_message_publish_receipt_listener(PublisherMessageReceiptListener())
        
    def publish_customer_flags_updates(self, customer_reference, rating):
            
        json_payload = json.loads('{"CustomerReference":"","EligibilityRating":""}')

        # update CustomerInsightType in json payload
        json_payload['CustomerReference'] = customer_reference
        json_payload['EligibilityRating'] = rating
        
        message_body = json.dumps(json_payload)
        outbound_msg = self.outbound_msg_builder.build(f'{message_body}')
                                                        
        #publishes the message withpayload                            
        self.publisher.publish( message=outbound_msg, destination=self.topic)
        print(f'Published message with {self.topic}')
    
    def test_publish_customer_flags_updates(self):
        self.publish_customer_flags_updates("123456789", "AAA")       


## Callback for received messages
class MessageHandlerImpl(MessageHandler):
    def __init__(self, persistent_receiver: PersistentMessageReceiver):
        self.receiver: PersistentMessageReceiver = persistent_receiver
        self.message_publisher = None
    def setMessagePublisher(self,message_publisher):
        self.message_publisher = message_publisher
      
    # Handles messages received from the broker. 
    def on_message(self, message: InboundMessage):
        topic = message.get_destination_name()
        print("\n" +f"Message received. Topic: {topic}") 
      
        payload = message.get_payload_as_string() 

        print(f"Message Payload: {payload}")
        # print("\n" + f"Message dump: {message} \n")

        
        # Parse the JSON data
        json_data = json.loads(payload)
        # Extract the CustomerInsightType
        expected_json_payload = '''
{
    "CustomerReference":"",
    "EligibilityRating":""
}
'''     
        ## Extract information from received message and store into DB
        try:         
            customer_reference = json_data.get("CustomerReference")
            eligibilty_rating = json_data.get("EligibilityRating")   
            
            ## backend logic, with a database 
                     
            # persist customer reference and eligibilty rating
            # INSERT into PartyRoutingProfile (CustomerReference, EligibilityRating) 
            # VALUES (@CustomerReference, @EligibilityRating)
            
            ## EXEC STORE_PROCEDURE @CustomerReference, @EligibilityRating 
            ## Call Salesforce via Salesforce API 
            ## Call REST API for Business logic Customor Tracing Fiserv 
            
            ## Front end integration    
            ## Websocket_UX_Customer_widget (customer_reference, eligibilty_rating) 
        
            ## Publish updates about customers' eligibility rating            
            self.message_publisher.publish_customer_flags_updates(customer_reference, eligibilty_rating)
           
            ## finally acknowledge the message received *after* pushing updates
            self.receiver.ack(message)
            
        except Exception as ex:
            print(ex)
        ## publish updates about custumerflags
        
            print(ex)   
            
def main():
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Listener to BIAN events')
    parser.add_argument('--queue', help='Queue name')
    args = parser.parse_args()

    ## connect to the broker and create a messaging service
    messaging_service = connect_to_broker() 

    queue_name = args.queue or "customer-eligibility-update-queue"
 
    ##### attempts to connect to BIAN cloud broker
    durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)
    
    # Build a receiver and bind it to the durable exclusive queue
    persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder()\
                .with_missing_resources_creation_strategy(MissingResourcesCreationStrategy.CREATE_ON_START)\
                .build(durable_exclusive_queue)
    persistent_receiver.start()
    
    msgpub = MessagePublisher(messaging_service)
    
    try:
        # Callback for received messages
        msg_handler = MessageHandlerImpl(persistent_receiver)
        persistent_receiver.receive_async(msg_handler)
        msg_handler.setMessagePublisher(msgpub)
        
        print(f'Persistent receiver started, bound to Queue [{durable_exclusive_queue.get_name()}]')
        try: 
            while True:
                time.sleep(1)
                # msgpub.test_publish_customer_flags_updates()
        except KeyboardInterrupt:
            print('\nDisconnecting Messaging Service')
    finally:
        print('\nTerminating receiver')
        persistent_receiver.terminate()
        print('\nDisconnecting Messaging Service')
        messaging_service.disconnect()

main()
    