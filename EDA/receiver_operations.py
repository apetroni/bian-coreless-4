""" CustomerBehavior Service Domain - Operations handler """

import time
from enum import Enum
import argparse

from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.resources.queue import Queue
from solace.messaging.config.missing_resources_creation_configuration import MissingResourcesCreationStrategy
from solace.messaging.receiver.message_receiver import (InboundMessage,MessageHandler)
from messaging_util import connect_to_broker


#static data as a test 
data = []
data.append( ["CustomerBehaviorInsights",'1'] ) 
data[0].append( ["Insight",'11'] )
data[0].append( ["Insight",'12'] )
data.append( ["CustomerBehaviorInsights",'2'] ) 
data[1].append( ["Insight",'21'] )
data[1].append( ["Insight",'22'] )

def handle_execute_CR_operation (CRL:str, CRID: str, message: InboundMessage):
    print("Handling Execute operation CR Level")

def handle_retrieve_CR_operation (CR: str, CRID: str, message: InboundMessage):
    print("Handling Retrieve operation CR Level")

def handle_execute_BQ_operation (CR: str, CRID: str, BQ: str, BQID: str, message: InboundMessage):
    print("Handling Retrieve operation BQ level")    

def handle_retrieve_BQ_operation (CR: str, CRID: str, BQ: str, BQID: str, message: InboundMessage):
    print("Handling Retrieve operation BQ level")   
    
## Callback for received messages
class MessageHandlerImpl(MessageHandler):
    def __init__(self, persistent_receiver: PersistentMessageReceiver):
        self.receiver: PersistentMessageReceiver = persistent_receiver
   
    """ Handles messages received from the broker. """ 
    def on_message(self, message: InboundMessage):
         
        topic = message.get_destination_name()
        #assert topic.startswith("BIAN.12.0.0.CustBevIns.Operation")
        #assert topic.startswith('BIAN-3/CustomerBehaviorInsights/10.0.0/CustomerBehaviorInsights')

        payload = message.get_payload_as_string() 
        
        print("\n" + f"Message Payload: {payload} \n")
        print("\n" + f"Message Topic: {topic} \n")
        print("\n" + f"Message dump: {message} \n")

        ## BIAN Code
        # check message label to validate expected payload 
        #assert topic.startswith('BIAN-3/CustomerBehaviorInsights/10.0.0')

        # OperationId is the last part of the URL
        #
        # 'BIAN-3/CustomerBehaviorInsights/10.0.0/CustomerBehaviorInsights/A01/Retrieve'  
        #   retrieve operation on control record with id='A01'
        
        # 'BIAN-3/CustomerBehaviorInsights/10.0.0/CustomerBehaviorInsights/A01/Insight/X02/Retrieve' -- BQ level retrieve
        #     retrive the 'Insight' behavior qualifier with id='X02' part of control record id='A01'
        #
        
        # Extract the service domain, control record, behavior qualifier, and operation   
        try: 
            parts = topic.split("/")
            # operation is the last element of the URL 
            operation_id = parts[-1] 
            service_domain = parts[1]
            sd_version = parts[2]
            control_record = parts[3]
            control_record_identifier = parts[4]
            
            behavior_qualifier = None
            behavior_qualifier_identifier = None
            
            # behavior qualifier is optional, if there extract details
            if parts[5] != operation_id:
                behavior_qualifier = parts[5] if len(parts) > 5 else None
                behavior_qualifier_identifier = parts[6] if len(parts) > 6 else None
            # TODO: recursive search sub behavior qualifier details
                            
            print(f"Service domain: {service_domain}\n operation: {operation_id}")
            print(f" control record: {control_record}, CR identifier: {control_record_identifier}") 
            print(f" behaviour qualifier: {behavior_qualifier}, BQ identifier: {behavior_qualifier_identifier}")    
            
            # handle operation at BQ level?
            if behavior_qualifier:
                if operation_id == "Execute":
                    handle_execute_BQ_operation(control_record, control_record_identifier, 
                                                behavior_qualifier, behavior_qualifier_identifier,
                                                message)
                elif operation_id == "Retrieve":
                    handle_retrieve_BQ_operation(control_record, control_record_identifier, 
                                                behavior_qualifier, behavior_qualifier_identifier,
                                                message)
                else:
                    # Handle unknown or unsupported BQ operation
                    print(f"Unsupported BQ operation: {operation_id}")
            else:    
                # handle operation at CR level?
                if operation_id == "Execute":
                    handle_execute_CR_operation(control_record, control_record_identifier, message)
                elif operation_id == "Retrieve":
                    handle_retrieve_CR_operation(control_record, control_record_identifier, message)
                else:
                    # Handle unknown or unsupported CR operation
                    print(f"Unsupported operation: {operation_id}")
            
            try:
                if behavior_qualifier:
                    out = data[control_record][control_record_identifier][behavior_qualifier][behavior_qualifier_identifier]   
                else:
                    out = data[control_record][control_record_identifier]
            except Exception as ex:
                pass 
            
        except Exception as ex:
            print ("Exception while processing incoming message" , ex) 
        finally:
            # acknowledge the message to the broker before leaving the callback
            # not doing so it will cause the message to be redelivered automatically
             self.receiver.ack(message)      
                

     
def main():
   
    
      # Parse command line arguments
    parser = argparse.ArgumentParser(description='Listener to BIAN events')
    parser.add_argument('--queue', help='Queue name')
    args = parser.parse_args()

    queue_name = args.queue or "bian-cust-insights-OPERATIONS"
 
    # attempts to connect to BIAN cloud broker
    messaging_service = connect_to_broker() 

    durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)

    # Build a receiver and bind it to the durable exclusive queue
    persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder()\
                .with_missing_resources_creation_strategy(MissingResourcesCreationStrategy.CREATE_ON_START)\
                .build(durable_exclusive_queue)
    persistent_receiver.start()

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
    