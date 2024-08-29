""" CustomerBehavior Service Domain - Customer Insights Publisher """
#
#  Author: Alessandro Petroni <apetroni@gmail.com>

import time
import random
import json
from abc import ABC
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher, MessagePublishReceiptListener

from messaging_util import connect_to_broker

BQ_insights_types = ['Demographics',
                    'Engagement', 
                    'Journey', 
                    'Psycographics', 	
                    'Retention', 
                    'Satisfaction']


class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"): 
        print("on_failed_publish")
class PublisherMessageReceiptListener(MessagePublishReceiptListener):
    def on_publish_receipt(self, publish_receipt: 'PublishReceipt'):
        pass
        #print("on publish, sends the publish receipt")


#            
def main():
    # attempts to connect to BIAN cloud broker
    messaging_service = connect_to_broker() 
    
    # Create a persistent message publisher and start it
    publisher: PersistentMessagePublisher = messaging_service.create_persistent_message_publisher_builder().build()
    publisher.start()
    # set a message delivery listener to the publisher
    publisher.set_message_publish_receipt_listener(PublisherMessageReceiptListener())

    # Creates a direct message publisher and starts it
    #direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    #direct_publisher.set_publish_failure_listener(PublisherErrorHandling())

    print(f'Publisher ready? {publisher.is_ready()}')

    # Prepare outbound message payload and body   
    json_payload = """
{
  "NotificationCustomerBehaviorAnalysis": {
    "CustomerBehaviorAnalysis": {
      "CustomerReference": {
        "CustomerID": "12345",
        "CustomerName": "John Doe"
      },
      "CustomerInsightAnalysisSchedule": "2024-06-30T14:00:00Z",
      "CustomerBehaviorAnalysisInsightsRecord": "Insight record data here...",
      "CustomerInsightCalculationDate": "2024-06-30"
    }
  }
}
"""
    cust_behavior_insights_json = json.loads(json_payload)
    
    outbound_msg_builder = messaging_service.message_builder() \
                    .with_application_message_id("id_001") 
    
    TOPIC_PREFIX = "BIAN/12.0.0/CustBehIns/Analysis/"
    
    print("\nSend a KeyboardInterrupt to stop publishing\n")
    count = 1
    try:
        while True:
            # select random BQ Insight type
            random_insight_type = random.choice(BQ_insights_types)
            
            # build the full topic for the BQ Insight 
            topic = Topic.of(f"{TOPIC_PREFIX}{random_insight_type}")
    
            # update CustomerInsightType in json payload
            cust_behavior_insights_json['NotificationCustomerBehaviorAnalysis']\
                                            ['CustomerBehaviorAnalysis']\
                                                ['CustomerInsightType'] = random_insight_type
            
            # creates the NotificationCustomerBehaviorAnalysis json message
            message_body = json.dumps(cust_behavior_insights_json)
            
            # creates the message to be sent
            outbound_msg = outbound_msg_builder \
                            .with_application_message_id(f'NEW {count}')\
                            .build(f'{message_body}')
            
            #publishes the message with application message id and payload                            
            publisher.publish( message=outbound_msg, destination=topic)

            print(f'Published message with {topic}')
            count += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    finally:
        print('\nTerminating receiver')
        publisher.terminate()
        print('\nDisconnecting Messaging Service')
        messaging_service.disconnect()


main()
    