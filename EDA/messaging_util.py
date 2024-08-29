
""" Messaging Utility module for BIAN Coreless v4 Pilot """

## Establish connectivity with the BIAN cloud broker 
## expects all parameter from environment variables 
##    SOLACE_HOST, SOLACE_VPN, SOLACE_USERNAME, SOLACE_PASSWORD, SOLACE_CERT

#  Adapted by: Alessandro Petroni <apetroni@gmail.com>
#  ref https://github.com/SolaceSamples/solace-samples-python
#  
import os
import platform
from solace.messaging.config.transport_security_strategy import TLS
# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import (MessagingService,
                                                ReconnectionAttemptListener,
                                                ReconnectionListener,
                                                RetryStrategy, ServiceEvent,
                                                ServiceInterruptionListener)

if platform.uname().system == 'Windows':
    os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer

# Handle received messages

# Inner classes for error handling
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, service_event: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {service_event.get_cause()}")
        print(f"Message: {service_event.get_message()}")
    
    def on_reconnecting(self, service_event: ServiceEvent):
        print("\non_reconnecting")
        print(f"Error cause: {service_event.get_cause()}")
        print(f"Message: {service_event.get_message()}")

    def on_service_interrupted(self, service_event: ServiceEvent):
        print("\non_service_interrupted")
        print(f"Error cause: {service_event.get_cause()}")
        print(f"Message: {service_event.get_message()}")

## Connects to the Solace broker using the provided environment variables for configuration.
#
# This function expects and asserts that the following environment variables are set:
#  - SOLACE_HOST:     The host of the Solace broker.
#  - SOLACE_VPN:      The messaging VPN for the BIAN Coreless v4project.
#  - SOLACE_USERNAME: The username for authentication with the Solace broker.
#  - SOLACE_PASSWORD: The password for authentication with the Solace broker.
#  - SOLACE_CERT:     The directory containing the TLS/SSL certificates for 
#                       secure communication with the Solace broker.
#
# The function connects to the Solace broker using the messaging service and adds error 
# handling listeners for reconnections, reconnection attempts, and service interruptions.
#
# Returns:
#    messaging_service (MessagingService): The connected messaging service object.
def connect_to_broker():
    broker_host = os.environ.get('SOLACE_HOST')
    assert broker_host, "SOLACE_HOST environment variable is not set"

    broker_vpn = os.environ.get('SOLACE_VPN')
    assert broker_vpn, "SOLACE_VPN environment variable is not set"

    broker_username = os.environ.get('SOLACE_USERNAME')
    assert broker_username, "SOLACE_USERNAME environment variable is not set"

    broker_password = os.environ.get('SOLACE_PASSWORD')
    assert broker_password, "SOLACE_PASSWORD environment variable is not set"

    broker_cert_dir = os.environ.get('SOLACE_CERT')
    assert broker_cert_dir, "SOLACE_CERT environment variable is not set"
    assert os.path.isdir(broker_cert_dir), "SOLACE_CERT directory does not exist"

    broker_props = {
        "solace.messaging.transport.host": broker_host,
        "solace.messaging.service.vpn-name": broker_vpn,
        "solace.messaging.authentication.scheme.basic.username": broker_username,
        "solace.messaging.authentication.scheme.basic.password": broker_password
    }

    transport_security = TLS.create() \
        .with_certificate_validation(True, 
                                     validate_server_name=False, 
                                     trust_store_file_path=broker_cert_dir)

    # Create a MessagingService object using the broker_props, a reconnection retry strategy, 
    # and the transport_security object and connects to the Solace cloud broker.
    messaging_service = MessagingService.builder().from_properties(broker_props) \
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3)) \
            .with_transport_security_strategy(transport_security) \
            .build()

    service_event_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(service_event_handler)
    messaging_service.add_reconnection_attempt_listener(service_event_handler)
    messaging_service.add_service_interruption_listener(service_event_handler)

    messaging_service.connect()
    # TODO add exception handing. Most of the connection challenges should be handled here.
    
    print(f'Connected to BIAN Solace event cloud broker: {broker_host} using: {broker_vpn} VPN, username: {broker_username}')
    
    return messaging_service


