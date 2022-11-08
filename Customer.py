import grpc
import bank_pb2
import bank_pb2_grpc
import time
import json

from utilities import get_operation, get_source_type_name, configure_logger, get_result_name
from concurrent import futures

logger = configure_logger("Customer")

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None
        # local clock
        self.clock = 1

    # TODO: students are expected to create the Customer stub
    def createStub(self, branch_bind_address):
        self.stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(branch_bind_address))

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        for event in self.events:
            operation = get_operation(event["interface"])
            logger.info("Customer {} sending event id {} request with operation {}, amout {}".format(
                self.id, event["id"], 
                event["interface"], 
                event["money"]
                )
            )
            customer_request = bank_pb2.MsgDelivery_request(
                operation_type = operation,
                source_type = bank_pb2.Source.customer,
                id = self.id,
                event_id = event["id"],
                amount = event["money"],
                clock = self.clock
            )
            response = self.stub.MsgDelivery(customer_request)
            self.recvMsg.append(response)
            logger.info("Customer {} has recevied the response from {} id {}, event id {}, amount {}, clock {}".format(
                self.id, 
                get_source_type_name(response.source_type), 
                response.id,
                response.event_id,
                response.amount,
                response.clock
                )
            )      


def execute_customer_request(id, branch_bind_address, events):
    cust = Customer(id, events)
    cust.createStub(branch_bind_address)
    cust.executeEvents()

