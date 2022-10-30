import grpc
import bank_pb2
import bank_pb2_grpc
import time
from utilities import configure_logger, get_operation_name, get_result_name, get_source_type_name
from concurrent import futures

logger = configure_logger("Branch")

class Branch(bank_pb2_grpc.BankServicer):

    def __init__(self, id, balance, branches, bind_addresses):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.events = list()
        # iterate the processID of the branches
        self.branches_bind_addresses = bind_addresses
        # local clock
        self.clock = 1

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        new_balance = 0
        op_result = bank_pb2.Result.failure
        operation_name = get_operation_name(request.operation_type)
        source_type = get_source_type_name(request.source_type)
        event_id = request.event_id
        #if request is a query, sleep 3 seconds and make sure all of propagate completed
        if request.operation_type == bank_pb2.Operation.query:
            time.sleep(3)
            new_balance = self.balance
            op_result = bank_pb2.Result.success
        #if request from customer, run propagate
        elif request.source_type == bank_pb2.Source.customer:
            self.Event_Request(operation_name, source_type, request.id, request.event_id, request.clock)
            if request.operation_type == bank_pb2.Operation.withdraw:
                op_result, new_balance = self.WithDraw(request.amount)
            elif request.operation_type == bank_pb2.Operation.deposit:
                op_result, new_balance = self.Deposit(request.amount)
            self.Event_Execute(operation_name, request.id, request.event_id)
            if op_result == bank_pb2.Result.success:
                self.Branch_Propagate(request.operation_type, request.amount, request.event_id)
                self.Event_Response(operation_name, request.id, request.event_id)
        #if request from branch, no progagate
        elif request.source_type == bank_pb2.Source.branch:
            #execute propagate request
            self.Propagate_Request(operation_name, source_type, request.id, request.event_id, request.clock)
            if request.operation_type == bank_pb2.Operation.withdraw:
                op_result, new_balance = self.WithDraw(request.amount)
            elif request.operation_type == bank_pb2.Operation.deposit:
                op_result, new_balance = self.Deposit(request.amount)
            #execute propagate execute
            self.Propagate_Execute(operation_name, request.id, request.event_id)
        #construct response
        response = bank_pb2.MsgDelivery_response(
            source_type = bank_pb2.Source.branch,
            operation_result = op_result,
            id = self.id,
            event_id = event_id,
            amount = new_balance,
            clock = self.clock,
        )
        return response

    def Deposit(self, amount):
        if amount < 0:
            return bank_pb2.Result.error, amount
        self.balance += amount
        return bank_pb2.Result.success, self.balance

    def WithDraw(self, amount):
        if amount > self.balance:
            return bank_pb2.Result.failure, amount
        self.balance -= amount
        return bank_pb2.Result.success, self.balance
    
    def Create_propagate_request(self, operation_type, amount, event_id):
        """
        Build the branch propagate request context
        """
        logger.info("Creating propagate request....")
        request = bank_pb2.MsgDelivery_request(
            operation_type = operation_type,
            source_type = bank_pb2.Source.branch,
            id = self.id,
            event_id = event_id,
            amount = amount,
            clock = self.clock,
        )
        return request

    def Create_branches_stub(self):
        """
        Create branches stub
        """
        logger.info("Creating branches stub....")
        for branch in self.branches:
            if branch != self.id:
                bind_address = self.branches_bind_addresses[branch]
                stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(bind_address))
                self.stubList.append(stub)

    def Branch_Propagate(self, operation_type, amount, event_id):
        """
        Run branches propagate
        If all of branches propagate return success, return list of branches propagate response clock
        """
        operation_name = get_operation_name(operation_type)
        if len(self.stubList) == 0:
            self.Create_branches_stub()
        for stub in self.stubList:
            propagate_request = self.Create_propagate_request(operation_type, amount, event_id)
            response = stub.MsgDelivery(propagate_request)
            logger.info("Event {} Propagate {} response from branch {}".format(
                response.event_id,
                get_result_name(response.operation_result), 
                response.id
                )
            )
            self.Propagate_Response(operation_name, response.event_id, response.clock)

    def Event_Request(self, operation_name, source_type, request_id, event_id, request_clock):
        self.clock = max(self.clock, request_clock) + 1
        operation_name = operation_name + "_request"
        self.events.append(
            {"id": event_id, "name": operation_name, "clock": self.clock}
        )
        logger.info(
            "Branch {} has received {} from {} {} event id {}, clock is {}, local clock is changing to {}".format(
                self.id, 
                operation_name, 
                source_type, 
                request_id,
                event_id,
                request_clock,
                self.clock,
            )
        )
    
    def Event_Execute(self, operation_name, request_id, event_id):
        self.clock += 1
        operation_name = operation_name + "_execute"
        self.events.append(
            {"id": event_id, "name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {} event id {}, local clock changed to {}".format(self.id, operation_name, event_id, self.clock))
    
    def Event_Response(self, operation_name, request_id, event_id):
        self.clock += 1
        operation_name = operation_name + "_response"
        self.events.append(
            {"id": event_id, "name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {} event id {}, local clock changed to {}".format(self.id, operation_name, event_id, self.clock))
    
    def Propagate_Request(self, operation_name, source_type, request_id, event_id, request_clock):
        self.clock = max(self.clock, request_clock) + 1
        operation_name = operation_name + "_propagate_request"
        self.events.append(
            {"id": event_id, "name": operation_name, "clock": self.clock}
        )
        logger.info(
            "Branch {} has received {} from {} {} event id {}, clock is {}, local clock is changing to {}".format(
                self.id, 
                operation_name, 
                source_type, 
                request_id,
                event_id,
                request_clock,
                self.clock,
            )
        )

    def Propagate_Execute(self, operation_name, request_id, event_id):
        self.clock += 1
        operation_name = operation_name + "_propagate_execute"
        self.events.append(
            {"id": event_id, "name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {} event id {}, local clock changed to {}".format(self.id, operation_name, event_id, self.clock))
    
    def Propagate_Response(self, operation_name, event_id, response_clock):
        self.clock = max(response_clock, self.clock) + 1
        operation_name = operation_name + "_propagate_response"
        self.events.append(
            {"id": event_id, "name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {} event id {}, local clock changed to {}".format(self.id, operation_name, event_id, self.clock))
    
    def export(self):
        output = {}
        output["pid"] = self.id
        output["data"] = self.events
        return output
