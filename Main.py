import argparse
import multiprocessing
import pdb
import time
import grpc
import bank_pb2
import bank_pb2_grpc
import json
import jsbeautifier
import re

from concurrent import futures

from Branch import Branch
from Customer import execute_customer_request
from utilities import configure_logger, get_operation_name, get_result_name, get_source_type_name, get_system_free_tcp_port, get_json_data

logger = configure_logger("main")

# argparse refer https://docs.python.org/3/howto/argparse.html
def get_args():
    parser = argparse.ArgumentParser(description="Input and output files")
    parser.add_argument("-i", "--input", required=False, type=str, default="input.json", help="Input file name wiht json format")
    parser.add_argument("-o", "--output", required=False, type=str, default="output.json",help="Output file name wiht json format")
    args = parser.parse_args()
    return args.input.strip(), args.output.strip()

def create_branch_input_data_collection(input_file):
    branches_data = []
    customers_events_data = []
    input_json_data = get_json_data(input_file)
    for data in input_json_data:
        if data["type"] == "branch":
            branch = {}
            branch["id"] = data["id"]
            branch["balance"] = data["balance"]
            port = get_system_free_tcp_port()
            bind_address = "[::]:{}".format(port)
            branch["bind_address"] = bind_address
            branches_data.append(branch)
        if data["type"] == "customer":
            customers_events_data.append(data)
    return branches_data, customers_events_data

def branch_service(branch, branches_bind_addresses, output):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1,),)
    bank_pb2_grpc.add_BankServicer_to_server(branch, server)
    server.add_insecure_port(branches_bind_addresses[branch.id])
    server.start()
    logger.info("Branch {} started with balance {} and is litsening on TCP port {}".format(
        branch.id, branch.balance, 
        branches_bind_addresses[branch.id].split(":")[-1]
        )
    )
    #Add delay and make sure all of customer events executed before export events
    time.sleep(15)
    #Export the event data to share dict
    output.append(branch.export())
    server.wait_for_termination()

def reorg_event_output(output):
    ##Sort output and convert output to list
    result = sorted(output, key=lambda x: x["pid"])
    events = [event for item in output for event in item["data"]]
    event_ids = set([event["id"] for event in events])
    for id in event_ids:
        single_event_result = {}
        single_event_result["eventid"] = id
        data = []
        for event in events:
            if event["id"] == id:
                data.append({"clock": event["clock"], "name": event["name"]})
        data = sorted(data, key=lambda x: x["clock"])
        single_event_result["data"] = data
        result.append(single_event_result)
    return result


def main():
    input_file, output_file = get_args()
    branches_data, customers_data = create_branch_input_data_collection(input_file)
    list_of_branches_id = [branch["id"] for branch in branches_data]
    branches_bind_addresses = {branch["id"]:branch["bind_address"] for branch in branches_data}
    manager = multiprocessing.Manager()
    branch_output = manager.list()
    #creating the branch instance
    branches_instances = []
    for branch in branches_data:
        branch_object = Branch(
            id=branch["id"], 
            balance=branch["balance"], 
            branches=list_of_branches_id, 
            bind_addresses=branches_bind_addresses
        )
        branches_instances.append(branch_object)
    
    workers = []
    for instance in branches_instances:
        branch_worker = multiprocessing.Process(
            name="Branch-{}".format(instance.id),
            target=branch_service,
            args=(instance, branches_bind_addresses, branch_output,),
        )
        branch_worker.start()
        workers.append(branch_worker)
    #Add delay to let branch to be ready for serve customer
    time.sleep(1)
    for customer in customers_data:
        customer_id = customer["id"]
        logger.info("Customer {} starting to execute events".format(customer_id))
        execute_customer_request(customer_id, branches_bind_addresses[customer_id], customer["events"])
    
    #Wait for branch export event to output
    for _ in range(10):
        if len(branch_output) < 3:
            logger.info("Wait for 1 second!")
            time.sleep(1)
        elif len(branch_output) == 3:
            logger.info("Exporting event data:")
            break
    events_output = reorg_event_output(branch_output)
    json_output = json.dumps(events_output, indent = 4)
    pdb.set_trace()
    #with open("output_project_2.json", "w") as file:
    #    file.write(json_output)
    re_format_json = ""
    for line in json_output.splitlines():
        if re.match("\s{12,17}.*", line):
            line = line.replace("\n", "")
        re_format_json += line
    
    with open("dump.json", "a") as file:
        file.write(re_format_json)
        
    for worker in workers:
        worker.terminate()

if __name__ == "__main__":
    main()