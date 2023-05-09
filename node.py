import logging
import threading
import time
import os
import socket
import csv
import random
import json
from enum import Enum
# BEGIN code diambil dari node_socket.py assignment1
class NodeSocket:
    def __init__(self, socket_kind: socket.SocketKind, port: int = 0):
        sc = socket.socket(socket.AF_INET, socket_kind)
        sc.bind(('127.0.0.1', port))
        self.sc = sc
class UdpSocket(NodeSocket):

    def __init__(self, port: int = 0):
        super(UdpSocket, self).__init__(socket.SOCK_DGRAM, port)

    def listen(self):
        input_value_byte, address = self.sc.recvfrom(1024)
        return input_value_byte.decode("UTF-8"), address

    @staticmethod
    def send(message: str, port: int = 0):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_socket.sendto(message.encode("UTF-8"), ("127.0.0.1", port))
        client_socket.close()
# END code dari node_socket.py assignment1

def thread_exception_handler(args):
    logging.error(f"Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

class MessageType(Enum):
    SYN = 1 
    ACK = 2 


class RingNode:

    ACK = 'ACK'

    def __init__(self, node_id: int, port: int, active_nodes: list, fault_duration: int, is_continue: bool,
                 heartbeat_duration: float, leader: int):
        self.node_id = node_id
        self.port = port
        self.active_nodes = active_nodes
        self.active_nodes_port = active_nodes_ports
        self.fault_duration = fault_duration
        self.heartbeat_duration = heartbeat_duration
        self.leader = leader
        self.socket = UdpSocket(self.port)
        pass
        
    def start_heartbeat_listen(self,fault_duration):
        raw_msg,address = self.socket.listen()
        message = json.loads(raw_msg)
        assert type(message) == dict
        if message['type'] == MessageType.ACK and message['receiver_id'] == self.node_id:
            logging.info(f"[HEARTBEAT] finished for node {message['sender_id']}: ACTIVE ")
        elif message['type'] == MessageType.SYN:
            raw_response = {
                'sender_id' : self.node_id,
                'target_id' : message['sender_id'],
                'type' : MessageType.ACK
            }
            response_msg = json.dumps(raw_response)
            self.socket.send(response_msg,address)

    def start_heartbeat(self):
        while True:
            position = self.active_nodes.index(self.node_id)
            num_nodes = len(self.active_nodes)
            prev_neighbor_id = (position - 1) % num_nodes
            prev_neighbor_port = self.active_nodes[prev_neighbor_id]
            raw_message = {
                'sender_id' : self.node_id,
                'target_id' : prev_neighbor_id,
                'type' : MessageType.SYN
            }
            message = json.dumps(raw_message)
            logging.info(f"[HEARTBEAT] sending heartbeat to node_{prev_neighbor_id}")
            self.socket.send(message,prev_neighbor_port)
        


    def become_candidate(self):
        pass

    def start(self):
        logging.info(f"Node {self.node_id} is starting...")
        logging.info("Initiating heartbeat listen thread")
        heartbeat_listen_thread = threading.Thread(target=self.start_heartbeat_listen)


def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.INFO)

def main(heartbeat_duration=1, fault_duration=1, port=1000, active_nodes=[],
         node_id=1, active_nodes_ports=[], leader=1):
    reload_logging_windows(f"logs/node{node_id}.txt")
    threading.excepthook = thread_exception_handler
    try:
        leader = node_id
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"fault_duration: {fault_duration}")
        logging.debug(f"port: {port}")
        logging.debug(f"active_nodes: {active_nodes_ports}")

        logging.info("Create node...")
        node = RingNode(node_id, port, active_nodes, fault_duration, active_nodes_ports, heartbeat_duration, leader)

        logging.info("Execute node.start()...")
        node.start()
    except Exception:
        logging.exception("Caught Error")
        raise
