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
    def __init__(self, socket_kind: socket.SocketKind, port: int = 0, timeout: int = None):
        sc = socket.socket(socket.AF_INET, socket_kind)
        sc.bind(('127.0.0.1', port))
        self.sc = sc
        self.sc.settimeout(timeout)
class UdpSocket(NodeSocket):

    def __init__(self, port: int = 0,timeout:int = None):
        super(UdpSocket, self).__init__(socket.SOCK_DGRAM, port,timeout)

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
    HEARTBEAT = 1


class RingNode:

    def __init__(self, node_id: int, port: int, active_nodes: list, fault_duration: int, active_nodes_ports: list,
                 heartbeat_duration: float, leader: int):
        self.node_id = node_id
        self.port = port
        self.active_nodes = active_nodes
        self.active_nodes_ports = active_nodes_ports
        self.fault_duration = fault_duration
        self.heartbeat_duration = heartbeat_duration
        self.leader = leader
        self.socket = UdpSocket(self.port,self.fault_duration)
        self.is_participant = False
        index = self.active_nodes.index(self.node_id)
        self.prev_node_index = (index - 1) % len(self.active_nodes)
        self.next_node_index = (index + 1) % len(self.active_nodes)
        
        
    def start_heartbeat_listen(self):
        logging.info("Heartbeat listen thread starting...")
        while True:
            try:
                raw_msg,_ = self.socket.listen()
                message = json.loads(raw_msg)
                assert type(message) == dict
                if message['type'] == "HEARTBEAT":
                    logging.info(f"[HEARTBEAT] Got heartbeat from node_{message['sender_id']}")
                
                elif message['type'] == "LEADER_ELECTION":
                    logging.info(f"[LEADER_ELECTION] Got leader election message from node_{message['sender_id']}")
                    if self.leader in self.active_nodes:
                        self.set_leader_as_none()

                    if message['candidate_id'] > self.node_id:
                        logging.info(f"[LEADER_ELECTION] Candidate id is {message['candidate_id']} which is higher than me. Forwarding to next node...")
                        message['sender_id'] = self.node_id
                        self.send_to_next_node(message)

                    elif message['candidate_id'] == self.node_id:
                        logging.info(f"[LEADER_ELECTION] Message have returned back to me with my id, i am the leader!")
                        self.leader = self.node_id
                        raw_msg_sent = {
                            'sender_id' : self.node_id,
                            'type': "UPDATE_LEADER",
                            'leader_id' : self.leader
                        }
                        logging.info("[UPDATE_LEADER] Propagating update leader message to next node")
                        self.send_to_next_node(raw_msg_sent)

                    elif not self.is_participant:
                        self.is_participant = True
                        raw_msg_sent = {
                            'sender_id' : self.node_id,
                            'type': "LEADER_ELECTION",
                            'candidate_id' : self.node_id
                        }
                        next_node_id = self.active_nodes[self.next_node_index]
                        logging.info(f"[LEADER_ELECTION] I've voted. Forwarding leader election message to node_{next_node_id}")
                        self.send_to_next_node(raw_msg_sent)

                    else:
                        logging.info(f"[LEADER_ELECTION] Already participated in leader election, message discarded")

                elif message['type'] == 'UPDATE_LEADER':
                    self.is_participant = False
                    if self.node_id != message['leader_id']:
                        logging.info(f"[UPDATE_LEADER] Update leader message received. Leader now is node_{message['leader_id']}")
                        self.leader = message['leader_id']
                        logging.info(f"[UPDATE_LEADER] Propagating leader message to next node...")
                        message['sender_id'] = self.node_id
                        self.send_to_next_node(message)
                    else:
                        logging.info(f"[UPDATE_LEADER] Message have come back to me. All active nodes leader are updated!")
            except socket.timeout as e:
                if not self.is_participant:
                    self.is_participant = True
                    logging.info("[HEARTBEAT_TIMEOUT] Haven't received heartbeat from previous node")
                    if self.active_nodes[self.prev_node_index] == self.leader:
                        logging.info("[LEADER_ELECTION] Leader timeout. Starting leader election...")
                        self.set_leader_as_none()
                        raw_msg_sent = {
                            'sender_id' : self.node_id,
                            'type': "LEADER_ELECTION",
                            'candidate_id' : self.node_id
                        }
                        next_node_id = self.active_nodes[self.next_node_index]
                        logging.info(f"[LEADER_ELECTION] I've voted. Forwarding leader election message to node_{next_node_id}")
                        self.send_to_next_node(raw_msg_sent)
                    else:
                        logging.info(f"nih {self.active_nodes[self.prev_node_index]}")
                        logging.info(f"nih {self.leader}")

    def set_leader_as_none(self):
        leader_index = self.active_nodes.index(self.leader)
        self.active_nodes.pop(leader_index)
        self.active_nodes_ports.pop(leader_index)
        self.update_prev_and_next_node_index()
        self.leader = None

    def update_prev_and_next_node_index(self):
        self.prev_node_index = (self.active_nodes.index(self.node_id) - 1) % len(self.active_nodes)
        self.next_node_index = (self.active_nodes.index(self.node_id) + 1) % len(self.active_nodes)

    def send_to_next_node(self,raw_msg,offset=0):
        msg = json.dumps(raw_msg)
        index = (self.next_node_index + offset) % len(self.active_nodes)
        dest_port = self.active_nodes_ports[index]
        self.socket.send(msg,dest_port)

    def start_heartbeat(self):
        logging.info("Heartbeat send thread starting...")
        while True:
            time.sleep(self.heartbeat_duration)
            position = self.active_nodes.index(self.node_id)
            num_nodes = len(self.active_nodes)
            next_neighbor_index = (position + 1) % num_nodes
            next_neighbor_id = self.active_nodes[next_neighbor_index]
            next_neighbor_port = self.active_nodes_ports[next_neighbor_index]
            logging.info(f"Sending to node_{next_neighbor_id} with port {next_neighbor_port}")
            raw_message = {
                'sender_id' : self.node_id,
                'type' : "HEARTBEAT"
            }
            message = json.dumps(raw_message)
            self.socket.send(message,next_neighbor_port)
        


    def become_candidate(self):
        pass

    def start(self):
        logging.info(f"Node {self.node_id} is starting...")
        logging.info("Initiating heartbeat send thread")
        heartbeat_send_thread = threading.Thread(target=self.start_heartbeat)
        heartbeat_send_thread.start()
        logging.info("Initiating heartbeat listen thread")
        heartbeat_listen_thread = threading.Thread(target=self.start_heartbeat_listen)
        heartbeat_listen_thread.start()



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
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"fault_duration: {fault_duration}")
        logging.debug(f"port: {port}")
        logging.debug(f"active_nodes: {active_nodes_ports}")

        logging.info("Create node...")
        node = RingNode(node_id, port, active_nodes, fault_duration, active_nodes_ports, heartbeat_duration, leader)

        logging.info("Execute node.start()...")
        logging.info(f"LEADERNYA {leader}")
        node.start()
    except Exception:
        logging.exception("Caught Error")
        raise
