import copy
import json
import socket
import socketserver
import sys
import time
from collections import deque
from paxos import PaxosNode
from queue import Queue
from threading import Thread
import sqlite3
from sqlite3 import Error

# Constants
GROUP_ASIA = 0
GROUP_AMERICA = 1
GROUP_EUROPE = 2
GROUP_AFRICA = 3

class MessageHandler(socketserver.StreamRequestHandler):
    """
    Message handler class for messaging between nodes.

    It is instantiated once per connection to the server, and must
    override the handle() method for specific implementation.
    """
    def handle(self):
        q = self.server.request_queue
        msg = self.rfile.read().decode()
        if len(msg) == 0:
            return
        try:
            msg = json.loads(msg)
        except json.JSONDecodeError:
            #print("Message not sent in JSON format.")
            #print(msg)
            return
        # #print(msg)
        q.put(msg)

class Node(PaxosNode):
    '''
    Peer-to-peer structure including peer and superpeer functions, superpeer election
    and backup system through central server.
    '''
    def __init__(self, group, name, port, registration_port, handler=MessageHandler):
        PaxosNode.__init__(self)
        # General attributes
        self.group = group
        self.name = name
        self.port = port
        self.registration_port = registration_port

        self.isSuper = False
        self.node_time = None
        self.request_queue = Queue()

        # Peer attributes
        self.peer_num = None
        self.superpeer = None
        self.election = False
        self.peer_list = {}
        self.msg_num = 0
        self.msg_dict = {}

        # Superpeer attributes
        self.superpeer_list = {}
        self.max_peer_num = None

        msg_receiver = socketserver.ThreadingTCPServer(("localhost", port), handler)
        msg_receiver.request_queue = self.request_queue
        msg_receiver.node = self
        m = Thread(target=msg_receiver.serve_forever)
        m.start()
        p = Thread(target=self.process)
        p.start()

    # Superpeer functions
    def set_superpeer(self):
        self.isSuper = True
        self.max_peer_num = 0
        while not self.query_superpeers("localhost", self.registration_port):
            time.sleep(5)
        self.send_to_list("superpeer", self.msg_superpeerlist())
        if self.name in self.peer_list:
            del self.peer_list[self.name]
        self.send_to_list("peer", "registerOK")
        self.send_to_list("peer", self.msg_peerlist())

    # Query registration server for Superpeer information.
    def query_superpeers(self, address, port):
        msg = self.send_to_port(address, port, self.msg_query(), need_reply=True)
        if msg:
            for superpeer in msg["superPeers"]:
                if superpeer["portNum"] != -1:
                    self.superpeer_list[superpeer["name"]] = superpeer
                if superpeer["group"] == self.group:
                    self.election_num = superpeer["elecNum"]
            # #print(self.superpeer_list)
            #print("Query to registration server complete.")
            return True
        else:
            #print("Connection to registration server failed.")
            return False

    # Peer functions
    def register(self):
        while True:
            msg = self.send_to_port("localhost", self.registration_port, self.msg_register(), need_reply=True, timeout=10)
            if msg is None:
                print("Connection to registration server failed. Retrying...")
            else:
                break
        if msg["action"] == "RegisterOK":
            #print("Successfully connected to registration sever. Connecting to superpeer.")
            msg = self.send_to_port("localhost", msg["portNum"], self.msg_register())
        elif msg["action"] == "RegisterURSuper":
            print("Successfully connected to registration sever. Appointed to superpeer.")
            self.set_superpeer()
        else:
            print("Connection to registration server failed.")

    def connect(self, address, port, timeout=5):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            # #print((address,port))
            s.connect((address, port))
            # #print("Connection established.")
            return s
        except socket.timeout:
            print("Connect: Connection timeout.")
        except ConnectionRefusedError:
            return None

    def send_to_port(self, address, port, msg, need_reply=False, timeout=5, retries=1):
        reply = None
        fails = 0
        for i in range(retries):
            soc = self.connect(address, port, timeout)
            if soc:
                try:
                    soc.send(msg)
                    if need_reply:
                        reply = json.loads(soc.recv(1024).decode())
                    else:
                        reply = True
                    soc.close()
                    break
                except socket.timeout:
                    fails += 1
                    #print("Send_to_port: Connection timeout.")
            else:
                fails += 1
        if fails == retries:
            return None
        return reply

    def send_to_list(self, list_type, msg, timeout=5, retries=1):
        new_list = self.peer_list if list_type == "peer" else self.superpeer_list
        old_list = copy.deepcopy(new_list)
        for name in list(new_list.keys()):
            if name == self.name:
                continue
            fails = 0
            soc = None
            for i in range(retries):
                try:
                    soc = self.connect("localhost", new_list[name]["portNum"], timeout)
                except KeyError:
                    break
                if soc:
                    try:
                        if msg == "registerOK":
                            msg = self.msg_registerOK(name, new_list[name]["portNum"])
                        soc.send(msg)
                        soc.close()
                        break
                    except socket.timeout:
                        fails += 1
                        #print("Send_to_list: Connection timeout.")
                else:
                    fails += 1
            if fails == retries:
                new_list.pop(name, None)
        if new_list != old_list:
            newlist = self.msg_peerlist() if list_type == "peer" else self.msg_superpeerlist()
            self.send_to_list(list_type, newlist)

    def send_message(self, msg):
        """
        Send JSON message to destination based on the role of node
        """
        dest = msg["dest"]
        msg_json = json.dumps(msg).encode()
        if self.isSuper:
            if dest in self.peer_list:
                #print("Sending message to peer.")
                self.send_to_port("localhost", self.peer_list[dest]["port"], msg_json)
            else:
                #print("Routing message to other superpeers.")
                self.send_to_list("superpeer", msg_json)
        else:
            if self.superpeer:
                success = self.send_to_port("localhost", self.superpeer, msg_json)
                if not success:
                    print("Superpeer cannot be contacted. Election starts.")
                    self.superpeer = None
                    self.elect_superpeer()
                    self.send_message(msg)
                else:
                    print("Message sent.")
            else:
                print("No current superpeer. Election starts.")
                self.elect_superpeer()
                self.send_message(msg)

    def elect_superpeer(self):
        """
        Paxos to elect new superpeer among peers
        """
        self.election = True
        self.responses = []
        # print("\tSend prepare to all peers.")        
        self.isReceiving = True
        self.send_to_list("peer", self.msg_prepare())
        # print("\tWaiting for replies.")
        time.sleep(5)
        self.isReceiving = False
        print("\tReceived {} promise.".format(len(self.responses)))
        if len(self.responses) >= len(self.peer_list)//2:
            #print("\tPromise quorum. Sending accept request...")
            max_proposal = -1
            max_name = None
            for msg in self.responses:
                if msg["accepted"] and msg["accepted"] > max_proposal:
                    max_proposal = msg["accepted"]
                    max_name = msg["name"]
            if max_proposal == -1:
                max_name = self.name
            self.responses = []
            self.isReceiving = True
            self.send_to_list("peer", self.msg_accept())
            time.sleep(5)
            self.isReceiving = False
            #print("\tReceived {} acceptance.".format(len(self.responses)))
            if len(self.responses) >= len(self.peer_list)//2:
                #print("\tAccepted Quorum. New superpeer is {}.".format(max_name))
                if max_name == self.name:
                    #print("\tWait... I am the new superpeer!")
                    #print("\tUpdating registration server.")
                    self.send_to_port("localhost", self.registration_port, self.msg_election())
                    self.set_superpeer()
            else:
                print("\tNo accepted quorum. Election ends.")
        else:
            print("\tNo promise quorum. Election ends.")
        self.election = False

    # Message Functions
    def msg_election(self):
        """
        Message Function - Election Results Update
        """
        msg = {}
        msg["action"] = "Election"
        msg["group"] = self.group
        msg["name"] = self.name
        msg["portNum"] = self.port
        msg["elecNum"] = self.election_num
        return json.dumps(msg).encode()

    def msg_peerlist(self):
        """
        Message Function - Peer List Update
        """
        msg = {}
        msg["action"] = "PeerListUpdate"
        msg["peer_list"] = self.peer_list
        return json.dumps(msg).encode()

    def msg_superpeerlist(self):
        """
        Message Function - Superpeer List Update
        """
        msg = {}
        msg["action"] = "SuperpeerListUpdate"
        msg["superpeer_list"] = self.superpeer_list
        return json.dumps(msg).encode()

    def msg_register(self):
        """
        Message Function - Register
        """
        msg = {}
        msg["action"] = "Register"
        msg["group"] = self.group
        msg["name"] = self.name
        msg["portNum"] = self.port
        return json.dumps(msg).encode()

    def msg_registerOK(self, name, port):
        """
        Message Function - RegisterOK
        """
        msg = {}
        msg["action"] = "RegisterOK"
        msg["portNum"] = self.port
        self.max_peer_num += 1
        msg["peerNum"] = self.max_peer_num
        msg["elecNum"] = self.election_num
        self.peer_list[name] = {"portNum": port, "peerNum": self.max_peer_num}
        return json.dumps(msg).encode()

    def msg_route(self, dest):
        """
        Message Function - Route

        Create basic structure of message to be routed to destination.
        """
        msg = {}
        msg["action"] = "Route"
        msg["orig"] = self.name
        msg["path"] = self.name + " (Super)"*self.isSuper
        msg["dest"] = dest
        self.msg_num += 1
        msg["msgNum"] = self.msg_num
        msg["sendTime"] = self.node_time
        return msg

    def msg_query(self):
        """
        Message Function - Query
        """
        msg = {}
        msg["action"] = "Query"
        msg["group"] = self.group
        return json.dumps(msg).encode()

    def check_message(self, msg):
        try:
            orig = msg["orig"]
        except KeyError:
            #print("\tRoute Message does not contain orig. Discarded.")
            return False
        try:
            msg_num = msg["msgNum"]
        except KeyError:
            #print("\tClient Route Message received.")
            return True

        if orig in self.msg_dict:
            if msg_num in self.msg_dict[orig]:
                ##print("\tDuplicate Route Message. Discarded.")
                return False
            else:
                self.msg_dict[orig].append(msg_num)
        else:
            self.msg_dict[orig] = deque(maxlen=100)
            self.msg_dict[orig].append(msg_num)
            # #print(self.msg_dict)
        return True

    def process(self):
        """
        Process function for messages between nodes
        """
        while True:
            if self.isSuper:
                pass
            else:
                if not self.superpeer and not self.election:
                    self.register()
            msg = self.request_queue.get()
            try:
                action = msg["action"]
            except KeyError:
                continue
            # Time Update Messages from Server
            if action == "TimeUpdate":
                self.node_time = msg["serverTime"]
                ##print(self.node_time)
                if self.isSuper:
                    self.send_to_list("peer", json.dumps(msg).encode())

                #################
                # Gianni's Code #
                #################

                s_date = msg['serverDate']
                s_time = msg['serverTime']

                # Grabs the quantity in the quantity table for a specific date time
                db_connection = sqlite3.connect('data/' + self.name + '.db')
                db_cursor = db_connection.cursor()

                db_cursor.execute('''SELECT * FROM stock_quantity_table WHERE quantity_date=? AND quantity_time=?''', [s_date, s_time,])
                all_rows = db_cursor.fetchall()

                # Updates the current quantity table only if the new quantity is > 0
                for row in all_rows:
                    stock_name = row[0]
                    new_quantity = row[1]
                    if new_quantity > 0:
                        print('New stocks for ' + stock_name + 'at ' + self.name + ' have IPO\'d')
                        self.update_quantity(stock_name, new_quantity)

                # Finally closes the connection
                db_connection.close()

                #################
                # Gianni's Code #
                #################

            elif action == "Register":
                if self.isSuper:
                    self.send_to_port("localhost", msg["portNum"], self.msg_registerOK(msg["name"], msg["portNum"]))
                    self.send_to_list("peer", self.msg_peerlist())
                    print(self.name, "received registration from ", msg["name"])
                else:
                    print("Received registration request but not a superpeer.")
            elif action == "RegisterOK":
                print("Registration to superpeer complete.")
                # #print(msg)
                self.superpeer = msg["portNum"]
                self.peer_num = msg["peerNum"]
                self.election_num = msg["elecNum"]
            elif action == "PeerListUpdate":
                self.peer_list = msg["peer_list"]
                print("Peer list updated.")
            elif action == "SuperpeerListUpdate":
                self.superpeer_list = msg["superpeer_list"]
                #print("Superpeer list updated.")
            elif action == "Route":
                if not self.check_message(msg):
                    continue
                #print(self.name," Received message from {}".format(msg["orig"]))
                dest = msg["dest"]
                if dest == self.name:
                    #print("\tThis message is for me!")
                    self.process_message(msg)
                elif self.isSuper:
                    #print("\tMessage not for me :(. Let's send it to the right place.")
                    msg["path"] += "/" + self.name + " (Super)"
                    if dest in self.peer_list:
                        self.send_to_port("localhost", self.peer_list[dest]["portNum"], json.dumps(msg).encode())
                        #print("\tThis is for my peer. Routing message to peer.")
                    else:
                        #print("\tLet's got through my superpeer list.")
                        for superpeer in self.superpeer_list:
                            if superpeer != self.name and not superpeer in msg["path"]:
                                #print("\tRouting message to ", superpeer)
                                self.send_to_port("localhost", self.superpeer_list[superpeer]["portNum"], json.dumps(msg).encode())
            else:
                self.process_paxos(msg)

    def process_message(self, msg):
        """
        Implementation specific function to process message when delivered
        """
        pass

    #################
    # Gianni's Code #
    #################

    def update_quantity(self, stock_name, change_in_quantity):
        """Updates the quantity in the stock table"""

        # Grabs the current quantity in the stock quantity table
        db_connection = sqlite3.connect('data/' + self.name + '.db')
        db_cursor = db_connection.cursor()
        db_cursor.execute('''SELECT * FROM stock_current_quantity_table WHERE stock_name=?''', [stock_name, ])
        all_rows = db_cursor.fetchall()

        # Grabs the old price and adds the change in quantity
        old_quantity = all_rows[0][1]
        new_quantity = old_quantity + change_in_quantity

        # Finally updates the price
        db_cursor.execute('''UPDATE stock_current_quantity_table SET current_quantity=? WHERE stock_name=?''', [new_quantity, stock_name,])
        db_connection.commit()
        db_connection.close()

    #################
    # Gianni's Code #
    #################

if __name__ == "__main__":
    num = sys.argv[1]
    if num == "0":
        n = Node(0, "Gianni", 13820, 12345)
    elif num == "01":
        n = Node(0, "xxxxxx", 13821, 12345)
        time.sleep(3)
        n.send_message(n.msg_route(dest="Jordan"))
    elif num == "02":
        n = Node(0, "yyyyyy", 13823, 12345)
    elif num == "1":
        n = Node(1, "Jordan", 13822, 12345)
    elif num == "2":
        n = Node(2, "zzzzzz", 13824, 12345)
