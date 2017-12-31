import json

class PaxosNode():
    def __init__(self):
        self.group = None
        self.name = None
        self.port = None
        self.peer_num = None
        self.election_num = 0
        self.promise = 0
        self.accepted = None

        self.isReceiving = False
        self.responses = []

    def send_to_port(self, address, port, msg, need_reply=False, timeout=5, retries=3):
        pass

    def msg_prepare(self):
        msg = {}
        msg["action"] = "Prepare"
        msg["group"] = self.group
        msg["name"] = self.name
        msg["portNum"] = self.port
        self.election_num += 1
        self.last_proposal = self.election_num*100 + self.peer_num
        msg["seq"] = self.last_proposal
        msg["elecNum"] = self.election_num
        return json.dumps(msg).encode()

    def msg_promise(self):
        msg = {}
        msg["action"] = "Promise"
        msg["group"] = self.group
        msg["name"] = self.name
        msg["portNum"] = self.port
        msg["accepted"] = self.accepted
        return json.dumps(msg).encode()

    def msg_accept(self):
        msg = {}
        msg["action"] = "Accept"
        msg["group"] = self.group
        msg["name"] = self.name
        msg["portNum"] = self.port
        msg["seq"] = self.last_proposal
        msg["elecNum"] = self.election_num
        return json.dumps(msg).encode()

    def msg_accepted(self):
        msg = {}
        msg["action"] = "Accepted"
        msg["group"] = self.group
        msg["name"] = self.name
        msg["portNum"] = self.port
        msg["accepted"] = self.accepted
        return json.dumps(msg).encode()

    def process_paxos(self, msg):
        if msg["action"] == "Prepare":
            print("Received election message: prepare.")
            if self.promise < msg["seq"]:
                self.promise = msg["seq"]
                print("\tSending out promise.")
                self.send_to_port("localhost", msg["portNum"], self.msg_promise())
        elif msg["action"] in ("Promise", "Accepted"):
            if self.isReceiving:
                self.responses.append(msg)
        elif msg["action"] == "Accept":
            print("Received election message: accept.")
            if self.promise == msg["seq"]:
                self.accepted = msg["seq"]
                self.election_num = msg["elecNum"]
                print("\tSending out accepted.")
                self.send_to_port("localhost", msg["portNum"], self.msg_accepted())
    