import sys
import socket
import json
import threading
import time
import sqlite3
from sqlite3 import Error
import datetime
from datetime import timedelta

# Constants
SERVER_PORT_NUM = 12345
CONNECTION_TRIALS = 3
CONNECTION_TRIALS_COOLDOWN = 1
HOST_NAME = 'localhost'
CONTINENTAL_GROUPS = 6
SUPER_PEER_LIST = []
SLEEP_TIMER = 1
DATABASE_NAME = 'registration.db'


##################
# CUSTOM CLASSES #
##################

class SuperPeer:
    """
    Super Peer Class

    Description:
        - Class used to hold a Super Peer's information

    Member Variables:
        - _group: Super peer's continental group number
        - _port_number: Super peer's port number
        - _clock_time: Super peer's current clock time
        - _election_count: Number of elections taken so far
        - _name: Super peer's name
    """

    def __init__(self, group, port_number, election_count, name):
        """The init simply sets up an empty SuperPeer class"""

        self._group = group
        self._port_number = port_number
        self._election_count = election_count
        self._name = name


class ServerDateTime:
    """
    ServerDateTime Class

    Description:
         - Class used to hold the server's time

    Member Variables:
        - string _dateTime: Registration Server's datetime
    """

    def __init__(self, start_day, start_month, start_year, start_time):
        """Initializes ServerDateTime"""

        self._dateTime = datetime.datetime(year=start_year, month=start_month, day=start_day, hour=start_time)

    def advance_time(self):
        """Advances Time"""

        advance_single_hour = timedelta(hours=1)
        advance_next_day = timedelta(hours=15)
        advance_full_day = timedelta(days=1)

        # Advance by 1 hour
        self._dateTime += advance_single_hour

        # Advance by 1 day
        if self._dateTime.hour > 16:
            self._dateTime += advance_next_day

            # If advance into saturday, skip the weekend
            if self._dateTime.weekday() == 5:
                self._dateTime += advance_full_day
                self._dateTime += advance_full_day

    def get_time(self):
        """Returns current datetime in a string tuple"""

        return self._dateTime.strftime("%-m/%-d/%Y"), self._dateTime.strftime("%H:%M")

    def print_time(self):
        """Prints current datetime for debugging"""

        # print('Current datetime is: ' + self._dateTime.strftime("%A, %m/%d/%Y %H:%M"))
        pass


###############
# TIME THREAD #
###############

class TimeThread(threading.Thread):
    """
    Time Thread Class

    Description:
        - Handles sending time updates to all the super peers

    Member Variables:
        - Nothing
    """

    def __init__(self, start_day, start_month, start_year, start_hour):
        threading.Thread.__init__(self)
        self._start_day = start_day
        self._start_month = start_month
        self._start_year = start_year
        self._start_hour = start_hour

    def run(self):
        global SLEEP_TIMER

        # We start at 7AM because we apply advance time right away
        server_datetime = ServerDateTime(self._start_day, self._start_month, self._start_year, self._start_hour)

        # Increments the global timer and sends it to all the super peers
        while True:
            server_datetime.advance_time()
            (server_date, server_time) = server_datetime.get_time()
            server_datetime.print_time()
            update_time_database(server_date, server_time)
            send_time_update(server_date, server_time)
            time.sleep(SLEEP_TIMER)


def send_time_update(server_date, server_time):
    """
    Description:
        - Void function that sends a JSON message to tell all the super peers to update clocks

    Args:
        - string server_date: The server's date
        - string server_time: The server's time

    Returns:
        - Nothing
    """

    global SUPER_PEER_LIST

    # Creates a message from json
    message_dict = {}
    message_dict['action'] = 'TimeUpdate'
    message_dict['serverDate'] = server_date
    message_dict['serverTime'] = server_time
    msg_string = json.dumps(message_dict)

    # For each super peer, if they are alive, open up a connection and send them a time update
    for super_peer in SUPER_PEER_LIST:
        # Will also test for the port's health
        if super_peer._port_number > 0 and test_super_peer(super_peer._port_number):
            # print('Sending time update to port number ' + str(super_peer._port_number))
            super_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            super_socket.connect(('localhost', super_peer._port_number))
            super_socket.send(msg_string.encode('ascii'))
            super_socket.close()


def update_time_database(server_date, server_time):
    """Saves the time onto the database"""

    global DATABASE_NAME

    # Get date components
    server_date_list = server_date.split('/')
    server_month = int(server_date_list[0])
    server_day = int(server_date_list[1])
    server_year = int(server_date_list[2])

    # Get hour
    server_hour = int(server_time.split(':')[0])

    # Updates and commits the changes
    db_connection = sqlite3.connect(DATABASE_NAME)
    db_cursor = db_connection.cursor()
    db_cursor.execute('''UPDATE date_time_table SET month=?, day=?, hour=?
                      WHERE year=?''', [server_month, server_day, server_hour, server_year,])
    db_connection.commit()
    db_connection.close()


##################
# SERVER PROCESS #
##################

def server_process():
    """
    Description:
        - Starts up a server-side process by creating a server socket, binds to it,
          and continually listens to incoming connections.
        - Will spawn new client threads whenever a connection successfully connects to it

    Args:
        - Nothing

    Returns:
        - Nothing
    """

    global SERVER_PORT_NUM
    global HOST_NAME

    # Creates a socket at the port number and binds to it
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST_NAME, SERVER_PORT_NUM))
    server_socket.listen(5)

    # Main Server Loop
    while True:
        # Accept message from any connected clients
        (client_socket, client_address) = server_socket.accept()

        # Receive message from client
        received_message = client_socket.recv(1024).decode('ascii')

        # Spawns a client thread to solve the message
        client_thread = ClientThread(client_socket, client_address, received_message)
        client_thread.start()


#################
# CLIENT THREAD #
#################

class ClientThread(threading.Thread):
    """
    Client Thread Class

    Description:
        - Handles actions recognition and calls the relevant functions
        - Will close the client socket afterwards

    Member Variables:
        - _client_socket: Super peer's port number
        - _client_address: Super peer's current clock time
        - _received_message: Number of elections taken so far
    """

    def __init__(self, client_socket, client_address, received_message):
        threading.Thread.__init__(self)
        self._client_socket = client_socket
        self._client_address = client_address
        self._received_message = received_message

    def run(self):

        global SUPER_PEER_LIST

        #########################
        #                       #
        # action:   Register    #
        # group:    0           #
        # name:     'PeerName'  #
        # portNum:  0           #
        #                       #
        #########################

        message_dict = json.loads(self._received_message)
        ack_dict = {}

        action = message_dict['action']

        # Handles a peer registration
        if action == 'Register':

            # The peer has this information in the original message
            continental_group = message_dict['group']
            new_port = message_dict['portNum']
            new_name = message_dict['name']

            print('Received Registration Request from port number ' + str(new_port))
            super_port_number = handle_registration(new_port, new_name, continental_group)

            # If this is the first peer, will register it as the new super peer
            if super_port_number == 0:
                # The peer has this information in the original message
                election_count = 0

                update_super_peer(new_name, new_port, continental_group, election_count)
                ack_dict['action'] = 'RegisterURSuper'
                ack_dict['elecNum'] = election_count

            # If the super peer fails to respond, will register as new super peer
            elif super_port_number == -1:
                # The peer has this information in the original message
                election_count = SUPER_PEER_LIST[continental_group]._election_count + 1

                update_super_peer(new_name, new_port, continental_group, election_count)
                ack_dict['action'] = 'RegisterURSuper'
                ack_dict['elecNum'] = election_count

            # If the new peer is the same entity as the old super peer, will keep everything the same
            elif super_port_number == -2:
                # The peer has this information in the original message
                election_count = SUPER_PEER_LIST[continental_group]._election_count + 1

                update_super_peer(new_name, new_port, continental_group, election_count)
                ack_dict['action'] = 'RegisterURSuper'
                ack_dict['elecNum'] = election_count

            # If the port number is known, will create the json string message
            else:
                ack_dict['action'] = 'RegisterOK'
                ack_dict['portNum'] = super_port_number

            ack_message = json.dumps(ack_dict)
            self._client_socket.send(ack_message.encode('ascii'))

        #########################
        #                       #
        # action:   Election    #
        # group:    0           #
        # name:     'PeerName'  #
        # portNum:  0           #
        # eleNum:   0           #
        #                       #
        #########################

        # Handles a super group election
        elif action == 'Election':

            # Fill up details
            new_name = message_dict['name']
            new_port = message_dict['portNum']
            continental_group = message_dict['group']
            election_count = message_dict['elecNum']
            print('Received Election Update from port number ' + str(new_port))

            # Finally handles the election and prints a message
            handle_message = handle_election(new_name, new_port, continental_group, election_count)

            if handle_message != 'ALL_GOOD':
                print(handle_message)
            else:
                print('Exchange ' + new_name + ' in continental group ' + str(continental_group) + ' with port number '
                    + str(new_port) + ' has gone through an election and is currently at '
                    + str(election_count) + ' elections.')

        #########################
        #                       #
        # action:   Query       #
        # group:    0           #
        #                       #
        #########################

        # Handles a super query
        elif action == 'Query':

            print('Received Query from group number ' + str(message_dict['group']))
            ack_message = handle_super_query(message_dict['group'])
            self._client_socket.send(ack_message.encode('ascii'))

        # Handles a syntax error
        else:
            print('Syntax Error, this should not happen')

        # Finally close the socket
        self._client_socket.close()


########################
# REGISTRATION HANDLER #
########################

def handle_registration(new_port, new_name, continental_group):
    """
    Description:
        - Handles new peer registration, by just returning the port number of the new super peer
        - The new node will then try to connect to the super peer by itself

    Args:
        - int continental_group: Continental group of new node

    Returns:
        - super_port: Returns the super peer's port number
        - 0: First time connecting to the continental group, caller will handle registration
        - -1: Super Peer might be dead or some other error
        - -2: New Peer is same as Super Peer
    """

    #########################
    #                       #
    # action:   registerOK  #
    # portNum:  0           #
    #                       #
    #########################

    ##############################
    #                            #
    # action:   registerURSuper  #
    # elecNum:  0                #
    #                            #
    ##############################

    global SUPER_PEER_LIST
    global CONNECTION_TRIALS

    try_num = CONNECTION_TRIALS

    # Tests for the super peer's connection, returns the port number if successful, else -1
    super_port = SUPER_PEER_LIST[continental_group]._port_number
    super_name = SUPER_PEER_LIST[continental_group]._name

    # If the new peer has the same port as the super peer, which is the case when a super peer reconnects,
    # it will just send back another registerUrSuper
    if super_port == new_port:
        # If both the port and the name are the same, then they are the same entity
        if new_name == super_name:
            print('Old Super Peer is back online')
            return -2
        # If the name is different, but the port is the same, we can assume that the old super peer is dead
        else:
            print('New Super Peer has taken over old Super Peer\'s port')
            return -1

    # Returns 0 if this is the first peer connecting to the super peer group
    if super_port == -1:
        print('Peer connecting for the first time')
        return 0

    # Tries to connect to a super peer for CONNECTION_TRIALS number of times
    while try_num > 0:
        # If the test is positive, will return the super peer's port number
        if test_super_peer(super_port):
            print('Super peer is alive!')
            return super_port
        # Otherwise it will decrement the number of tries by 1 and wait a bit
        try_num -= 1
        time.sleep(CONNECTION_TRIALS_COOLDOWN)

    # Returns -1 to indicate super peer was dead and the super group needs a new super peer
    print('Super peer is dead, need a new super peer')
    return -1


def test_super_peer(super_peer_port_number):
    """
    Description:
        - Tests for the presence of a super peer

    Args:
        - int super_peer_port_number: Super peer's port number

    Returns:
        - True: Super Peer is still alive
        - False: Super Peer is not alive and/or election is in progress
    """

    global HOST_NAME

    # Socket number must be greater than 0
    if super_peer_port_number < 0:
        return False

    # Creates a new socket and sets the timeout to 0.1 seconds
    super_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    super_socket.settimeout(0.1)

    # Tries to connect to the super peer socket
    try:
        super_socket.connect((HOST_NAME, super_peer_port_number))
    except socket.error:
        super_socket.close()
        return False

    # If it's all good, return True
    super_socket.close()
    return True


####################
# ELECTION HANDLER #
####################

def handle_election(new_name, new_port, continental_group, election_count):
    """
    Description:
        - Handles an election, by updating the super peer's list with the new information
        - Assumes that it is the latest information since it is expected to be called in the commit phase

    Args:
        - string new_name: Name of the new super peer
        - int new_port: Port number of the new super peer
        - int continental_group: Continental group of the new super peer
        - int election_count: Number of elections so far for continental group

    Returns:
        - ALL_GOOD: All good
        - Else: String with error information
    """

    global SUPER_PEER_LIST
    global CONTINENTAL_GROUPS

    # Need to test if continentalGroup is within range
    if continental_group < 0 or continental_group > CONTINENTAL_GROUPS:
        return 'New Continental Group ' + str(continental_group) + ' is not in range'

    # Tests new port to see if new port actually works
    if not test_super_peer(new_port):
        return 'New Port number ' + str(new_port) + ' is dead'

    # Tests if the election count is actually higher than what we have
    if election_count <= SUPER_PEER_LIST[continental_group]._election_count:
        return ('New Election count of ' + str(election_count) + 'is too low. Current election count is at '
               + str(SUPER_PEER_LIST[continental_group]._election_count))

    # If all test are passed, just update the super peer list
    update_super_peer(new_name, new_port, continental_group, election_count)

    return 'ALL_GOOD'


def update_super_peer(new_name, new_port, continental_group, election_count):
    """
    Description:
        - Handles a super peer update, by updating the super peer's list with the new information
        - Works for both new super peers and old super peers
        - Note: no checking!

    Args:
        - int new_name: Name of the new super peer
        - int new_port: Port number of the new super peer
        - int continental_group: Continental group of the new super peer
        - int election_count: Number of elections so far for continental group

    Returns:
        - Nothing
    """

    global SUPER_PEER_LIST
    global DATABASE_NAME

    # Update locally
    SUPER_PEER_LIST[continental_group]._name = new_name
    SUPER_PEER_LIST[continental_group]._port_number = new_port
    SUPER_PEER_LIST[continental_group]._election_count = election_count

    # Update remotely
    db_connection = sqlite3.connect(DATABASE_NAME)
    db_cursor = db_connection.cursor()
    db_cursor.execute('''UPDATE super_peers_table SET port_number=?, election_count=?, name=?
                      WHERE group_id=?''', [new_port, election_count, new_name, continental_group,])
    db_connection.commit()
    db_connection.close()


#################
# QUERY HANDLER #
#################

def handle_super_query(group):
    """
    Description:
        - Handles a super peer query by returning a JSON filled with each known super
          peer information, including port number and election count

    Args:
        - Nothing

    Returns:
        - json_string: Returns the json string filled with the information
    """

    #############################
    #                           #
    # action:   queryAck        #
    #                           #
    # superPeers:               #
    # [                         #
    #   {                       #
    #   group:    0             #
    #   name:     'PeerName'    #
    #   portNum:  0             #
    #   elecNum:  0             #
    #   },                      #
    #   {                       #
    #   group:    0             #
    #   name:     'PeerName'    #
    #   portNum:  0             #
    #   elecNum:  0             #
    #   }                       #
    #   ...                     #
    # ]                         #
    #                           #
    #############################

    global SUPER_PEER_LIST

    query_ack = {}
    super_groups = []

    query_ack['action'] = 'QueryAck'

    for super_peer in SUPER_PEER_LIST:
        super_dict = {}
        super_dict['group'] = super_peer._group
        super_dict['name'] = super_peer._name
        super_dict['portNum'] = super_peer._port_number
        super_dict['elecNum'] = super_peer._election_count

        super_groups.append(super_dict)

    query_ack['superPeers'] = super_groups

    json_string = json.dumps(query_ack)
    return json_string


#################
# SQL FUNCTIONS #
#################

def setup_database():
    """"""
    global DATABASE_NAME
    global CONTINENTAL_GROUPS

    # Connect to sqlite database
    try:
        db_connection = sqlite3.connect(DATABASE_NAME)
    except Error as e:
        print(e)
        exit(0)

    # Get a cursor
    db_cursor = db_connection.cursor()

    # Create super peers table
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS super_peers_table
                      (group_id INTEGER PRIMARY KEY, port_number INTEGER, election_count INTEGER, name text)''')

    # Create datetime table
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS date_time_table
        (year INTEGER PRIMARY KEY, month INTEGER, day INTEGER, hour INTEGER)''')

    # Sets up the super peers if they do not exist
    db_cursor.execute('''INSERT OR IGNORE INTO super_peers_table (group_id, port_number, election_count, name)
                      VALUES (0, -1, 0, 'UNIDENTIFIED'), (1, -1, 0, 'UNIDENTIFIED'), (2, -1, 0, 'UNIDENTIFIED'),
                      (3, -1, 0, 'UNIDENTIFIED'), (4, -1, 0, 'UNIDENTIFIED'), (5, -1, 0, 'UNIDENTIFIED')''')

    # Sets up the datetime if it does not exist
    db_cursor.execute('''INSERT OR IGNORE INTO date_time_table (year, month, day, hour) VALUES (2016, 1, 1, 7)''')

    # Save (commit) the changes
    db_connection.commit()

    # Grab all the contents from super peers table
    db_cursor.execute('''SELECT * FROM super_peers_table''')
    all_rows = db_cursor.fetchall()

    # Populates the Super Peer List with the saved super peers
    for i in range(CONTINENTAL_GROUPS):
        super_peer_tuple = all_rows[i]
        super_peer = SuperPeer(super_peer_tuple[0], super_peer_tuple[1], super_peer_tuple[2], super_peer_tuple[3])
        SUPER_PEER_LIST.append(super_peer)

    # Grab all the contents from date time table
    db_cursor.execute('''SELECT * FROM date_time_table''')
    all_rows = db_cursor.fetchall()

    # Start the time thread
    time_thread = TimeThread(all_rows[0][2], all_rows[0][1], all_rows[0][0], all_rows[0][3])
    time_thread.start()

    # Finally close the connection
    db_connection.close()


#################
# MAIN FUNCTION #
#################

def main():
    """Main entry point"""

    global CONTINENTAL_GROUPS
    global SUPER_PEER_LIST
    global DATABASE_NAME
    global SERVER_PORT_NUM

    if len(sys.argv) == 2 and sys.argv[1].isdigit():
        SERVER_PORT_NUM = int(sys.argv[1])

    # Sets up the database
    setup_database()

    # Start the server process
    server_process()

main()
