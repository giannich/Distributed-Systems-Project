import socket
import json

SERVER_ADDRESS = 'localhost'
SERVER_PORT_NUM = 12345
TEST_PORTNUM = 12347


#########################
#                       #
# action:   Register    #
# group:    0           #
# name:     'PeerName'  #
# portNum:  0           #
#                       #
#########################

def test_register(server_address, server_port_num, group, name, port_num):

    print('In test_register')

    # Creates the json message
    msg_dict = {}

    msg_dict['action'] = 'Register'
    msg_dict['group'] = group
    msg_dict['name'] = name
    msg_dict['portNum'] = port_num

    msg_string = json.dumps(msg_dict)

    # Creates a new socket sends the message
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect((server_address, server_port_num))
    server_socket.send(msg_string.encode('ascii'))
    received_message = server_socket.recv(1024).decode('ascii')

    # Receives and interprets the message
    message_dict = json.loads(received_message)

    if message_dict['action'] == 'RegisterOK':
        print('Successfully registered with super peer at port number: ' + str(message_dict['portNum']))
    elif message_dict['action'] == 'RegisterURSuper':
        election_number = message_dict['elecNum']
        print('Successfully registered as super peer with election number ' + str(election_number))
    else:
        print('Syntax Error: This should not happen')


#########################
#                       #
# action:   Election    #
# group:    0           #
# name:     'PeerName'  #
# portNum:  0           #
# eleNum:   0           #
#                       #
#########################

def update_election(server_address, server_port_num, group, name, port_num, elec_num):

    print('In update_election')

    # Creates the json message
    msg_dict = {}

    msg_dict['action'] = 'Election'
    msg_dict['group'] = group
    msg_dict['name'] = name
    msg_dict['portNum'] = port_num
    msg_dict['elecNum'] = elec_num

    msg_string = json.dumps(msg_dict)

    # Creates a new socket sends the message
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect((server_address, server_port_num))
    server_socket.send(msg_string.encode('ascii'))


#########################
#                       #
# action:   Query       #
# group:    0           #
#                       #
#########################

def query_supers(server_address, server_port_num, group):

    print('In query_supers')

    # Creates the json message
    msg_dict = {}

    msg_dict['action'] = 'Query'
    msg_dict['group'] = group

    msg_string = json.dumps(msg_dict)

    # Creates a new socket sends the message
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect((server_address, server_port_num))
    server_socket.send(msg_string.encode('ascii'))
    received_message = server_socket.recv(1024).decode('ascii')

    # Receives and interprets the message
    message_dict = json.loads(received_message)

    print(received_message)

    if message_dict['action'] == 'QueryAck':

        group_array = message_dict['superPeers']

        for super in group_array:

            group = super['group']
            name = super['name']
            port_num = super['portNum']
            elec_num = super['elecNum']

            print('Exchange ' + name + ' in continental group ' + str(group) + ' with port number '
                  + str(port_num) + ' has gone through ' + str(elec_num) + ' elections')

    else:
        print('Syntax Error: This should not happen')


def time_test(port_num):

    # Creates a socket at the port number and binds to it
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('localhost', port_num))
    server_socket.listen(5)

    while True:
        # Accept message from any connected clients
        (client_socket, client_address) = server_socket.accept()

        # Receive message from client and load up the dict
        received_message = client_socket.recv(1024).decode('ascii')

        if len(received_message) > 0:
            # Receives and interprets the message
            message_dict = json.loads(received_message)
            action = message_dict['action']

            # If action is for time update
            if action == 'TimeUpdate':
                server_date = message_dict['serverDate']
                server_time = message_dict['serverTime']
                print('Server time is at: ' + server_date + ' ' + server_time)

def main():

    global SERVER_ADDRESS
    global SERVER_PORT_NUM
    global TEST_PORTNUM

    test_register(server_address=SERVER_ADDRESS, server_port_num=SERVER_PORT_NUM, group=5, name='Test2', port_num=TEST_PORTNUM)
    #update_election(server_address=SERVER_ADDRESS, server_port_num=SERVER_PORT_NUM, group=0, name='TestExchange2', port_num=TEST_PORTNUM, elec_num=1)
    query_supers(server_address=SERVER_ADDRESS, server_port_num=SERVER_PORT_NUM, group=0)
    #time_test(port_num=TEST_PORTNUM)

main()
