import sys
import socket
import json

HOST_NAME = 'localhost'


def trade_mutual_fund(customer_port, exchange_name, exchange_port_num, action, mutual_fund_name, quantity):
    """
    Description:
        - Void function that sends a JSON message to the local exchange to buy or sell a mutual fund

    Args:
        - int exchange_port_num: The local exchange's port number, default address is localhost
        - string action: The customer's desired action, can only be buy or sell
        - string mutual_fund_name: The target mutual fund
        - int quantity: The target quantity

    Returns:
        - Nothing
    """

    global HOST_NAME

    # Initializes the message dict and fills it with information
    message_dict = {}

    message_dict['action'] = 'Route'
    message_dict['orig'] = customer_port
    message_dict['dest'] = exchange_name
    message_dict['data'] = mutual_fund_name
    message_dict['exchange_action'] = 'TradeMF'
    message_dict['qty'] = quantity

    # Converts the message dict into a JSON string
    message_string = json.dumps(message_dict)

    # Creates a socket at the port number and binds to it
    customer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    customer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    customer_socket.bind((HOST_NAME, customer_port))
    #customer_socket.settimeout(5)

    # Tries to connect to the super peer socket
    try:
        customer_socket.connect(('localhost', exchange_port_num))
        customer_socket.send(message_string.encode('ascii'))
        customer_socket.close()
    except socket.error:
        print('Failed to connect to local exchange')
        customer_socket.close()
        return

    # Reads whatever the local exchange sends back and interprets it
    customer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    customer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    customer_socket.bind((HOST_NAME, customer_port))
    customer_socket.listen(1)
    conn, addr = customer_socket.accept()
    received_message = conn.recv(1024).decode('ascii')
    message_dict = json.loads(received_message)

    # Interprets the result
    if message_dict['exchange_action'] == 'TradeMFAck':
        if message_dict['result'] == 'OK':
            #total_cost = message_dict['total_cost']
            print('Successfully bought/sold ' + str(quantity) + ' shares of mutual fund ' + mutual_fund_name)
        elif message_dict['result'] == 'Timeout':
            print('Failed to buy/sell ' + str(quantity) + ' shares of mutual fund ' + mutual_fund_name + ' due to server timeout')
        elif message_dict['result'] == 'Fail':
            print('Failed to buy/sell ' + str(quantity) + ' shares of mutual fund ' + mutual_fund_name)
        else:
            print('Syntax Error on result: We should not receive the following message: ')
            print(received_message)
    else:
        print('Syntax Error on exchange action: We should not receive the following message: ')
        print(received_message)

    # Finally closes the socket
    customer_socket.close()
    return


def main():
    """Main Entry Point"""

    # Prints correct usage if wrong usage
    if len(sys.argv) != 7:
        print('Error: not enough arguments. There should be 6, but you only have ' + str(len(user_input_list) - 1))
        print('Usage: customer.py <customer_port_number> <exchange_port_number> <exchange_name> <action> <mutual_fund_name> <quantity>')
        return

    elif not sys.argv[1].isdigit():
        print('Error: Customer port number is not a digit')
        print('Usage: customer.py <customer_port_number> <exchange_port_number> <exchange_name> <action> <mutual_fund_name> <quantity>')
        return

    elif not sys.argv[2].isdigit():
        print('Error: Customer port number is not a digit')
        print('Usage: customer.py <customer_port_number> <exchange_port_number> <exchange_name> <action> <mutual_fund_name> <quantity>')
        return

    elif sys.argv[4] != 'buy' and sys.argv[4] != 'sell':
        print('Error: Action should be either buy or sell')
        print('Usage: customer.py <customer_port_number> <exchange_port_number> <exchange_name> <action> <mutual_fund_name> <quantity>')
        return

    elif not sys.argv[6].isdigit():
        print('Error: Quantity should be a number')
        print('Usage: customer.py <customer_port_number> <exchange_port_number> <exchange_name> <action> <mutual_fund_name> <quantity>')
        return

    # Gets the local exchange port number from the first argument
    customer_port = int(sys.argv[1])
    local_exchange_port = int(sys.argv[2])
    local_exchange_name = sys.argv[3]
    action = sys.argv[4]
    mutual_fund_name = sys.argv[5]
    quantity = int(sys.argv[6])

    print('Welcome to the trading system')
    print('Your local exchange name is ' + local_exchange_name + ' you will be connecting to port number ' + str(local_exchange_port))

    # Finally contacts the local exchange to trade the mutual fund
    trade_mutual_fund(customer_port=customer_port, exchange_name=local_exchange_name, exchange_port_num=local_exchange_port, action=action, mutual_fund_name=mutual_fund_name, quantity=quantity)

    print('Exiting customer program. Tanks for using our the trading system!')

main()
