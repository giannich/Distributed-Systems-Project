import Node
import time
import socket
import json
import sys
import sqlite3
from threading import Timer

class Exchange(Node.Node):
    """The exchange class handle the trading logic in our system
    it is responsible for handling reservations for stocks, and executing those reservations
    
    Attributes:
        stocks: A dict containing information about the price and quantity of stocks CURRENTLY available
        reservations: An array containing reservation dicts for stocks this exchange trades
        orders: An array containing order dicts for stocks on other exchanges
        precommit_acks: A dict, keyed by order number, of sets of reservation numbers
            for which we have received precommit acks
        mutual_funds: A dict containing Mutual Fund dicts
    """
    #Timeouts
    kReservationTimeout = 10
    #Message Constants
    kExchangeAction = "exchange_action"
    kMessageBuy = "TradeMF"
    kMessageReserve = "reserve"
    kMessageReserveAck = "reserve_ack"
    kMessagePreCommit = "precommit"
    kMessagePreCommitAck = "precommit_ack"
    kMessageCommit = "commit"
    kMessageCancelRes = "cancel_reservation"
    kMessageAbortPreCommit = "cancel_precommit"
    kOrderNumber = "order_number"
    kReservationNumber = "reservation_number"
    kStocksDict = "stocks"
    #Reservation Constants
    kReservationStatus = "status"
    kReserved = "reserved"
    kPreCommit = "precommit"
    kCommitted = "committed"
    kCancelled = "cancelled"
    kReservationFailed = -1

    def __init__(self, group, name, port, registration_port, reservations = [], orders = [], mutual_funds = {}, precommit_acks = {}):
        self.reservations = reservations
        self.orders = orders
        self.mutual_funds = mutual_funds
        self.precommit_acks = precommit_acks
        self.clients = {} # dict of client ports to send response on order termination, keyed by order_number

        with open('MutualFunds.json') as mf_data:
            self.mutual_funds = json.load(mf_data)

        super().__init__(group,name,port,registration_port)

        # Gianni's Code Below

        # Connect to sqlite database
        try:
            db_connection = sqlite3.connect('data/' + self.name + '.db')
        except Error as e:
            print(e)
            exit(0)

        self.stock_prices = {}
        self.stocks = {}

        # Gets the cursor
        db_cursor = db_connection.cursor()

        # Gets information from the stock price table
        db_cursor.execute('''SELECT * FROM stock_price_table''')
        all_rows = db_cursor.fetchall()

        # Fills the stock_prices with the pricing information
        for row in all_rows:
            stock_name = row[0]
            stock_price = row[1]
            stock_date = row[2]
            stock_time = row[3]

            # Inits a new stock if it doesn't exist
            if stock_name not in self.stock_prices:
                self.stock_prices[stock_name] = {}

            # Inits a new date if it doesn't exist
            if stock_date not in self.stock_prices[stock_name]:
                self.stock_prices[stock_name][stock_date] = {}

            # Finally fills the dict
            self.stock_prices[stock_name][stock_date][stock_time] = stock_price

        # Gets information from the current quantity table
        db_cursor.execute('''SELECT * FROM stock_current_quantity_table''')
        all_rows = db_cursor.fetchall()

        # Fills the stock_dict with the current quantity
        for row in all_rows:
            stock_name = row[0]
            stock_qty = row[1]
            self.stocks[stock_name] = stock_qty

        # Finally closes the connection
        db_connection.close()

        # For Debugging purposes
        #for stock, qty in stock_dict.items():
        #    print(stock + ': ' + str(qty))
        print(self.name, " ready!")

    # Initialization Code
    def add_stocks(self, stocks):
        """ Adds the given stocks to the stocks dict for this exchange
        Any adding of stocks to the exchange should pass through this method"""
        for symbol, qty in stocks.items():
            if symbol in self.stocks:
                self.stocks[symbol] += qty
                self.update_quantity(symbol, qty)
            else:
                self.stocks[symbol] = qty

    # Routing Code
    def process_message(self, msg):
        """Implementation passed up to the Node Class for handling succesfully routed
        messages."""
        #try:
        #####################
        # 3PC client routes #
        #####################

        # Ack'd messages
        if msg[self.kExchangeAction] == self.kMessageReserve:
            reservation_result = self.reserve_stocks(msg[self.kStocksDict])
            self.send_reseravation_ack(msg["orig"], reservation_result, msg[self.kOrderNumber])
        elif msg[self.kExchangeAction] == self.kMessagePreCommit:
            precommit_result = self.precommit_reservation(msg[self.kReservationNumber])
            self.send_precommit_ack(msg["orig"], precommit_result, msg[self.kOrderNumber])
        # Unack'd messages
        elif msg[self.kExchangeAction] == self.kMessageCommit:
            self.execute_reservation(msg[self.kReservationNumber])
        elif msg[self.kExchangeAction] == self.kMessageCancelRes:
            self.cancel_reservation(msg[self.kReservationNumber])
        elif msg[self.kExchangeAction] == self.kMessageAbortPreCommit:
            self.cancel_reservation(msg[self.kReservationNumber])

        ##########################
        # 3PC coordinator routes #
        ##########################
        elif msg[self.kExchangeAction] == self.kMessageBuy:
            order_num = self.receive_buy_order(msg["data"], msg["qty"])
            self.clients[order_num] = msg["orig"]
            #report_order_ack(order_num)
        elif msg[self.kExchangeAction] == self.kMessageReserveAck:
            self.receive_reservation_response(msg[self.kOrderNumber],
                                         msg["orig"],
                                         msg[self.kReservationNumber])
        elif msg[self.kExchangeAction] == self.kMessagePreCommitAck:
            self.receive_precommit_response(msg[self.kOrderNumber],
                                       msg[self.kReservationNumber])
        else:
            print("Message not handled!!!!!\n", msg)
        #except KeyError as e:
        #    print("Missing key in message: ", msg,"\n",e)

    # Trader Client Code
    def __send_msg_client(self, msg):
        """ TODO """
        print(msg)
        s = socket.socket()
        s.connect(("localhost", msg["dest"]))
        s.send(json.dumps(msg).encode('ascii'))
        s.close()

    def __create_msg_client(self, client_port):
        msg = {}
        msg["dest"] = client_port
        return msg

    '''
    def report_order_ack(self, order_number):
        print("Reporting Order creation to client")
        client_port = self.clients[order_number]
        msg = __create_msg_client(client_port)
        msg[self.kExchangeAction] = "ack"
        msg[self.kOrderNumber] = order_number
        __send_msg_client(msg)
    '''

    def report_order_fail(self, order_number):
        print("Reporting Order failure to client")
        client_port = self.clients[order_number]
        msg = self.__create_msg_client(client_port)
        msg[self.kExchangeAction] = "TradeMFAck"
        msg["result"] = "Timeout"
        msg[self.kOrderNumber] = order_number
        self.__send_msg_client(msg)

    def report_order_success(self, order_number):
        print("Reporting Order success to client")
        client_port = self.clients[order_number]
        msg = self.__create_msg_client(client_port)
        msg[self.kExchangeAction] = "TradeMFAck"
        msg["result"] = "OK"
        msg[self.kOrderNumber] = order_number
        self.__send_msg_client(msg)

    #######################################
    # Three Phase Commit Coordinator Code #
    #######################################

    def receive_buy_order(self, requested_fund_name, requested_qty):
        """ Send reservation requests to each exchange as needed. """
        time.sleep(1)
        print(self.name, " received buy order for ", requested_qty, " ",requested_fund_name)
        if requested_fund_name not in self.mutual_funds:
            print("Invalid mutual fund!")
            print("Options: ", self.mutual_funds)
            return 1

        # Create order - we have a valid MF to buy
        order = {} # Dict of exchange:reservation_number
        self.orders.append(order)
        order_number = len(self.orders) - 1
        for mutual_fund_name, exchanges_dict in self.mutual_funds.items():
            if mutual_fund_name == requested_fund_name:
                for exchange, stocks_dict in exchanges_dict.items():
                    if exchange == self.name:
                        print(self.name, "Reserving stocks locally: ", stocks_dict)
                        order[exchange] = self.reserve_stocks(stocks_dict)
                        # Need to call this manually, since we're skipping routing
                        self.receive_reservation_response(order_number, self.name, order[exchange])
                    else:
                        print(self.name, " sending buy request to: ", exchange, " for order ",order_number)
                        buy_msg = self.__create_buy_message(order_number, exchange, stocks_dict)
                        self.send_message(buy_msg) # Hand off message to Node class
                        order[exchange] = None
                # Set timer for receive reservation timeout
                Timer(self.kReservationTimeout, self.__abort_valid_reservations, [order_number]).start()
                return order_number

    def __create_buy_message(self, order_number, exchange, stocks_dict):
        """ Returns a message dict to send """
        msg = self.msg_route(exchange) # Grab routing data from Node Class
        msg[self.kExchangeAction] = self.kMessageReserve
        msg[self.kOrderNumber] = order_number
        msg[self.kStocksDict] = stocks_dict
        return msg

    def receive_reservation_response(self, order_number, origin, reservation_number):
        """ Called when a reservation response message is received. Once all
        reservations have either a valid reservation, a failure confirmation,
        or a timeout, the protocol will move to precommit or abort."""
        print(self.name, " received reservation response for order", order_number, " of ",reservation_number, " from ", origin)
        try:
            order_dict = self.orders[order_number]
            order_dict[origin] = reservation_number
            if reservation_number == self.kReservationFailed: # Cancel each valid reservation
                self.__abort_valid_reservations(order_number)
            # If we have valid reservations for all exchanges, send precommits
            elif all(not (v == self.kReservationFailed or v is None) 
                     for v in order_dict.values()):
                # Set timer for receive precommit acks
                self.__send_precommit_messages(order_number)
                Timer(self.kReservationTimeout, self.__abort_valid_reservations, [order_number]).start()
        except IndexError:
            print("Trying to process response for invalid order number: ", order_number)
        except KeyError:
            print("Order ", order_number, " does not contain entry for ", origin)
        
    def __abort_valid_reservations(self, order_number):
        try:
            order_dict = self.orders[order_number]
            valid_reservations_dict = {
                k: v for k,v in order_dict.items()
                if k != self.kReservationFailed and k is not None}
            for exchange, reservation_number in valid_reservations_dict.items():
                cancel_msg = self.__create_cancel_message(exchange, reservation_number)
                self.send_message(cancel_msg)
                order_dict[exchange] = self.kReservationFailed # Invalidate the reservation locally
            self.report_order_fail(order_number)
        except IndexError:
            print("Tried to abort reservations for invalid order number: ", order_number)
        
    def __abort_unacked_reservations(self, order_number):
        try:
            order_dict = self.orders[order_number]
            if any(v is None or v is self.kReservationFailed for v in order_dict.values()):
                unacked_reservations_dict = {
                    k: v for k,v in order_dict.items() 
                    #if k != self.kReservationFailed}
                    if k != self.kReservationFailed and k is not None}
                print("Cancelling reservations on timeout: ",unacked_reservations_dict)
                for exchange, reservation_number in unacked_reservations_dict.items():
                    cancel_msg = self.__create_cancel_message(exchange, reservation_number)
                    self.send_message(cancel_msg)
                    order_dict[exchange] = self.kReservationFailed # Invalidate the reservation locally
                self.report_order_fail(order_number)
        except IndexError:
            print("Tried to abort reservations for invalid order number: ", order_number)

    def __create_cancel_message(self, exchange, reservation_number):
        """ Construct a message to cancel the reservation on the given exchange """
        msg = self.msg_route(exchange)
        msg[self.kExchangeAction] = self.kMessageCancelRes
        msg[self.kReservationNumber] = reservation_number
        return msg

    def __send_precommit_messages(self, order_number):
        """ Send precommit messages to each exchange in the order dict. Call
        only after all reservation responses have been received """
        print(self.name, "Sending precommits for order ", order_number, "\nOrder Dict: ", self.orders[order_number])
        try:
            # Add this order to the dict of precommit state orders
            if order_number in self.precommit_acks:
                self.precommit_acks[order_number].append(order_number)
            else:
                self.precommit_acks[order_number] = [order_number,]
            for exchange, reservation_number in self.orders[order_number].items():
                if exchange == self.name:
                    res_num = self.orders[order_number][self.name]
                    print("Precommitting local reservation: ", res_num)
                    self.reservations[res_num]["status"] = self.kPreCommit
                    self.receive_precommit_response(order_number, res_num)
                else:
                    precommit_msg = self.__create_precommit_message(exchange,
                                                               reservation_number,
                                                               order_number)
                    self.send_message(precommit_msg)
        except IndexError:
            print("Trying to precommit invalid order number: ", order_number)

    def __create_precommit_message(self, exchange, reservation_number, order_number):
        """ Create a precommit message for a reservation on a different exchange. """
        msg = self.msg_route(exchange)
        msg[self.kExchangeAction] = self.kMessagePreCommit
        msg[self.kReservationNumber] = reservation_number
        msg[self.kOrderNumber] = order_number
        return msg

    def receive_precommit_response(self, order_number, reservation_number):
        """ Called when a precommit ack is received. Once all precommits are
        received, then a final commit message is sent. if any precommits
        timeout or fail, then an abort message is sent to all exchanges."""
        print(self.name, " received precommit ack for order ", order_number, " of reservation: ", reservation_number)
        try:
            self.precommit_acks[order_number].append(reservation_number)
            if len(self.precommit_acks[order_number]) == len(self.orders[order_number]):
                self.__send_commit_messages(order_number)
                del self.precommit_acks[order_number]
                self.report_order_success(order_number)
        except KeyError:
            print("Error: Received precommit for non-precommitted order: ", order_number)
        
    def __send_commit_messages(self, order_number):
        """ Send out Commit messages to every exchange for this order number. 
        Additionally, notify the original client. """
        print("Sending final commit for order: ",order_number)
        try:
            for exchange, reservation_number in self.orders[order_number].items():
                msg = self.__create_commit_message(exchange, reservation_number)
                self.send_message(msg)
        except IndexError:
            print("Trying to Commit invalid order number: ", order_number)

    def __create_commit_message(self, exchange, reservation_number):
        """ Create final commit message for a reservation on a different exchange"""
        msg = self.msg_route(exchange)
        msg[self.kExchangeAction] = self.kMessageCommit
        msg[self.kReservationNumber] = reservation_number
        return msg

    ###########################################
    # Three Phase Commit Client Handling Code #
    ###########################################

    def reserve_stocks(self, stocks_dict):
        """ Try to reserve each stock in the given stock dict.
        If we can't reserve any single stock, then fail the whole reservation.
        Returns a reservation number corresponding to the reserved stocks. """
        time.sleep(1)
        print(self.name, " received a request to reserve: ", stocks_dict)
        print("Available stocks:\n", self.stocks)
        reservation = {}
        order_failed = False

        try:
            #Gather stocks for reservation
            for symbol, qty in stocks_dict.items():
                if not order_failed and self.stocks[symbol] >= qty:
                    reservation[symbol] = qty
                    self.stocks[symbol] -= qty
                    self.update_quantity(symbol, qty)
                else:
                    order_failed = True
                    break

            #Return all the stocks reserved so far if the order failed
            if order_failed:
                for symbol, qty in reservation.items():
                    if symbol != "status":
                        self.stocks[symbol] += qty
                        self.update_quantity(symbol, qty)
                return self.kReservationFailed # -1
            else:
                reservation[self.kReservationStatus] = self.kReserved
                self.reservations.append(reservation)
                # Set up timer to unreserve the stocks
                reservation_number = len(self.reservations) - 1
                Timer(self.kReservationTimeout, self.__timeout_reservation, [reservation_number]).start()
                return reservation_number
        except KeyError as e:
            print("Missing stock: ", e, "\nStocks:\n",self.stocks)
            return self.kReservationFailed

    def send_reseravation_ack(self, exchange, reservation_result, order_number):
        msg = self.msg_route(exchange)
        msg[self.kExchangeAction] = self.kMessageReserveAck
        msg[self.kReservationNumber] = reservation_result
        msg[self.kOrderNumber] = order_number
        self.send_message(msg)

    def precommit_reservation(self, reservation_number):
        """ Put the given reservation into the precommit state. If the reservation times out,
        then it will be executed instead of being cancelled."""
        time.sleep(1)
        print(self.name, "receiving precommit for reservation: ", reservation_number)
        try:
            if self.reservations[reservation_number][self.kReservationStatus] == self.kReserved:
                self.reservations[reservation_number][self.kReservationStatus] = self.kPreCommit
                return self.__add_to_database(reservation_number)
            else:
                print("Tried to precommit a reservation that was not reserved first!")
                return 2
        except IndexError:
            print("Invalid Reservation Number: ", reservation_number)
            return 1

    def __add_to_database(self,reservation_number):
        """ Add the precommitted reservation to the database, so it can still be executed
        in case this server goes down and needs to be recovered."""
        try:
            conn = sqlite3.connect('data/exchange.db')
            cursor = conn.cursor()
            # Prepare statement to avoid sql injection
            # SQL table will contain reservation number, and JSON string to rehydrate dict from
            sql_command = "INSERT INTO preCommit VALUES (?,?)"
            cursor.execute(sql_command,
                [reservation_number,
                 self.reservations[reservation_number],])
            conn.commit()
            conn.close()
            return 0
        except sqlite3.Error as e:
            print("SQL ERROR: ",e)
            return 1
        except IndexError:
            print("Invalid Reservation Number: ", reservation_number)
            return 2

    def send_precommit_ack(self, exchange, reservation_number, order_number):
        msg = self.msg_route(exchange)
        msg[self.kExchangeAction] = self.kMessagePreCommitAck
        msg[self.kReservationNumber] = reservation_number
        msg[self.kOrderNumber] = order_number
        self.send_message(msg)

    def execute_reservation(self, reservation_number):
        """ We need to define what executing a reservation actually does in terms of data,
        since we don't actually ask the trader for money."""
        time.sleep(1)
        print(self.name, " executing reservation number: ", reservation_number)
        try:
            reservation_status = self.reservations[reservation_number][self.kReservationStatus]
            if reservation_status == self.kCancelled:
                print("Error: tried to execute cancelled reservation")
                return 1
            elif reservation_status == self.kCommitted:
                print("Error: tried to execute same reservation twice")
                return 2
            elif reservation_status == self.kReserved:
                print("Error: tried to execute a reservation that has not been preCommitted")
                return 4
            elif reservation_status == self.kPreCommit:
                self.reservations[reservation_number][self.kReservationStatus] = self.kCommitted
                return 0
            else:
                print("Error: unknown status code in reservation: ", reservation_status)
                return 3
        except IndexError:
            print("Invalid Reservation Number: ", reservation_number)
            return 5
        except KeyError:
            print("Invalid Reservation Status Key for ", reservation_number)
            return 6

    def cancel_reservation(self, reservation_number):
        """ Cancelling a reservation will return its stocks to the pool of available stocks."""
        try:
            print("Cancelling Res#: ",reservation_number)
            print(self.reservations[reservation_number])
            reservation_status = self.reservations[reservation_number][self.kReservationStatus]
            if reservation_status == self.kReserved:
                self.__force_cancel_reservation(reservation_number)
                return 0
            elif reservation_status == self.kPreCommit:
                self.__force_cancel_reservation(reservation_number)
                return 0
            else:
                print("Tried to cancel a reservation that was already status: ",
                    reservation_status)
                return 2
        except IndexError:
            print("Trying to cancel Invalid Reservation Number: ", reservation_number)
            return 1

    def __force_cancel_reservation(self, reservation_number):
        """ Should never be called by anything other than cancel_reservation"""
        try:
            self.reservations[reservation_number][self.kReservationStatus] = self.kCancelled
            print("Res dict:\n",self.reservations[reservation_number])
            for symbol, qty in self.reservations[reservation_number].items():
                if symbol != "status":
                    self.stocks[symbol] += qty
                    self.update_quantity(symbol, qty)
        except IndexError:
            print("Invalid Reservation Number: ", reservation_number)
        
    def __timeout_reservation(self, reservation_number):
        """ Handles timeout behavior for the given reservation. What is done depends
        on the phase of the Three Phase Commit protocol the transaction is on."""
        try:
            #Will behave differently based on the phase of 3pc we're currently in
            reservation_status = self.reservations[reservation_number][self.kReservationStatus]
            if reservation_status == self.kReserved:
                self.cancel_reservation(reservation_number)
            elif reservation_status == self.kPreCommit:
                self.execute_reservation(reservation_number)
            else:
                print("Tried to timeout a reservation that was already status: ",
                    reservation_status)
        except IndexError:
            print("Invalid Reservation Number: ", reservation_number)

if __name__ == "__main__":
    group = int(sys.argv[1])
    name = sys.argv[2]
    port = int(sys.argv[3])
    reg_port = int(sys.argv[4])
    Exchange(group,name,port,reg_port)
