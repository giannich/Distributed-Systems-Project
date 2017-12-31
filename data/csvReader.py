import csv

#####################
# GET ALL EXCHANGES #
#####################

def get_exchange_list(file_name):
    """Gets a list of exchanges"""

    # Reads from the csv file
    reader = csv.DictReader(open(file_name), fieldnames='')

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)

    # dict to dict_values
    exchange_dict_values = next(reader).values()
    # dict_values to list
    exchange_list = list(exchange_dict_values)
    # list to unique list
    unique_exchange_list = list(set(exchange_list[0]))
    unique_exchange_list.remove('')
    unique_exchange_list.remove('Market')

    return unique_exchange_list


def print_exchange_list(exchange_list):
    """Simply prints all the exchanges in the exchange list"""

    for exchange in exchange_list:
        print(exchange)


####################################
# READ FOR SINGLE EXCHANGE - PRICE #
####################################

def read_price_for_exchange(file_name, exchange_name):
    """Returns an exchange dict and a list of stocks in the exchange"""

    # First round of reading to get all the stocks
    reader = csv.DictReader(open(file_name), fieldnames='')

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)
    # Skip Market
    next(reader)

    # dict to dict_values
    stock_dict_values = next(reader).values()
    # dict_values to list
    stock_list = list(stock_dict_values)
    # list to tuples
    stock_tuples = tuple(stock_list[0])

    # Second round of reading now that we know the fieldnames
    reader = csv.DictReader(open(file_name), fieldnames=stock_tuples)

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)

    # A Dict of stocks mapped to exchanges
    stock_to_exchange_dict = next(reader)

    stock_list = []
    stock_dict = {}

    # Fills the stock_list with the stock names
    for stock, exchange in stock_to_exchange_dict.items():
        if exchange == exchange_name:
            stock_list.append(stock)
            stock_dict[stock] = {}

    # Skip Stock
    next(reader)

    # Reads all the remaining rows
    for row in reader:

        # Get the date and the time
        row_date = row['Date']
        row_time = row['GMT Time']

        # For each stock, append its datetime and stock value
        for stock_name in stock_list:

            # If the date does not exist, creates the date dict
            if row_date not in stock_dict[stock_name]:
                stock_dict[stock_name][row_date] = {}

            # Finally appends the price of the stock to the time
            stock_dict[stock_name][row_date][row_time] = row[stock_name]

    return stock_dict, stock_list


def print_stock_prices_for_dict(stock_dict, stock_name):
    """Prints out the prices of a specific stock"""

    # date_dict is the value in stock_dict that has stock_name as key
    date_dict = stock_dict[stock_name]
    print('Stock is: ' + stock_name)

    # Iterates through each date in date_dict
    for date, time_dict in sorted(date_dict.items()):
        print('\tDate is: ' + date)

        # Iterates through each time in time_dict and prints out the price
        for time, price in sorted(time_dict.items()):
            print('\t\t' + time + ': ' + str(price))


#######################################
# READ FOR SINGLE EXCHANGE - QUANTITY #
#######################################

def read_quantity_for_exchange(file_name, exchange_name):
    """Returns an exchange dict and a list of stocks in the exchange"""

    # First round of reading to get all the stocks
    reader = csv.DictReader(open(file_name), fieldnames='')

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)
    # Skip Market
    next(reader)

    # dict to dict_values
    stock_dict_values = next(reader).values()
    # dict_values to list
    stock_list = list(stock_dict_values)
    # list to tuples
    stock_tuples = tuple(stock_list[0])

    # Second round of reading now that we know the fieldnames
    reader = csv.DictReader(open(file_name), fieldnames=stock_tuples)

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)

    # A Dict of stocks mapped to exchanges
    stock_to_exchange_dict = next(reader)

    stock_list = []
    stock_dict = {}

    # Fills the stock_list with the stock names
    for stock, exchange in stock_to_exchange_dict.items():
        if exchange == exchange_name:
            stock_list.append(stock)
            stock_dict[stock] = {}

    # Skip Stock
    next(reader)

    # Reads all the remaining rows
    for row in reader:

        # Get the date and the time
        row_date = row['Date']
        row_time = row['GMT Time']

        # For each stock, append its datetime and stock value
        for stock_name in stock_list:

            # If the date does not exist, creates the date dict
            if row_date not in stock_dict[stock_name]:
                stock_dict[stock_name][row_date] = {}

            # Finally appends the quantity of the stock to the time
            if row[stock_name] == '':
                stock_dict[stock_name][row_date][row_time] = 0
            else:
                stock_dict[stock_name][row_date][row_time] = row[stock_name]

    return stock_dict, stock_list


def print_stock_quantities_for_dict(stock_dict, stock_name):
    """Prints out the quantities of a specific stock"""

    # date_dict is the value in stock_dict that has stock_name as key
    date_dict = stock_dict[stock_name]
    print('Stock is: ' + stock_name)

    # Iterates through each date in date_dict
    for date, time_dict in sorted(date_dict.items()):
        print('\tDate is: ' + date)

        # Iterates through each time in time_dict and prints out the price
        for time, quantity in sorted(time_dict.items()):
            if quantity > 0:
                print('\t\t' + time + ': ' + str(quantity))


##################################
# READ FOR ALL EXCHANGES - PRICE #
##################################

def read_price_for_all_exchanges(file_name):
    """Returns a dict of exchanges and a list of all the stocks"""

    # First round of reading to get all the stocks
    reader = csv.DictReader(open(file_name), fieldnames='')

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)
    # Skip Market
    next(reader)

    # dict to dict_values
    stock_dict_values = next(reader).values()
    # dict_values to list
    stock_list = list(stock_dict_values)
    # list to tuples
    stock_tuples = tuple(stock_list[0])

    # Second round of reading now that we know the fieldnames
    reader = csv.DictReader(open(file_name), fieldnames=stock_tuples)

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)

    # A Dict of stocks mapped to exchanges
    stock_to_exchange_dict = next(reader)

    exchange_dict = {}
    all_stock_list = []

    # Iterates through the list
    for stock, exchange in stock_to_exchange_dict.items():

        # If exchange has not been initialized, creates it
        if exchange not in exchange_dict:
            exchange_dict[exchange] = {}

        # If stock has not been initialized, creates it
        if (stock not in exchange_dict[exchange]) and (stock != 'Date') and (stock != 'GMT Time'):
            all_stock_list.append(stock)
            exchange_dict[exchange][stock] = {}

    # Skip Stock
    next(reader)

    # Reads all the remaining rows
    for row in reader:

        # Get the date and the time
        row_date = row['Date']
        row_time = row['GMT Time']

        # For each stock, append its datetime and stock value
        for stock_name in all_stock_list:

            # Gets the actual exchange name
            exchange_name = stock_to_exchange_dict[stock_name]

            if row_date not in exchange_dict[exchange_name][stock_name]:
                exchange_dict[exchange_name][stock_name][row_date] = {}

            # Finally appends the price of the stock to the time
            exchange_dict[exchange_name][stock_name][row_date][row_time] = row[stock_name]

    # Deletes empty key
    del exchange_dict['']

    return exchange_dict, stock_list


def print_stock_prices_for_all(exchange_dict, stock_name):
    """Prints out the prices of a specific stock"""

    # Iterates through each exchange in exchange_dict
    for exchange, stock_dict in sorted(exchange_dict.items()):
        print('Exchange is: ' + exchange)

        # Iterates through each stock in stock_dict
        for stock, date_dict in sorted(stock_dict.items()):
            if stock == stock_name:
                print('\tStock is: ' + stock)

                # Iterates through each date in date_dict
                for date, time_dict in sorted(date_dict.items()):
                    print('\t\tDate is: ' + date)

                    # Iterates through each time in time_dict and prints out the price
                    for time, price in sorted(time_dict.items()):
                        print('\t\t\t' + time + ': ' + str(price))


#####################################
# READ FOR ALL EXCHANGES - QUANTITY #
#####################################

def read_quantity_for_all_exchanges(file_name):
    """Returns a dict of exchanges and a list of all the stocks"""

    # First round of reading to get all the stocks
    reader = csv.DictReader(open(file_name), fieldnames='')

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)
    # Skip Market
    next(reader)

    # dict to dict_values
    stock_dict_values = next(reader).values()
    # dict_values to list
    stock_list = list(stock_dict_values)
    # list to tuples
    stock_tuples = tuple(stock_list[0])

    # Second round of reading now that we know the fieldnames
    reader = csv.DictReader(open(file_name), fieldnames=stock_tuples)

    # Skip Continent
    next(reader)
    # Skip Country
    next(reader)

    # A Dict of stocks mapped to exchanges
    stock_to_exchange_dict = next(reader)

    exchange_dict = {}
    all_stock_list = []

    # Iterates through the list
    for stock, exchange in stock_to_exchange_dict.items():

        # If exchange has not been initialized, creates it
        if exchange not in exchange_dict:
            exchange_dict[exchange] = {}

        # If stock has not been initialized, creates it
        if (stock not in exchange_dict[exchange]) and (stock != 'Date') and (stock != 'GMT Time'):
            all_stock_list.append(stock)
            exchange_dict[exchange][stock] = {}

    # Skip Stock
    next(reader)

    # Reads all the remaining rows
    for row in reader:

        # Get the date and the time
        row_date = row['Date']
        row_time = row['GMT Time']

        # For each stock, append its datetime and stock value
        for stock_name in all_stock_list:

            # Gets the actual exchange name
            exchange_name = stock_to_exchange_dict[stock_name]

            if row_date not in exchange_dict[exchange_name][stock_name]:
                exchange_dict[exchange_name][stock_name][row_date] = {}

            # Finally appends the price of the stock to the time
            if row[stock_name] == '':
                exchange_dict[exchange_name][stock_name][row_date][row_time] = 0
            else:
                exchange_dict[exchange_name][stock_name][row_date][row_time] = row[stock_name]

    # Deletes empty key
    del exchange_dict['']

    return exchange_dict, stock_list


def print_stock_quantities_for_all(exchange_dict, stock_name):
    """Prints out the prices of a specific stock"""

    # Iterates through each exchange in exchange_dict
    for exchange, stock_dict in sorted(exchange_dict.items()):
        print('Exchange is: ' + exchange)

        # Iterates through each stock in stock_dict
        for stock, date_dict in sorted(stock_dict.items()):
            if stock == stock_name:
                print('\tStock is: ' + stock)

                # Iterates through each date in date_dict
                for date, time_dict in sorted(date_dict.items()):
                    print('\t\tDate is: ' + date)

                    # Iterates through each time in time_dict and prints out the quantity
                    for time, quantity in sorted(time_dict.items()):
                        if quantity > 0:
                            print('\t\t\t' + time + ': ' + str(quantity))


# Returns a list of exchanges
#exchange_list = get_exchange_list(file_name=CSV_FILENAME_PRICE)
#print_exchange_list(exchange_list)

#########
# PRICE #
#########

# Returns a dict of stocks for exchange NYSE and prints out Intel's pricing information
#(stock_dict_price, stock_list) = read_price_for_exchange(file_name=CSV_FILENAME_PRICE, exchange_name=TEST_EXCHANGE)
#print_stock_prices_for_dict(stock_dict=stock_dict_price, stock_name=TEST_STOCK)

# Returns a dict of exchanges and prints out IBM's pricing information
#(exchange_dict_price, all_stock_list) = read_price_for_all_exchanges(file_name=CSV_FILENAME_PRICE)
#print_stock_prices_for_all(exchange_dict=exchange_dict_price, stock_name=TEST_STOCK)

############
# QUANTITY #
############

# Returns a dict of stocks for exchange NYSE and prints out Intel's quantity information
#(stock_dict_quantity, stock_list) = read_quantity_for_exchange(file_name=CSV_FILENAME_QUANTITY, exchange_name=TEST_EXCHANGE)
#print_stock_quantities_for_dict(stock_dict=stock_dict_quantity, stock_name=TEST_STOCK)

# Returns a dict of exchanges and prints out IBM's pricing information
#(exchange_dict_quantity, all_stock_list) = read_quantity_for_all_exchanges(file_name=CSV_FILENAME_QUANTITY)
#print_stock_quantities_for_all(exchange_dict=exchange_dict_quantity, stock_name=TEST_STOCK)