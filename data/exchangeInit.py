import csvReader
import sqlite3
from sqlite3 import Error

CSV_FILENAME_PRICE = 'price_stocks.csv'
CSV_FILENAME_QUANTITY = 'qty_stocks.csv'
TEST_EXCHANGE = 'New York Stock Exchange'
TEST_STOCK = 'Intel'


def init_database_for_exchange(database_name, exchange_name):
    """Inits the exchange by filling the database with price and quantities"""

    # Fill the dicts with data
    (stock_dict_price, stock_list) = csvReader.read_price_for_exchange(file_name=CSV_FILENAME_PRICE, exchange_name=exchange_name)
    (stock_dict_quantity, stock_list) = csvReader.read_quantity_for_exchange(file_name=CSV_FILENAME_QUANTITY, exchange_name=exchange_name)

    print(stock_list)

    # Connect to sqlite database
    try:
        db_connection = sqlite3.connect(database_name)
    except Error as e:
        print(e)
        exit(0)

    # Gets the cursor
    db_cursor = db_connection.cursor()

    # Creates stocks table
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS stock_price_table
                      (stock_name TEXT, price REAL, price_date TEXT, price_time TEXT)''')

    # Init the price list
    stock_price_inserts = []

    # Iterates through each stock in stock_dict
    for stock, date_dict in stock_dict_price.items():
        # Iterates through each date in date_dict
        for date, time_dict in date_dict.items():
            # Iterates through each time in time_dict
            for time, price in sorted(time_dict.items()):
                stock_price_inserts.append((stock, float(price), date, time))

    # Finally executes the price
    db_cursor.executemany('INSERT OR IGNORE INTO stock_price_table (stock_name, price, price_date, price_time) VALUES (?, ?, ?, ?)', stock_price_inserts)

    # Creates quantity table
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS stock_quantity_table
                      (stock_name TEXT, quantity INTEGER, quantity_date TEXT, quantity_time TEXT)''')

    # Init the qty list
    stock_qty_inserts = []

    # Iterates through each stock in stock_dict
    for stock, date_dict in stock_dict_quantity.items():
        # Iterates through each date in date_dict
        for date, time_dict in date_dict.items():
            # Iterates through each time in time_dict
            for time, qty in sorted(time_dict.items()):
                stock_qty_inserts.append((stock, int(qty), date, time))

    # Finally executes the quantity
    db_cursor.executemany('INSERT OR IGNORE INTO stock_quantity_table (stock_name, quantity, quantity_date, quantity_time) VALUES (?, ?, ?, ?)', stock_qty_inserts)

    # Creates current quantity table
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS stock_current_quantity_table
                          (stock_name TEXT PRIMARY KEY, current_quantity INTEGER)''')

    # Init the current qty list
    stock_current_qty_inserts = []

    # Iterates through each stock in stock_dict
    for stock, date_dict in stock_dict_quantity.items():
        # Only appends the very first time
        stock_current_qty_inserts.append((stock, int(date_dict['1/1/2016']['8:00'])))

    # Finally executes the current quantity
    db_cursor.executemany('INSERT OR IGNORE INTO stock_current_quantity_table (stock_name, current_quantity) VALUES (?, ?)', stock_current_qty_inserts)

    db_cursor.execute('''SELECT * FROM stock_current_quantity_table''')
    all_rows = db_cursor.fetchall()

    # Save (commit) the changes and close the connection
    db_connection.commit()
    db_connection.close()


def init_exchange(exchange_name):
    """Inits the exchange by reading from the database"""

    # Connect to sqlite database
    try:
        db_connection = sqlite3.connect(exchange_name + '.db')
    except Error as e:
        print(e)
        exit(0)

    print('Connected to London')

    stock_prices = {}
    stock_dict = {}

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
        if stock_name not in stock_prices:
            stock_prices[stock_name] = {}

        # Inits a new date if it doesn't exist
        if stock_date not in stock_prices[stock_name]:
            stock_prices[stock_name][stock_date] = {}

        # Finally fills the dict
        stock_prices[stock_name][stock_date][stock_time] = stock_price

    # Gets information from the current quantity table
    db_cursor.execute('''SELECT * FROM stock_current_quantity_table''')
    all_rows = db_cursor.fetchall()

    # Fills the stock_dict with the current quantity
    for row in all_rows:
        stock_name = row[0]
        stock_qty = row[1]
        stock_dict[stock_name] = stock_qty

    # Finally closes the connection
    db_connection.close()

    # Debug
    #for stock, qty in stock_dict.items():
    #    print(stock + ': ' + str(qty))


def update_quantity(stock_name, change_in_quantity):
    """Updates the quantity in the stock table"""

    global DATABASE_NAME

    # Grabs the current quantity in the stock quantity table
    db_connection = sqlite3.connect(DATABASE_NAME)
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


def advance_time_quantity(date, time):
    """Updates the quantity in the stock table due to natural time passing"""

    global DATABASE_NAME

    # Grabs the quantity in the quantity table for a specific date time
    db_connection = sqlite3.connect(DATABASE_NAME)
    db_cursor = db_connection.cursor()
    db_cursor.execute('''SELECT * FROM stock_quantity_table WHERE quantity_date=? AND quantity_time=?''', [date, time,])
    all_rows = db_cursor.fetchall()

    # Updates the current quantity table only if the new quantity is > 0
    for row in all_rows:
        stock_name = row[0]
        new_quantity = row[1]
        if new_quantity > 0:
            update_quantity(stock_name, new_quantity)

    # Finally closes the connection
    db_connection.close()


def print_price_table(stock_name):
    """Prints the stock price table for only a specific stock"""
    global DATABASE_NAME

    db_connection = sqlite3.connect(DATABASE_NAME)
    db_cursor = db_connection.cursor()
    db_cursor.execute('''SELECT * FROM stock_price_table WHERE stock_name=?''', [stock_name,])
    all_rows = db_cursor.fetchall()
    print(all_rows)
    db_connection.close()


def print_all_current_qty_table():
    """Prints the stock price table for only a specific stock"""
    global DATABASE_NAME

    db_connection = sqlite3.connect('London.db')
    db_cursor = db_connection.cursor()
    db_cursor.execute('''SELECT * FROM stock_current_quantity_table''')
    all_rows = db_cursor.fetchall()
    print(all_rows)
    db_connection.close()


def test_stuff():
    global DATABASE_NAME
    global TEST_STOCK

    # Gets a list of exchanges
    exchange_list = csvReader.get_exchange_list(CSV_FILENAME_PRICE)

    # Creates the db of the exchanges
    for exchange in exchange_list:
        print('Init exchangename ' + exchange)
        init_database_for_exchange(exchange + '.db', exchange)

    #init_database_for_exchange('New York Stock Exchange.db', 'New York Stock Exchange')
    #init_exchange('London')
    #print_price_table(TEST_STOCK)
    #update_quantity('IBM', 400)
    #advance_time_quantity('2/29/2016', '15:00')
    #print_all_current_qty_table()


test_stuff()