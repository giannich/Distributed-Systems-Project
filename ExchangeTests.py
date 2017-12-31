import Exchange

TXT_CLR = '\033[0;36m'
OK_CLR = '\033[1;32m'
FAIL_CLR = '\033[0;31m'
NO_CLR = '\033[m'

def test_assert_equal(a, b):
    if a == b:
        print(OK_CLR, "Test Passed!", NO_CLR)
    else:
        print(FAIL_CLR, "Test Failed: Expected ", a, "But got: ", b, NO_CLR)

def print_exchange_snapshot():
       print(TXT_CLR,my_exchange.stocks,NO_CLR)

my_exchange = Exchange.Exchange()
stock_dict = {"AAPL": 50,
              "MSFT": 75,
              "AERO": 80}
my_exchange.add_stocks(stock_dict)

print_exchange_snapshot()

stocks_to_reserve = {"AAPL": 20,
                     "AERO": 100}

print(TXT_CLR,"Testing unable to reserve more stocks than available...", NO_CLR)
test_assert_equal(-1,my_exchange.reserve_stocks(stocks_to_reserve))

print_exchange_snapshot()

print(TXT_CLR,"Testing stock availability unchanged...", NO_CLR)
test_assert_equal(stock_dict, my_exchange.stocks)

valid_reservation = {"AAPL": 10,
                     "MSFT": 10,
                     "AERO": 10}

print(TXT_CLR,"Testing able to reserve stocks...", NO_CLR)
print(TXT_CLR,"Attempting to reserve ", valid_reservation)
res_num = my_exchange.reserve_stocks(valid_reservation)
test_assert_equal(0, res_num)

print_exchange_snapshot()

print(TXT_CLR,"Testing unable to execute reserved transaction...", NO_CLR)
test_assert_equal(4, my_exchange.execute_reservation(res_num))

print(TXT_CLR,"Testing precommit reservation...",NO_CLR)
test_assert_equal(0, my_exchange.precommit_reservation(res_num))

print(TXT_CLR,"Testing execute precommitted reservation...",NO_CLR)
test_assert_equal(0, my_exchange.execute_reservation(res_num))

print_exchange_snapshot()
