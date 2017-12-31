#!/bin/bash

# Teardown all python processes
echo "Terminating old exchanges..."
lsof -i | grep Python | awk -F " " '{print $2}' | xargs kill

# Teardown all restore databases
echo "Restoring all exchange backup databases..."
rm registration.db
touch registration.db
rm exchange.db
touch exchange.db

# Start Login Server
echo "Starting Registration Server..."
python3 registrationServer.py 12350&
sleep 1

# Bring Superpeers up
echo "Initializing Superpeers..."
python3 Exchange.py 0 "New York Stock Exchange" 12351 12350&
sleep 1
python3 Exchange.py 1 "Sao Paulo" 12352 12350&
sleep 1
python3 Exchange.py 2 "Euronext Paris" 12353 12350&
sleep 1
python3 Exchange.py 3 "Johannesburg" 12354 12350&
sleep 1
python3 Exchange.py 4 "Tokyo" 12355 12350&
sleep 1
python3 Exchange.py 5 "Sydney" 12356 12350&

# Wait for all superpeers to be registered
sleep 2

# Register Europe Peers
echo "Registering EU Peers..."
python3 Exchange.py 2 "Frankfurt" 12361 12350&
sleep 1
python3 Exchange.py 2 "London" 12362 12350&
sleep 1
python3 Exchange.py 2 "Brussels" 12363 12350&
sleep 1
python3 Exchange.py 2 "Lisbon" 12364 12350&
sleep 1
python3 Exchange.py 2 "Zurich" 12365 12350&
sleep 1

# Register Asia Peers
echo "Registering Asia Peers..."
python3 Exchange.py 4 "Hong Kong" 12371 12350&
sleep 1
python3 Exchange.py 4 "Shanghai" 12372 12350&
sleep 1
python3 Exchange.py 4 "Shenzhen" 12373 12350&
sleep 1
python3 Exchange.py 4 "Seoul" 12374 12350&
sleep 1
python3 Exchange.py 4 "Bombay" 12375 12350&
sleep 1

# Register North America Peers
echo "Registering NA Peer..."
python3 Exchange.py 0 "Toronto" 12381 12350&
sleep 1

sleep 2

echo "Connecting to NYSE..."
python3 customer.py 12399 12351 "New York Stock Exchange" buy bank 1

# Teardown all python processes
lsof -i | grep Python | awk -F " " '{print $2}' | xargs kill
