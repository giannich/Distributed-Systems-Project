# This file contains message prototypes 

## Reserve Message (after reaching destination):
{
    "action" : "Route",
	"orig": "Euronext",
	"dest": "NYSE",
	"path": ["NYSE", "London", "Euronext"],

	"exchange_action": "reserve",
    "order_number": 42,
	"stocks": {"GOOG": 10,
		       "MSFT": 15}
}

## Reserve Ack
{
    "action" : "Route",
    "orig": "NYSE",
    "dest": "Euronext",
	"path": ["Euronext", "London", "NYSE"],

    "exchange_action": "reserve_ack",
    "order_number": 42,
    "reservation_number": 23
}

## Cancel Reservation Message
{
    "action" : "Route",
	"orig": "Euronext",
	"dest": "NYSE",
	"path": ["NYSE", "London", "Euronext"],

	"exchange_action": "cancel_reservation",
    "reservation_number": 23
}

## Precommit Message
{
    "action" : "Route",
	"orig": "Euronext",
	"dest": "NYSE",
	"path": ["NYSE", "London", "Euronext"],

	"exchange_action": "precommit",
    "order_number" : 42,
    "reservation_number": 23
}

## Precommit Ack
{
    "action" : "Route",
	"orig": "Euronext",
	"dest": "NYSE",
	"path": ["NYSE", "London", "Euronext"],

	"exchange_action": "precommit_ack",
    "order_number" : 42,
    "reservation_number": 23
}

## Commit Message
{
    "action" : "Route",
	"orig": "Euronext",
	"dest": "NYSE",
	"path": ["NYSE", "London", "Euronext"],

	"exchange_action": "commit",
    "reservation_number": 23
}

## Trade Mutual Fund Message (from client connecting to local exchange):
## If qty is positive, then it's a buy order, if qty is negative, then it's a sell order
{
    "action": "Route",
    "orig": 12345,
    "dest": "New York Stock Exchange",

    "exchange_action": "TradeMF",
    "data"  : "someName",
    "qty"   : 5
}

## Trade Mutual Fund Ack (from local exchange connecting to client):
## Accepted values for result are OK, Timeout, Fail
{
    "action": "TradeMFAck"
    "result": "Timeout"
    "total_cost": 1000
}

# Gianni: Registration Server Communications

## Registration Message (Request from node to registration server)
{
	"action": 		"Register",
    "group": 		0,
    "name": 		"exchangeName",
    "portNum: 		0
}

## Registration Message Good Ack (Returns the super peer's port number)
{
	"action": 		"RegisterOK",
    "portNum: 		0
}

## Registration Message Bad Ack (Known super peer is dead or is first peer to connect)
{
	"action": 		"RegisterURSuper",
    "elecNum":      0
}

## Election Message (Sent after election from super peer to registration server)
{
	"action": 		"Election",
    "group": 		0,
    "name": 		"exchangeName",
    "portNum: 		0,
    "electionNum": 	0
}

## Query Message (Query asking for all known super peers)
{
	"action": 		"Query",
    "group": 		0,
}

## Query Message Ack (Returns all the known super peer information, if portNum is -1, then it means no peer has registered yet)
{
	"action": 		"QueryAck",
    "groups":
    [
    	{
    	"group": 	0,
    	"name:": 	"exchangeName",
    	"portNum":	0,
    	"elecNum":	0
    	},

    	{
    	"group": 	0,
    	"name:": 	"exchangeName",
    	"portNum":	0,
    	"elecNum":	0
    	},

    	...
    ]
}

## Time Update Message (Registration server will broadcast this to all super peers)
{
    "action":       "TimeUpdate",
    "serverDate":   "1/1/16",
    "serverTime":   "8:00"
}

