# -*- coding: utf-8 -*
# !/usr/bin/env python3

from socket import AF_INET, socket, SOCK_STREAM
import queue
import threading
from threading import Thread
import numpy as np
import json
from datetime import date
import sys
import pandas as pd

# event and queue
def receive(e, q):
    """Handles receiving of messages."""
    total_server_response = []
    msg_end_tag = ".$$$$"
    while True:
        try:
            recv_end = False
            server_response = client_socket.recv(BUFSIZ).decode("utf8")
            if server_response:
                if msg_end_tag in server_response:
                    server_response = server_response.replace(msg_end_tag, '')
                    recv_end = True
                total_server_response.append(server_response)
                # print(total_server_response)
                if recv_end == True:
                    server_response_message = ''.join(total_server_response)
                    data = json.loads(server_response_message)
                    # print(data)
                    q.put(data)
                    total_server_response = []
                    if e.isSet():
                        e.clear()
        except OSError:  # Possibly client has left the chat.
            break

def send(event=None):
    Thread(target=receive).start()
    while True:
        user_input = input("Action:")
        if "Logon" in user_input:
            client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Logon\"}"
        elif "Help" in user_input:
            client_msg = json.dumps({'Client': clientID, 'Status': 'Help'})
        elif "Users" in user_input:
            client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"User List\"}"
        elif "Stocks" in user_input:
            client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Stock List\"}"
        elif "Stock" in user_input:
            stock_list = user_input.split(" ")
            if "Date" in stock_list:
                client_msg = json.dumps(
                    {'Client': clientID, 'Status': 'Inquiry', 'Stock': stock_list[1], 'Date': stock_list[3]})
            elif "StartDate" in stock_list:
                if "EndDate" in stock_list:
                    client_msg = json.dumps({'Client': clientID, 'Status': 'Inquiry',
                                             'Stock': stock_list[1],
                                             'StartDate': stock_list[3],
                                             'EndDate': stock_list[5]})
                else:
                    client_msg = json.dumps({'Client': clientID, 'Status': 'Inquiry',
                                             'Stock': stock_list[1],
                                             'StartDate': stock_list[3],
                                             'EndDate': str(date.today())})
            else:
                client_msg = json.dumps({'Client': clientID, 'Status': 'Order Inquiry', 'Symbol': stock_list[1]})
        #   elif "Quote" in stock_list:
        #       client_msg = json.dumps({'Client':clientID, 'Status':'Quote', 'Stock':stock_list[1]})
        elif "Order" in user_input:
            order_list = user_input.split(" ")
            client_msg = json.dumps(
                {'Client': clientID, 'Status': order_list[1] + ' Order', 'Symbol': order_list[2], 'Side': order_list[3],
                 'Price': order_list[4], 'Qty': order_list[5]})
        elif "Quit" in user_input:
            client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Quit\"}"
        else:
            print("Invalid message")
            client_socket.close()
            sys.exit(0)
        client_socket.send(bytes(client_msg, "utf8"))
        data = json.loads(client_msg)
        print(data)
        if data["Status"] == "Quit":
            break

def logon():
    client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Logon\"}"
    return client_msg

def get_user_list():
    client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"User List\"}"
    return client_msg

def get_stock_list():
    client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Stock List\"}"
    return client_msg

def get_market_status():
    client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Market Status\"}"
    return client_msg

def get_order_table(stock_list):
    client_msg = json.dumps({'Client': clientID, 'Status': 'Order Inquiry', 'Symbol': stock_list})
    return client_msg

def enter_a_new_order(symbol, side, price, qty):
    client_msg = json.dumps(
        {'Client': clientID, 'Status': 'New Order', 'Symbol': symbol, 'Side': side, 'Price': price, 'Qty': qty})
    return client_msg

def quit_connection():
    client_msg = "{\"Client\":\"" + clientID + "\", \"Status\":\"Quit\"}"
    return client_msg

def send_msg(client_msg):
    client_socket.send(bytes(client_msg, "utf8"))
    data = json.loads(client_msg)
    return data

def set_event(e):
    e.set()

def wait_for_an_event(e):
    while e.isSet():
        continue

def get_data(q):
    data = q.get()
    q.task_done()
    # print(data)
    return data


# Parameter is a orderbook in json format, output is my order in json format.
# Assume the strategy is to take the best ask orders to open my position
def strategy1(order_book):
    order_table = pd.DataFrame(order_book)
    stock_list = order_table["Symbol"].drop_duplicates(keep='first').values

    myorders = []
    for stock in stock_list:
        # sellorder = order_table.loc[(order_table["Symbol"] == stock) & (order_table["Side"] == "Buy")].sort_values(
        #    by="Price", ascending=True)[-1]
        buyorder = order_table.loc[(order_table["Symbol"] == stock) & (order_table["Side"] == "Sell")]
        idxmin = buyorder["Price"].idxmin()
        myorder = buyorder.loc[idxmin].to_frame().T
        # print(buyorder)
        # myorders.append(sellorder)
        myorders.append(myorder)
    myorders = pd.concat(myorders)
    myorders["Side"] = np.where(myorders["Side"] == "Buy", "Sell", "Buy")

    myorders = myorders.to_json(orient='table')
    myorders = json.loads(myorders)["data"]
    return myorders


def check_closed(trade_book):
    open_position = trade_book.loc[trade_book["Status"] == "Open"]
    if len(open_position) > 0:
        return False
    return True


def join_trade(e, q):
    global trade_book

    threading.Thread(target=receive, args=(e, q)).start()

    # Log on
    set_event(e)
    send_msg(logon())
    wait_for_an_event(e)  # wait until all data is received and event clears
    get_data(q)

    # Get user list
    set_event(e)
    send_msg(get_user_list())
    wait_for_an_event(e)
    data = get_data(q)
    print(data)

    # Get Market status
    set_event(e)
    client_msg = get_market_status()
    send_msg(client_msg)
    wait_for_an_event(e)
    data = get_data(q)
    market_status = data["Market Status"]
    if market_status != "Open":
        print("No trade avalable")
        return

    # Get stocklist
    set_event(e)
    send_msg(get_stock_list())
    wait_for_an_event(e)
    data = get_data(q)
    stock_list = list(data['Stock List'].split(','))
    print(stock_list)

    # Get order table
    set_event(e)
    client_msg = get_order_table(stock_list)
    send_msg(client_msg)
    wait_for_an_event(e)
    data = get_data(q)
    # the order table is actually a string, need to json.load again to convert it to json format
    order_data = json.loads(data)
    order_table = order_data["data"]

    # Use a specific trading strategy to generate signals
    myorders = strategy1(order_table)
    orderindex = 1

    for order in myorders:
        set_event(e)
        client_msg = enter_a_new_order(order['Symbol'], order['Side'], order['Price'], order['Qty'])
        send_msg(client_msg)
        wait_for_an_event(e)
        data = get_data(q)

        if data["Status"] == "Order Fill":
            print("Open")
            trade_book.loc[orderindex] = [order["Symbol"], order["Side"], order["Price"], order["Qty"], "Open", None]
            orderindex += 1


    # Close Order
    closed = False
    while not closed:
        # Find open position
        open = trade_book.loc[trade_book["Status"] == "Open"]
        open_index = open.index.tolist()
        open_stocks = open["Symbol"].values.tolist()

        # Get Market status
        set_event(e)
        client_msg = get_market_status()
        send_msg(client_msg)
        wait_for_an_event(e)
        data = get_data(q)
        market_status = data["Market Status"]

        # Get Order Table
        set_event(e)
        client_msg = get_order_table(open_stocks)
        send_msg(client_msg)
        wait_for_an_event(e)
        data = get_data(q)
        # the order table is actually a string, need to json.load again to convert it to json format
        order_data = json.loads(data)
        order_table = order_data["data"]
        order_table = pd.DataFrame(order_table)

        try:
            for index in open_index:
                stock = open.loc[index, "Symbol"]
                buyorders = order_table.loc[(order_table["Symbol"] == stock) & (order_table["Side"] == "Buy")]
                idxmax = buyorders["Price"].idxmax()
                bestbuy = buyorders.loc[idxmax].to_frame().T

                # current_position = trade_book.loc[(trade_book["Symbol"] == stock)]
                bestprice = bestbuy["Price"].values[0]
                openprice = trade_book.loc[index, "OpenPrice"]
                Qty = bestbuy['Qty'].values[0]

                # When pending closing, close the order with the best price even without profit
                if market_status == "Pending Closing":
                    set_event(e)
                    client_msg = enter_a_new_order(stock, "Sell", str(bestprice), str(Qty))
                    print(client_msg)
                    send_msg(client_msg)
                    wait_for_an_event(e)
                    data = get_data(q)
                    if data["Status"] == "Order Fill":
                        print("Close")
                        trade_book.loc[index, "ClosePrice"] = bestprice
                        trade_book.loc[index, "Status"] = "Closed"
                        print(trade_book)
                # When Open, close the order only when profit is made
                elif bestprice > openprice:
                    set_event(e)
                    # Quantity may not match between open and close position, ignore it here
                    client_msg = enter_a_new_order(stock, "Sell", str(bestprice), str(Qty))
                    print(client_msg)
                    send_msg(client_msg)
                    wait_for_an_event(e)
                    data = get_data(q)
                    if data["Status"] == "Order Fill":
                        print("Close")
                        trade_book.loc[index, "ClosePrice"] = bestprice
                        trade_book.loc[index, "Status"] = "Closed"
                        print(trade_book)

        # Handle the exception caused by null order_book
        except KeyError:
            continue
        except ValueError:
            continue

        closed = check_closed(trade_book)

if (len(sys.argv) > 1):
    clientID = sys.argv[1]
else:
    clientID = "Xutao"

# HOST = input('Enter host: ')
# if not HOST:
# HOST = "192.168.1.12"
#    HOST = "10.18.188.190"
# HOST = "10.0.192.80"
# PORT = input('Enter port: ')
# if not PORT:
#    PORT = 6500
# else:
#    PORT = int(PORT)

HOST = "192.168.0.3"
PORT = 6501
BUFSIZ = 1024
ADDR = (HOST, PORT)

client_socket = None

trade_book_columns = ['OrderIndex', 'Symbol', 'Side', 'OpenPrice', 'Qty', 'Status', 'ClosePrice']
trade_book = pd.DataFrame(columns=trade_book_columns)
trade_book.set_index("OrderIndex", inplace=True)
trade_book = trade_book.fillna(0)



def trade():
    global client_socket
    global trade_book
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect(ADDR)

    # build_pair_trading_model()
    e = threading.Event()
    q = queue.Queue()
    open_thread = threading.Thread(target=join_trade, args=(e, q))

    open_thread.start()

    open_thread.join()

    trade_book.to_csv("static/cache/tradebook.csv")