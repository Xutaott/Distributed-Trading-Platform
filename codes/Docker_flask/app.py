from flask import Flask
from flask import render_template, request
import platform_clients
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import json

app = Flask(__name__)

engine = create_engine('sqlite:///stocks7.db')

def getToday():
    today = datetime.today()
    while True:
        if today.weekday() < 5:
            return today.strftime('%Y-%m-%d')
        today -= timedelta(days=1)

def generateTickersCSV(tickerName):
    TickersDf = pd.read_sql_table('Stocks', con=engine)
    tickerDf = TickersDf[TickersDf['symbol'] == tickerName]
    tickerDf['previousClose'] = tickerDf['closePrice'].shift(1)
    tickerDf['changePercent'] = tickerDf['closePrice'] / tickerDf['previousClose'] - 1
    tickerDf.to_csv('static/data/interday/{}.csv'.format(tickerName))

today = getToday()
startDate = '2018-01-02'

defaultTicker = 'CHU'
defaultDate = '2018-10-19'
@app.route('/')
@app.route('/index')
def index():
    # Plot need data from csv, so we need to generate a csv file first
    generateTickersCSV(defaultTicker)
    # Watch list just show all stock's info at a specific date (most recent trading day), could get data from database
    stocks = engine.execute('SELECT * FROM Stocks WHERE date = ?', (defaultDate)).fetchall()
    return render_template('index.html', plot_ticker = 'CHU', subscribe_list=stocks)


@app.route('/inter_day/', defaults={'tickerName':'CHU'})
@app.route('/inter_day/<string:tickerName>')
def interDay(tickerName):
    generateTickersCSV(tickerName)
    stocks = engine.execute('SELECT * FROM Stocks WHERE symbol = ?', (tickerName)).fetchall()
    return render_template('index.html', plot_ticker = tickerName, subscribe_list=stocks)


@app.route('/pair_analysis/', defaults={'pairId': 1})
@app.route('/pair_analysis/<int:pairId>')
def pairAnalysis(pairId):
    PairsDf = pd.read_sql_table('Pairs', con=engine)
    PairsDf.to_csv('app\\static\\cache\\pair_analysis.csv')
    pairList = engine.execute('SELECT * FROM Pairs').fetchall()
    return render_template('pair_analysis.html', pair_id = pairId, pair_list=pairList)


@app.route('/back_test', methods=['GET', 'POST'])
def backTest():
    pairList = engine.execute('SELECT * FROM Pairs').fetchall()
    pairId = request.form.get('pair_select')
    if request.method == 'POST':
        rolling_window = request.form['rolling_window']
        k = request.form['multiplier']
        print(pairId, rolling_window, k)
        """
        for i in range(10):
            startDate_ = datetime.strptime(startDate, '%Y-%m-%d')
            endDate_ = startDate + timedelta(days=rolling_window)
            #backtestPnL(k, startDate_, endDate_, pairId, rolling_window)
        """
    return render_template('pair_backtest.html', pair_list=pairList)


@app.route("/join_trade")
def orderTable():
    platform_clients.trade()
    order_table = pd.read_csv('static/cache/tradebook.csv')
    order_table["Return"] = order_table["ClosePrice"] / order_table["OpenPrice"] - 1

    table = order_table.to_json(orient="table")
    table = json.loads(table)
    table = table["data"]
    return render_template("order_table.html",order_table=table)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
