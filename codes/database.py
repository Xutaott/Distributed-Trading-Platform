import pandas as pd
import json
import urllib.request
from sqlalchemy import Column, ForeignKey, Integer, Float, String, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import aliased
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import numpy as np

Base = declarative_base()

startDate = "2010-01-01"
endDate = "2018-10-26"  #EndDate must be a trading date!!

startDate_ratio = "2010-01-01"
endDate_ratio = "2018-05-01"

startDate_k = "2018-05-01"
endDate_k = "2018-10-26"


# Table to store all stocks' history data
class Stock(Base):
    __tablename__ = 'Stocks'
    # recordId = Column(Integer, primary_key=True)
    symbol = Column(String(50), nullable=False, primary_key=True)
    date = Column(String(50), nullable=False, primary_key=True)
    openPrice = Column(Float, nullable=False)
    highPrice = Column(Float, nullable=False)
    lowPrice = Column(Float, nullable=False)
    closePrice = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)

# Table to store trading pairs' info
class Pair(Base):
    __tablename__ = "Pairs"
    pairid = Column(String(50), nullable=False, primary_key=True)
    ticker1 = Column(String(50), ForeignKey(Stock.symbol), nullable=False)
    name1 = Column(String(50), nullable=True)
    ticker2 = Column(String(50), ForeignKey(Stock.symbol), nullable=False)
    name2 = Column(String(50), nullable=True)
    correlation = Column(Float, nullable=True)
    ratio = Column(Float, nullable=True)
    k = Column(Float, nullable=True)


# Table to record real-time trading history
class TradeHistory(Base):
    __tablename__ = "TradeHistory"
    tradeid = Column(String(50), nullable=False, primary_key=True)
    tradedate = Column(String(50), nullable=False)
    tradetime = Column(String(50),nullable=False)  # In real-time trading, order may be closed before market closes
    open1 = Column(Float, nullable=False)
    close1 = Column(Float, nullable=True)
    open2 = Column(Float, nullable=False)
    close2 = Column(Float, nullable=True)
    pairdid = Column(String(50), ForeignKey(Pair.pairid))
    returns = Column(Float, nullable=True)

requestURL = "https://eodhistoricaldata.com/api/eod/"
myEodKey = "5ba84ea974ab42.45160048"

# Retrive the daily data of one symbol from startdate to enddate, the return results are data in Json format
def dailyData2(symbol, startDate, endDate, apiKey=myEodKey):
    symbolURL = str(symbol) + ".US?"
    startDateURL = "from=" + str(startDate)
    endDateURL = "to=" + str(endDate)
    apiKeyURL = "api_token=" + apiKey
    completeURL = requestURL + symbolURL + startDateURL + '&' + endDateURL + '&' + apiKeyURL + '&period=d&fmt=json'
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data


# Process Json format data of one symbol and insert it into database's Stocks table
def insert2(symbol, data, engine):
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    for record in data:
        trading_date = record["date"]
        trading_open = record['open']
        trading_high = record['high']
        trading_low = record['low']
        trading_close = record['close']
        trading_volume = record['volume']
        aStock = Stock(symbol=symbol, date=trading_date, openPrice=trading_open,
                       highPrice=trading_high, lowPrice=trading_low, closePrice=trading_close,
                       volume=trading_volume)
        session.add(aStock)
    session.commit()


# Populate the database -- Stocks table, Pairs table
def populate(df, engine):
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    # Delete the data before to avoid primary key duplicate exception
    session.query(Stock).delete()
    session.query(Pair).delete()
    session.commit()

    pairlist = []
    retrieved = [] # Avoid duplicate retriving
    for i in range(len(df)):
        pair = tuple(df.iloc[i, :])
        pairlist.append(pair)
    paidid = 1
    for (ticker1, name1, ticker2, name2, correlation) in pairlist:
        nodata = False
        if ticker1 not in retrieved:
            data1 = dailyData2(ticker1, startDate, endDate)
            if data1[-1]["date"] != endDate:  # Check whether the stock is still trading
                nodata = True
            else:
                insert2(ticker1, data1, engine)
                retrieved.append(ticker1)
                print("%s retrieved" % ticker1)
        if ticker2 not in retrieved:
            data2 = dailyData2(ticker2, startDate, endDate)
            if data2[-1]["date"] != endDate:
                nodata = True
            else:
                insert2(ticker2, data2, engine)
                retrieved.append(ticker2)
                print("%s retrieved" % ticker2)
        if not nodata:
            pair = Pair(pairid=paidid, ticker1=ticker1, name1=name1, ticker2=ticker2, name2=name2, correlation=correlation)
            session.add(pair)
            session.commit()
            paidid += 1


# Update the std of price ratio for each pair
def updateRatio(endDate, engine):
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    stalias1 = aliased(Stock)
    stalias2 = aliased(Stock)
    # Use Query to generate a dataframe
    df = session.query(Pair.pairid, Pair.ticker1, stalias1.closePrice.label("close1"), Pair.ticker2,
                       stalias2.closePrice.label("close2")) \
        .join(stalias1, Pair.ticker1 == stalias1.symbol) \
        .join(stalias2, Pair.ticker2 == stalias2.symbol) \
        .filter(stalias1.date == stalias2.date) \
        .filter(stalias1.date < endDate).all()
    df = pd.DataFrame(df)
    # Calculate the std of price ratio in dataframe
    df["ratio"] = df["close1"] / df["close2"]
    grpby = df.groupby(["pairid"])
    std = grpby.apply(lambda x: np.std(x["ratio"]))
    # Update it in the database
    for id in std.index:
        session.query(Pair).filter(Pair.pairid == id).update({Pair.ratio: std[id]})
    session.commit()


# Update the best k for each pair in the database
def updateK(bestK, engine):
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    # add_column = DDL('ALTER TABLE Pairs ADD COLUMN k FLOAT AFTER ratio')
    # engine.execute(add_column)
    for i in range(len(bestK)):
        session.query(Pair).filter(Pair.pairid == str(i + 1)).update({Pair.k: bestK[i]})
    session.commit()


# Backtest to find the best k for each pair
def findK(klist, startDate, endDate, engine):
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    stalias1 = aliased(Stock)
    stalias2 = aliased(Stock)

    ids = session.query(Pair.pairid).all()
    ratios = session.query(Pair.ratio).all()

    # Generate a dataframe for each pair
    dflist = []
    for id in ids:
        id = id[0]
        df1 = session.query(Pair.ticker1, stalias1.openPrice.label("open1"), stalias1.closePrice.label("close1"),
                            Pair.ticker2, stalias2.openPrice.label("open2"), stalias2.closePrice.label("close2"),
                            stalias1.date) \
            .join(stalias1, Pair.ticker1 == stalias1.symbol) \
            .join(stalias2, Pair.ticker2 == stalias2.symbol) \
            .filter(stalias1.date == stalias2.date) \
            .filter(and_(stalias1.date < endDate, stalias1.date > startDate)) \
            .filter(Pair.pairid == id).all()
        df1 = pd.DataFrame(df1)
        dflist.append(df1)

    # Find the best k for each dataframe
    bestklist = []
    bestpnllist = []
    for i in range(len(dflist)):
        df1 = dflist[i]
        ratio = ratios[i][0]
        bestpnl = 0
        bestk = 0
        for k in klist:
            currentpnl, df_p = pnl2(df1, k, ratio)
            if currentpnl > bestpnl:
                bestk = k
                bestpnl = currentpnl
        bestklist.append(bestk)
        bestpnllist.append(bestpnl)
    return bestklist, bestpnllist


# Backtest with selected ks and store the backtest results in database
def backtest(engine, startDate, endDate):
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    stalias1 = aliased(Stock)
    stalias2 = aliased(Stock)
    ids = session.query(Pair.pairid).all()
    ratios = session.query(Pair.ratio).all()
    ks = session.query(Pair.k).all()

    # Generate a dataframe for each pair
    dflist = []
    for id in ids:
        id = id[0]
        df1 = session.query(Pair.ticker1, stalias1.openPrice.label("open1"), stalias1.closePrice.label("close1"),
                            Pair.ticker2, stalias2.openPrice.label("open2"), stalias2.closePrice.label("close2"),
                            stalias1.date) \
            .join(stalias1, Pair.ticker1 == stalias1.symbol) \
            .join(stalias2, Pair.ticker2 == stalias2.symbol) \
            .filter(stalias1.date == stalias2.date) \
            .filter(and_(stalias1.date < endDate, stalias1.date > startDate)) \
            .filter(Pair.pairid == id).all()
        df1 = pd.DataFrame(df1)
        dflist.append(df1)

    # Calculate pnl for each pair, update the pnl
    pnllist = []
    for i in range(len(dflist)):
        df1 = dflist[i]
        ratio = ratios[i][0]
        k = ks[i][0]
        pnl,df_p = pnl2(df1, k, ratio)
        print(df_p)
        pnllist.append(pnl)
    return pnllist


# pnl function for backtesting
def pnl2(df1, k, ratio):
    df1["condition"] = np.abs(df1["close1"].shift(1) / df1["close2"].shift(1) - df1["open1"] / df1["open2"])
    df1["value"] = 10000 * (df1["close1"] - df1["open1"] - df1["open1"] / df1["open2"] * (df1["close2"] - df1["open2"]))
    df1["pnl"] = np.where(df1["condition"] > k * ratio, -df1["value"], df1["value"])
    df1 = df1[1:] # Drop the first date (No trade happens)
    totalpnl = np.nansum(df1["pnl"])
    return totalpnl,df1


if __name__ == '__main__':
    engine = create_engine('sqlite:///stocks8.db')
    df = pd.read_csv("Pairs2.csv")

    # Initialize database
    Base.metadata.create_all(engine)
    Base.metadata.bind = engine

    # start = time.time()
    populate(df,engine)
    # end = time.time()
    # print("Time to populate %s seconds"%(end-start))
    updateRatio("2018-01-01",engine)

    # Backtest to find k
    klist = np.arange(0, 2, 0.1)
    print("The range of k for testing")
    print(klist)

    bestklist, bestpnllist = findK(klist, "2018-01-01", "2018-09-01", engine)
    print("Best k for each pair")
    print(bestklist)
    print("Best pnl for each pair")
    print(bestpnllist)

    updateK(bestklist, engine)

    backtestpnl = backtest(engine, "2018-09-01", "2018-10-19")
    print("Backtest PNL:")
    print(backtestpnl)
    print(np.nansum(backtestpnl))