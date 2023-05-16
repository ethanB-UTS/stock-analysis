import json
import re
import urllib.parse
import boto3
import mysql.connector
import csv
import yfinance as yf
import pandas as pd

# Establish a connection
# yfinance
# pandas


s3 = boto3.client("s3")
BUCKET_NAME = "stock-info-source"
CSV_KEY = "nasdaq_screener_1677814203706.csv"


def lambda_handler(event, context):
    """Main function called when an event triggers the lambda.
    This function gets the top 200 stocks from a file in an S3 bucket.
    Extracts top 200 stock info from the yfinace API.
    Loads the data into MySql database
    Executes queries to transform the data in the database
    """
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=CSV_KEY)
    top200_stocks = openCsv(obj)
    put(top200_stocks)
    db = mySqlDb()
    db.insertBiggestGainers()
    db.insertGainersMonthly()
    db.insertGainersWeekly()
    db.insertVolatility()
    db.insertVolatilityMonthly()
    db.insertVolatilityWeekly()


class mySqlDb:
    """MySql database class to handle the connection and execute queries in our MySql database"""

    def __init__(self):
        self.host = (
            "stock-info-dev-instance-1.clq8sv8sg7bt.ap-southeast-2.rds.amazonaws.com"
        )
        self.user = "admin"
        self.password = "admin123"
        self.database = "stock_schema"
        self.port = "3306"
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def closeConnection(self):
        self.connection.close()

    def insertRecords(self, landTableObj):
        """Inserts records into the landing table"""
        mycursor = self.connection.cursor()
        sql = "INSERT INTO landingTable (tickerCode, stockDate, openVal, highVal, lowVal, closeVal, volume, dividends) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
            landTableObj.symbol,
            landTableObj.date,
            landTableObj.openV,
            landTableObj.highV,
            landTableObj.lowV,
            landTableObj.closeV,
            landTableObj.volume,
            landTableObj.dividends,
        )
        mycursor.execute(sql, val)
        self.connection.commit()

    def truncateTable(self, tableName):
        """Used to truncate a given table via tableName"""
        mycursor = self.connection.cursor()
        sql = "truncate " + tableName
        mycursor.execute(sql)
        self.connection.commit()

    def insertInfo(self, tickerCode, industry, name, marketCap):
        """Inserts stock info data"""
        mycursor = self.connection.cursor()
        sql = "INSERT INTO tickerInfo (tickerCode, industry, stockName, marketCap) VALUES (%s, %s, %s, %s)"
        val = (tickerCode, industry, name, marketCap)
        mycursor.execute(sql, val)
        self.connection.commit()

    def insertBiggestGainers(self):
        """Inserts data into the biggestGainers table. (landingTable must be populated first)"""
        mycursor = self.connection.cursor()
        sql = """INSERT INTO biggestGainers(id,tickerCode,stockDate,gainVal, gainPercentage) 
                    SELECT id,tickerCode,stockDate,(openVal-closeVal + dividends), -((closeVal - openVal) / openVal)
                        FROM stock_schema.landingTable WHERE NOT EXISTS(SELECT id from biggestGainers)"""
        mycursor.execute(sql)
        self.connection.commit()

    def insertGainersMonthly(self):
        """Inserts data into gainersMonthly table. (biggestGainers must be populated first)"""
        self.truncateTable("gainersMonthly")
        mycursor = self.connection.cursor()
        sql = """INSERT INTO gainersMonthly(tickerCode,yearMonth,gainVal, gainPer)
                    select tickerCode, yearMonth, gainVal, gainPer from(
                SELECT concat(CAST(year(stockDate) AS CHAR), CAST(MONTH(stockDate) AS CHAR)) yearMonth, tickerCode, sum(gainVal) gainVal, sum(gainPercentage) gainPer
	                FROM biggestGainers 
	                GROUP BY tickerCode, concat(CAST(year(stockDate) AS CHAR), CAST(MONTH(stockDate) AS CHAR))
                    ORDER BY concat(CAST(year(stockDate) AS CHAR), CAST(MONTH(stockDate) AS CHAR))) as t
                WHERE NOT EXISTS(SELECT tickerCode, yearMonth from gainersMonthly);
                """
        mycursor.execute(sql)
        self.connection.commit()

    def insertGainersWeekly(self):
        """Inserts data into gainersWeekly table. (biggestGainers must be populated first)"""
        self.truncateTable("gainersMonthly")
        mycursor = self.connection.cursor()
        sql = """
            INSERT INTO gainersWeekly(tickerCode, yearWeek, gainVal, gainPer)
                select tickerCode, yearWeek, gainVal, gainPer from(
            SELECT yearweek(stockDate) yearWeek, tickerCode, sum(gainVal) gainVal, sum(gainPercentage) gainPer
                FROM biggestGainers 
                GROUP BY tickerCode, yearweek(stockDate) 
                ORDER BY yearweek(stockDate)) as t
            WHERE NOT EXISTS(SELECT tickerCode, yearWeek from gainersWeekly);
        """
        mycursor.execute(sql)
        self.connection.commit()

    def insertVolatility(self):
        """Inserts data into the volatility table. (landingTable must be populated first)"""
        mycursor = self.connection.cursor()
        sql = """
            INSERT INTO volatility(tickerCode,stockDate,volRating, volDeep)
            SELECT tickerCode,stockDate,((highVal - lowVal)/lowVal), (-((closeVal - openVal) / openVal)*((highVal - lowVal)/lowVal))
            FROM stock_schema.landingTable
            WHERE NOT EXISTS(SELECT stockDate, tickerCode from volatility)
        """
        mycursor.execute(sql)
        self.connection.commit()

    def insertVolatilityMonthly(self):
        """Inserts data into volatilityMonthly table. (volatility must be populated first)"""
        self.truncateTable("volatilityMonthly")
        mycursor = self.connection.cursor()
        sql = """
            INSERT INTO volatilityMonthly(tickerCode,yearMonth,volRating, volDeep)
            select tickerCode, yearMonth, volRating, volDeep from(
            SELECT concat(CAST(year(stockDate) AS CHAR), CAST(MONTH(stockDate) AS CHAR)) yearMonth, tickerCode, sum(volRating) volRating, sum(volDeep) volDeep
                FROM volatility 
                GROUP BY tickerCode, concat(CAST(year(stockDate) AS CHAR), CAST(MONTH(stockDate) AS CHAR))
                ORDER BY concat(CAST(year(stockDate) AS CHAR), CAST(MONTH(stockDate) AS CHAR))) as t
            WHERE NOT EXISTS(SELECT tickerCode, yearMonth from volatilityMonthly);
        """
        mycursor.execute(sql)
        self.connection.commit()

    def insertVolatilityWeekly(self):
        """Inserts data into volatilityWeekly table. (volatility must be populated first)"""
        self.truncateTable("volatilityWeekly")
        mycursor = self.connection.cursor()
        sql = """
            INSERT INTO volatilityWeekly(tickerCode, yearWeek, volRating, volDeep)
            select tickerCode, yearWeek, volRating, volDeep from(
            SELECT yearweek(stockDate) yearWeek, tickerCode, sum(volRating) volRating, sum(volDeep) volDeep
                FROM volatility 
                GROUP BY tickerCode, yearweek(stockDate) 
                ORDER BY yearweek(stockDate)) as t
            WHERE NOT EXISTS(SELECT tickerCode, yearWeek from volatilityWeekly);
        """
        mycursor.execute(sql)
        self.connection.commit()


def openCsv(csv_data):
    """Opens given csv file with pandas, returns a list of the top 200 stocks based on market cap"""
    # df = pd.read_csv("nasdaq_screener_1677814203706.csv")
    df = pd.read_csv(csv_data["Body"])
    clean_df = df.dropna()
    df_list = clean_df["Market Cap"].values.tolist()
    clean_df = clean_df[["Symbol", "Name", "Market Cap"]]
    top_200_values = sorted(df_list, reverse=True)[:200]

    top200_symbols = []
    for k, v in clean_df.iterrows():
        for top200 in top_200_values:
            if v["Market Cap"] == top200:
                top200_symbols.append(v["Symbol"])

    return top200_symbols


class apiData:
    """Class to handle the yfinace API inputs and responses"""

    def __init__(self, ticker_code):
        self.ticker_code = ticker_code

    def getStockInfo(self):
        """Returns stock info from ticker_code for the last days of data"""
        tickers = yf.Tickers(self.ticker_code)
        # try:
        #    tickers.tickers[self.ticker_code].info
        # except Exception:
        #    print(tickers.tickers[self.ticker_code].info)
        return tickers.tickers[self.ticker_code].history(period="1d")


class landingTableObj:
    """Class to represent the data structure of the MySql landing table"""

    def __init__(self, symbol, date, openV, lowV, highV, closeV, volume, dividends):
        self.symbol = str(symbol)
        self.date = str(date)
        self.openV = str(openV)
        self.lowV = str(lowV)
        self.closeV = str(closeV)
        self.volume = str(volume)
        self.dividends = str(dividends)
        self.highV = str(highV)

    def toString(self):
        print(
            "Symbol: "
            + self.symbol
            + "\nDate: "
            + self.date
            + "\nOpen: "
            + self.openV
            + "\nClose: "
            + self.closeV
            + "\nlow: "
            + self.lowV
            + "\nhigh: "
            + self.highV
            + "\nvolume: "
            + self.volume
            + "\ndividends: "
            + self.dividends
        )


def put(top200_stocks):
    """Takes the top 200 stocks ticker codes and calls the API for the ticker codes.
    Inserts the top 200 API stock data into the landing table"""
    # top200_stocks = openCsv("ethan")
    sqlObj = mySqlDb()
    for symbol in top200_stocks:
        thing = apiData(symbol)
        for k, v in thing.getStockInfo().iterrows():
            ltobj = landingTableObj(
                symbol,
                k,
                v["Open"],
                v["Low"],
                v["High"],
                v["Close"],
                v["Volume"],
                v["Dividends"],
            )

            # ltobj.toString()
            sqlObj.insertRecords(ltobj)
    sqlObj.closeConnection()


def clean_name(name):
    """Cleans a string of parts we don't want. For the ticker info table"""
    clean_string = name.split("Inc.")[0] + "Inc."
    clean_string = clean_string.replace("Common Stock", "")
    clean_string = clean_string.replace("Class A", "")
    clean_string = clean_string.replace("Ordinary Stock", "")
    clean_string = clean_string.replace("Ordinary Shares", "")
    clean_string = re.sub(r"\([^)]*\)", "", clean_string)
    clean_string = clean_string.replace("  ", " ")
    clean_string = re.sub(r"\([^)]*\)", "", clean_string)

    return clean_string


def create_ticker_info():
    """Extracts top 200 stocks and inserts relevant data into the stock info table"""
    df = pd.read_csv("nasdaq_screener_1677814203706.csv")
    clean_df = df.dropna()
    df_list = clean_df["Market Cap"].values.tolist()
    clean_df = clean_df[["Symbol", "Name", "Market Cap", "Industry"]]
    top_200_values = sorted(df_list, reverse=True)[:200]
    query = mySqlDb()
    for k, v in clean_df.iterrows():
        for top200 in top_200_values:
            if v["Market Cap"] == top200:
                cleaned_name = clean_name(str(v["Name"]))
                cleaned_industry = clean_name(str(v["Industry"]))
                query.insertInfo(
                    str(v["Symbol"]),
                    cleaned_industry,
                    cleaned_name,
                    str(v["Market Cap"]),
                )
    query.closeConnection()


"""
CREATE TABLE `stock_schema`.`gainersWeekly` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `yearWeek` VARCHAR(45) NULL,
  `gainVal` VARCHAR(45) NULL,
  `gainPer` VARCHAR(45) NULL,
  `tickerCode` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);
"""
