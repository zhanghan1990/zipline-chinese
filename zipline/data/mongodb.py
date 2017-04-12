#encoding:utf-8
import sys

#verion1: get all companies data from tushare and store them in Mongodb
import pymongo
import datetime
import tushare as ts
import time
import json
import pandas as pd
from collections import OrderedDict
import pytz
import types 
import requests
from io import BytesIO, StringIO
import os
import click
import re
from os import listdir
from os.path import isfile, join
from os import walk

from pandas import DataFrame



class LoadDataCVS:

    basedir="/home/zipline/zipline"
    stockdata=basedir+"/stock data"
    indexdata=basedir+"/index data"

    #treasurvity 
    in_package_data = range(2002, 2017)
    DONWLOAD_URL = "http://yield.chinabond.com.cn/cbweb-mn/yc/downYearBzqx?year=%s&&wrjxCBFlag=0&&zblx=txy&ycDefId=%s"
    YIELD_MAIN_URL = 'http://yield.chinabond.com.cn/cbweb-mn/yield_main'


    
    def __init__(self,Ip,port):
        self.ip=Ip
        self.port=port

    ## connect to the data base
    def Conn(self):
        self.client = pymongo.MongoClient(self.ip,self.port)
        self.connection=self.client.stock #storage stock information
        self.index=self.client.index #storage index
        self.pool=self.client.pool  #storate pool
        self.treasure=self.client.treasure
        #print self.connection.collection_names()
        #print self.index.collection_names()
        #print self.pool.collection_names()
    def Close(self):
        self.client.close()
    
        
    #get the particular stock list from data base
    def getstocklist(self,kind):
        ret=[]
        if kind=="hs300":
            for t in self.pool['hz300'].find():
                ret.append(t['code'])
        if kind =="zz500":
            for t in self.pool['zz500'].find():
                ret.append(t['code'])
        if kind=='sz50':
            for t in self.pool['sz'].find():
                ret.append(t['code'])
        if kind =='st':
            for t in self.pool['st'].find():
                ret.append(t['code'])
        if kind == 'all':
            for t in self.pool['all'].find():
                ret.append(t['codes'])
            
        return ret 
        
        #get daily stock information from database
        #return dataframe which contains the information we set in the parameters

    def getstockdaily(self,code,start,end):
        total=[]
        startdate = datetime.datetime.strptime(start, "%Y-%m-%d")
        enddate=datetime.datetime.strptime(end, "%Y-%m-%d")
        series={"date":[],"open":[],"close":[],"high":[],"low":[],"volume":[],"price":[],"change":[],"code":[]}
        
        for stockdaily in self.connection[code].find({"date": {"$gte": startdate,"$lt":enddate}}).sort("date"):
            series["date"].append(stockdaily["date"])
            series["open"].append(stockdaily["open"])
            series["close"].append(stockdaily["close"])
            series["high"].append(stockdaily["high"])
            series["low"].append(stockdaily["low"])
            series["volume"].append(stockdaily["volume"])
            series["price"].append(stockdaily["adjust_price_f"])
            series["change"].append(stockdaily["change"])
            series["code"].append(stockdaily["code"])
            
        totaldata=zip(series['open'],series['high'],series['low'],series['close'],series['volume'],series["price"],series["change"],series["code"])
        df = pd.DataFrame(data=list(totaldata),index=series["date"],columns = ['open','high','low','close','volume','price','change',"code"])
        return df



    
    def getBenchamark(self,code,start,end):
        #if it is timestamp type
        startdate=start
        enddate=end
        if type(start) is types.StringType:
            startdate = datetime.datetime.strptime(start, "%Y-%m-%d")
        if type(end) is types.StringType:
            enddate=datetime.datetime.strptime(end, "%Y-%m-%d")           
        series={"date":[],"change":[]}
        for stockdaily in self.index[code].find({"date": {"$gte": startdate,"$lt":enddate}}).sort("date"):
            series["date"].append(stockdaily["date"])
            series["change"].append(stockdaily["change"])
        df=pd.Series(data=series["change"],index=series["date"])
        return df.sort_index().tz_localize('UTC')

    def getindexdaily(self,code,start,end):
        total=[]
        startdate = datetime.datetime.strptime(start, "%Y-%m-%d")
        enddate=datetime.datetime.strptime(end, "%Y-%m-%d")
        series={"date":[],"open":[],"close":[],"high":[],"low":[],"volume":[]}
        
        for stockdaily in self.index[code].find({"date": {"$gte": startdate,"$lt":enddate}}).sort("date"):
            series["date"].append(stockdaily["date"])
            series["open"].append(stockdaily["open"])
            series["close"].append(stockdaily["close"])
            series["high"].append(stockdaily["high"])
            series["low"].append(stockdaily["low"])
            series["volume"].append(stockdaily["volume"])
            
        totaldata=zip(series['date'],series['open'],series['close'],series['high'],series['low'],series['volume'])
        df = pd.DataFrame(list(totaldata))
        df.index=df.date
        return df      
    def read_treasure_from_mongodb(self,start,end):

        startdate=start
        enddate=end
        series={"Time Period":[],"1month":[],"3month":[],"6month":[],"1year":[],"2year":[],"3year":[],"5year":[],"7year":[],"10year":[],"20year":[],"30year":[]}
        if type(start) is types.StringType:
            startdate = datetime.datetime.strptime(start, "%Y-%m-%d")
        if type(end) is types.StringType:
            enddate=datetime.datetime.strptime(end, "%Y-%m-%d")
        for treasuredaily in self.treasure['treasure'].find({"Time Period": {"$gte": startdate,"$lt":enddate}}).sort("date"):
            series["Time Period"].append(treasuredaily["Time Period"])
            series["1month"].append(treasuredaily["1month"])
            series["3month"].append(treasuredaily["3month"])
            series["6month"].append(treasuredaily["6month"])
            series["1year"].append(treasuredaily["1year"])
            series["2year"].append(treasuredaily["2year"])
            series["3year"].append(treasuredaily["3year"])
            series["5year"].append(treasuredaily["5year"])
            series["7year"].append(treasuredaily["7year"])
            series["10year"].append(treasuredaily["10year"])
            series["20year"].append(treasuredaily["20year"])
            series["30year"].append(treasuredaily["30year"])
        totaldata=zip(series["1month"],series["3month"],series["6month"],series["1year"],series["2year"],series["3year"],series["5year"],series["7year"],series["10year"],series["20year"],series["30year"])
        df = pd.DataFrame(data=list(totaldata),index=series["Time Period"],columns = ['1month', '3month','6month', '1year', '2year', '3year', '5year', '7year', '10year', '20year', '30year'])
        return df.sort_index().tz_localize('UTC')
