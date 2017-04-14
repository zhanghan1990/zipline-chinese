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
from mongodb import LoadDataCVS
from pandas import DataFrame
import constants
# add new data and use tushare to update data
class InsertDataCVS:

    basedir=constants.basedir
    stockdata=constants.stockdata
    indexdata=constants.indexdata
    newdata=constants.newdata

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
        
        self.oriprice=self.client.stockoriginalprice #stock original price
        self.qfqprice=self.client.stockqfqprice #stock qfq price
        self.hfqprice=self.client.stockhfqprice #stock hfq price

        self.test=self.client.test





    def Close(self):
        self.client.close()


    #store data information into database, do not always call this
    def storagedaily(self):
        #get the filelist in stockdata
        onlyfiles = [ f for f in listdir(self.stockdata) if isfile(join(self.stockdata,f)) ]
        
        #read from using pandas
        alldatabase=self.oriprice.collection_names()
        for f in onlyfiles:
            df = pd.read_csv(self.stockdata+"/"+f)
            #del those, insert them into another data base
            del df['adjust_price']
            del df['report_type']
            del df['report_date']
            del df['PE_TTM']
            del df['PS_TTM']
            del df['PC_TTM']
            del df['PB']
            del df['adjust_price_f']
            s=f.split('.')
            name = s[0][2:8]

            if name in alldatabase:
                print(name+u"2017-2-28之前数据已经存在")
                continue

            #first drop the database if exits
            #self.connection.drop_collection(name)
            print(u"正在插入数据:"+name)
            records = json.loads(df.T.to_json()).values()
            for row in records:
                row['date'] = datetime.datetime.strptime(row['date'], "%Y-%m-%d")


            self.oriprice[name].insert_many(records) 


        # get stock data in newdata dir
        onlyfiles = [ f for f in listdir(self.newdata) if isfile(join(self.newdata,f)) ]

        #read from using pandas
        #print self.connection.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='i':
                continue

            df = pd.read_csv(self.newdata+"/"+f)
            if len(f) < 12:
                continue

            del df['adjust_price']
            del df['report_type']
            del df['report_date']
            del df['PE_TTM']
            del df['PS_TTM']
            del df['PC_TTM']
            del df['PB']
            del df['adjust_price_f']

            codes=df.code
            series={}


            for i in range(0,len(codes)):
                code=df.iloc[i].code
                series["open"]=df.iloc[i].open
                series["high"]=df.iloc[i].high
                series["low"]=df.iloc[i].low
                series["close"]=df.iloc[i].close
                series["change"]=df.iloc[i].change
                series["volume"]=df.iloc[i].volume
                series["money"]=df.iloc[i].money
                series["traded_market_value"]=df.iloc[i].traded_market_value
                series["market_value"]=df.iloc[i].market_value
                series["turnover"]=df.iloc[i].turnover
                series['date'] = datetime.datetime.strptime(df.iloc[i].date, "%Y-%m-%d")
                name=code[2:8]
                #check exits
                countstock=self.oriprice[name].find({"date":series['date']}).count()
                if countstock ==0:
                    print (u"插入股票"+name+str(series["date"])+u"数据")
                    self.oriprice[name].insert_one(series)
                else:
                    print(u"股票"+name+" "+str(series['date'])+u"已经存在")
                




    #store index information into database,do not always call this
            
    def storageindex(self):
        #get the filelist
        alldatabase=self.index.collection_names()

        onlyfiles = [ f for f in listdir(self.indexdata) if isfile(join(self.indexdata,f)) ]
        #read from using pandas
        for f in onlyfiles:
            df = pd.read_csv(self.indexdata+"/"+f)
            s=f.split('.')
            name = s[0][2:8]
            if name in alldatabase:
                print(u"指数"+name+u" 2017-2-28之前数据已经在数据库中")
                continue
            records = json.loads(df.T.to_json()).values()
            for row in records:
                row['date'] = datetime.datetime.strptime(row['date'], "%Y-%m-%d")
            self.index[name].insert_many(records)


        # get stock data in newdata dir
        onlyfiles = [ f for f in listdir(self.newdata) if isfile(join(self.newdata,f)) ]

        #read from using pandas
        #print self.connection.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='d':
                continue

            df = pd.read_csv(self.newdata+"/"+f)
            if len(f) < 12:
                continue

            codes=df.index_code
            series={}


            for i in range(0,len(codes)):
                code=df.iloc[i].index_code
                series["open"]=df.iloc[i].open
                series["high"]=df.iloc[i].high
                series["low"]=df.iloc[i].low
                series["close"]=df.iloc[i].close
                series["change"]=df.iloc[i].change
                series["volume"]=df.iloc[i].volume
                series["money"]=df.iloc[i].money
                series['date'] = datetime.datetime.strptime(df.iloc[i].date, "%Y-%m-%d")
                name=code[2:8]
                #check exits
                countstock=self.index[name].find({"date":series['date']}).count()
                if countstock ==0:
                    print (u"插入指数"+name+str(series["date"])+u"数据")
                    self.index[name].insert_one(series)
                else:
                    print(u"指数"+name+" "+str(series['date'])+u"已经存在")




            
    
    
    #storage stock pool into database
    def storagepool(self):
        #storage zz500
        df=ts.get_zz500s()
        self.pool['zz500'].insert_many(json.loads(df.to_json(orient='records')))
        #hs300
        df=ts.get_hs300s()
        self.pool['hz300'].insert_many(json.loads(df.to_json(orient='records')))
        #zh50
        df=ts.get_sz50s()
        self.pool['sz'].insert_many(json.loads(df.to_json(orient='records')))
        #st
        df=ts.get_st_classified()
        self.pool['st'].insert_many(json.loads(df.to_json(orient='records')))






    def get_data(self):

        in_package_data = range(2002, 2017)
        cur_year = datetime.datetime.now().year
        last_in_package_data = max(in_package_data)


        # download new data
        to_downloads = range(last_in_package_data + 1, cur_year + 1)

        # frist, get ycDefIds params
        response = requests.get(self.YIELD_MAIN_URL)

        matchs = re.search(r'\?ycDefIds=(.*?)\&', response.text)
        ycdefids = matchs.group(1)
        assert (ycdefids is not None)

        fetched_data = []
        for year in to_downloads:
            print('Downloading from ' + self.DONWLOAD_URL % (year, ycdefids))
            response = requests.get(self.DONWLOAD_URL % (year, ycdefids))
            fetched_data.append(BytesIO(response.content))

        # combine all data

        dfs = []

        basedir = os.path.join(os.path.dirname(__file__), "xlsx")

        for i in in_package_data:
            dfs.append(pd.read_excel(os.path.join(basedir, "%d.xlsx" % i)))

        for memfile in fetched_data:
            dfs.append(pd.read_excel(memfile))

        df = pd.concat(dfs)

        return df

    def get_pivot_data(self):

        df = self.get_data()
        return df.pivot(index=u'日期', columns=u'标准期限(年)', values=u'收益率(%)')



    def insert_zipline_treasure_format(self):
        self.treasure['treasure'].drop()
        pivot_data = self.get_pivot_data()

        frame=pivot_data[[0.08,0.25,0.5,1,2,3,5,7,10,20,30]]
        frame['Time Period']=frame.index
        frame['Time Period']=frame['Time Period'].astype('str')
        frame.columns=['1month', '3month','6month', '1year', '2year', '3year', '5year', '7year', '10year', '20year', '30year','Time Period']
        records = json.loads(frame.T.to_json()).values()
        for row in records:
            temp=row['Time Period']
            temp=temp.split('T')[0]
            row['Time Period'] = datetime.datetime.strptime(temp, "%Y-%m-%d")

        self.treasure['treasure'].insert_many(records)



    def InsertTodayAll(self,name,series):
        #check exits
        countstock=self.test[name].find({"date":series['date']}).count()
        if countstock ==0:
            print (u"插入股票"+name+str(series["date"])+u"数据")
            self.test[name].insert_one(series)
        else:
            print(u"股票"+name+" "+str(series['date'])+u"已经存在")


    def Close(self):
        self.client.close()



    def storageStockName(self):
        totalstock=[]
        onlyfiles = [ f for f in listdir(self.stockdata) if isfile(join(self.stockdata,f)) ]
        for f in onlyfiles:
            s=f.split('.')
            name=s[0][2:8]
            totalstock.append(name)
            
        data = {'codes': totalstock}
        frame = DataFrame(data)
        
        self.pool['all'].insert_many(json.loads(frame.to_json(orient='records')))
        print frame
            
        '''
        {u'traded_market_value': 1277523000.0, u'PB': 6.569209, u'market_value': 3806400000.0, u'code': u'sz002759', u'adjust_price': 39.882956, u'close': 15.86, u'change': 0.099861, u'money': 1991223.0, u'report_date': u'2016-04-28', u'volume': 125550.0, u'high': 15.86, u'PE_TTM': 69.434622, u'low': 15.86, u'report_type': u'2016-03-31', u'PS_TTM': None, u'date': datetime.datetime(2016, 6, 30, 0, 0), u'PC_TTM': 414.510229, u'_id': ObjectId('58b78bc81d41c81cfd004274'), u'open': 15.86, u'adjust_price_f': 15.86, u'turnover': 0.001559}

        '''

if __name__ == '__main__':
    I=InsertDataCVS('127.0.0.1',27017)
    #l=LoadDataCVS('127.0.0.1',27017)
    I.Conn()
    I.storagedaily()
    I.storageindex()

    #I.InsertStock("/Users/zhanghan/Downloads/trading-data-push-20170301/2017-03-01 data.csv")
    #print l.getstockdaily('002759','2016-2-21','2016-7-2')
    # l.Conn()
    # l.storagedaily()
    # l.storageindex()
    # l.storagepool()
    # l.storageStockName()
    # l.insert_zipline_treasure_format()

    #l.storageStockName()
    #print l.getstocklist('all')
