#encoding:utf-8
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
from zipline import TradingAlgorithm
from zipline.api import order, sid,get_datetime
from zipline.data.loader import load_data
from zipline.api import order_target, record, symbol, history, add_history,symbol,set_commission,order_percent,set_long_only,get_open_orders,run_monthly,run_weekly
from zipline.finance.commission import OrderCost
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False



def analyze(context=None, results=None):
    import matplotlib.pyplot as plt

    # Plot the portfolio and asset data.
    ax1 = plt.subplot(211)
    results.algorithm_period_return.plot(ax=ax1,color='blue',legend=u'策略收益')
    ax1.set_ylabel(u'收益')
    results.benchmark_period_return.plot(ax=ax1,color='red',legend=u'基准收益')

    # Show the plot.
    plt.gcf().set_size_inches(18, 8)
    plt.show()



# loading the data
input_data = load_data(
    stockList= ['000001','000002','000004','000005'],
    start="2013-11-01",
    end="2016-01-16"
)


def initialize(context):
    # 初始化此策略
    # 设置我们要操作的股票池
    context.stocks = ['000001','000002','000004','000005']



# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    
    # context.i+=1
    # if context.i<=5:
    #     return
    # 循环每只股票

    closeprice= history(5,'1d','close')
    for security in context.stocks:
        vwap=(closeprice[symbol(security)][-2]+closeprice[symbol(security)][-3]+closeprice[symbol(security)][-4])/3
        price = closeprice[symbol(security)][-2]
        print get_datetime(),security,vwap,price
        # # 如果上一时间点价格小于三天平均价*0.995，并且持有该股票，卖出
        if price < vwap * 0.995:
            # 下入卖出单
            order(symbol(security),-300)
            print get_datetime(),("Selling %s" % (security))
            # 记录这次卖出
            #log.info("Selling %s" % (security))
        # 如果上一时间点价格大于三天平均价*1.005，并且有现金余额，买入
        elif price > vwap * 1.005:
            # 下入买入单
            order(symbol(security),300)
            # 记录这次买入
            print get_datetime(),("Buying %s" % (security))
            #log.info("Buying %s" % (security))


algo = TradingAlgorithm(initialize=initialize, handle_data=handle_data,capital_base=100000,benchmark='000300')

#print input_data
#api: print all the api function
#print algo.all_api_methods()
results = algo.run(input_data)
#print results['benchmark_period_return'],results['portfolio_value']
analyze(results=results)


