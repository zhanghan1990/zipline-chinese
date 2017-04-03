#encoding:utf-8
import pandas as pd
from zipline import TradingAlgorithm
from zipline.api import order, sid,get_datetime
from zipline.data.loader import load_data
from zipline.api import order_target, record, \
symbol, history, add_history,symbol,set_commission,order_percent,set_long_only,get_open_orders,order_value,order_target

from zipline.finance.commission import OrderCost
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
import pytz
from datetime import datetime, timedelta


input_data = load_data(
    stockList=['000001'],
    start="2014-11-04",
    end="2016-01-16"
)


# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 定义一个全局变量, 保存要操作的股票
    # 000001(股票:平安银行)
    context.security = '000001'

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    # 获取股票的收盘价
    close_data = history(12,'1d','close')
    # 取得过去五天的平均价格
    ma5 = close_data[-6:-2].mean()
    # 取得过去10天的平均价格
    ma10 = close_data[-11:-2].mean()
    # 取得当前的现金

    print get_datetime(),ma5,ma10
    cash = context.portfolio.cash
    
    #print ma5[sid(symbol(context.security))],ma10[sid(stock)],cash,symbol(context.security)
    #如果当前有余额，并且五日均线大于十日均线
    if ma5[sid(symbol(context.security))] > ma10[sid(symbol(context.security))]:
         order_value(symbol(context.security), cash)
    # 如果五日均线小于十日均线，并且目前有头寸
    elif ma5[sid(symbol(context.security))] < ma10[sid(symbol(context.security))]:
        # 全部卖出
        order_target(symbol(context.security), 0)
        # 记录这次卖出
        #log.info("Selling %s" % (context.security))

    #record(short_mavg=ma)

    # # 绘制五日均线价格
    # record(ma5=ma5)
    # # 绘制十日均线价格
    # record(ma10=ma10)
def analyze(context=None, results=None):
    import matplotlib.pyplot as plt
    import logbook
    logbook.StderrHandler().push_application()
    log = logbook.Logger('Algorithm')

    fig = plt.figure()
    ax1 = fig.add_subplot(211)

    results.algorithm_period_return.plot(ax=ax1,color='blue',legend=u'策略收益')
    ax1.set_ylabel(u'收益')
    results.benchmark_period_return.plot(ax=ax1,color='red',legend=u'基准收益')



    # ax2 = fig.add_subplot(212)
    # ax2.set_ylabel('Price (USD)')

    # # If data has been record()ed, then plot it.
    # # Otherwise, log the fact that no data has been recorded.
    # if ('ma51' in results and 'ma101' in results):
    #     results[['ma51', 'ma101']].plot(ax=ax2)

    #     trans = results.ix[[t != [] for t in results.transactions]]
    #     buys = trans.ix[[t[0]['amount'] > 0 for t in
    #                      trans.transactions]]
    #     sells = trans.ix[
    #         [t[0]['amount'] < 0 for t in trans.transactions]]
    #     ax2.plot(buys.index, results.short_mavg.ix[buys.index],
    #              '^', markersize=10, color='m')
    #     ax2.plot(sells.index, results.short_mavg.ix[sells.index],
    #              'v', markersize=10, color='k')
    #     plt.legend(loc=0)
    # else:
    #     msg = 'AAPL, short_mavg & long_mavg data not captured using record().'
    #     ax2.annotate(msg, xy=(0.1, 0.5))
    #     log.info(msg)

    plt.show()

# capital_base is the base value of capital
#
algo = TradingAlgorithm(initialize=initialize, handle_data=handle_data,capital_base=10000,benchmark='000300')

#print input_data
#api: print all the api function
#print algo.all_api_methods()
results = algo.run(input_data)
#print results['benchmark_period_return'],results['portfolio_value']
analyze(results=results)

