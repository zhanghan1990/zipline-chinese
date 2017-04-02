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



# loading the data
input_data = load_data(
    stockList=['002057'],
    start="2015-11-04",
    end="2016-01-16"
)
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


def weekly_test(context,data):
    #输出当前时间
    print get_datetime()


def initialize(context):
    set_commission(OrderCost(open_tax=0,close_tax=0.001,open_commission=0.0003,close_commission=0.0003,close_today_commission=0,min_commission=5))
    set_long_only()
    run_weekly(weekly_test,1)

def handle_data(context, data):
    pass


algo = TradingAlgorithm(initialize=initialize, handle_data=handle_data,capital_base=10000)

results = algo.run(input_data)

analyze(results=results)
