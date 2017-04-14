#coding=utf-8
import time
import sched
import os
import threading
import tushare as ts
import datetime
import logging
logging.basicConfig()


from insertDatabase import InsertDataCVS

from apscheduler.schedulers.blocking import BlockingScheduler

def event_func():
	"""
	触发调度函数
	"""
	# 首先获取当日的交易数据
	I=InsertDataCVS('127.0.0.1',27017)
	I.Conn()
	todaydata=ts.get_today_all()
	now=time.time()
	stocktime=time.strftime('%Y-%m-%d',time.localtime(time.time()))
	print stocktime
	length=len(todaydata.index)
	print todaydata
	for i in range(0,length):
		series={}
		series["open"]=todaydata.iloc[i].open
		series["high"]=todaydata.iloc[i].high
		series["low"]=todaydata.iloc[i].low
		series["close"]=todaydata.iloc[i].trade
		series["change"]=todaydata.iloc[i].changepercent
		series["volume"]=todaydata.iloc[i].volume
		series["money"]=todaydata.iloc[i].amount
		series["traded_market_value"]=todaydata.iloc[i].mktcap
		series["market_value"]=todaydata.iloc[i].nmc
		series["turnover"]=todaydata.iloc[i].turnoverratio
		series['date'] = datetime.datetime.strptime(stocktime, "%Y-%m-%d")
		I.InsertTodayAll(todaydata.iloc[i].code,series)
	I.insert_zipline_treasure_format()
	I.close()
	

# BlockingScheduler
scheduler = BlockingScheduler()
scheduler.add_job(event_func, 'cron', day_of_week='1-5', hour=12, minute=32)
scheduler.start()


"""
sched模块，准确的说，是一个调度（延时处理机制），每次想要定时执行某任务都必须写入一个调度。
使用步骤如下：
(1)生成调度器：
s = sched.scheduler(time.time,time.sleep)
第一个参数是一个可以返回时间戳的函数，第二个参数可以在定时未到达之前阻塞。可以说sched模块设计者是
“在下很大的一盘棋”，比如第一个函数可以是自定义的一个函数，不一定是时间戳，第二个也可以是阻塞socket等。
(2)加入调度事件
其实有enter、enterabs等等，我们以enter为例子。
s.enter(x1,x2,x3,x4)
四个参数分别为：间隔事件、优先级（用于同时间到达的两个事件同时执行时定序）、被调用触发的函数，给他的
参数（注意：一定要以tuple给如，如果只有一个参数就(xx,)）
(3)运行
s.run()
注意sched模块不是循环的，一次调度被执行后就Over了，如果想再执行，请再次enter
"""
##########################
#初始化sched模块的scheduler类
##########################
s = sched.scheduler(time.time,time.sleep)

##########################
#调度函数定义
##########################


###############################################################
#定义执行函数，并通过enter函数加入调度事件
#enter四个参数分别为：间隔事件、优先级（用于同时间到达的两个事件同时执行时定序）,
#被调用触发的函数，给他的参数（注意：一定要以tuple给如，如果只有一个参数就(xx,)）
###############################################################

def perform(inc,name,start):
	"""
	实现inc周期执行任务
	"""
	s.enter(inc,0,perform,(inc,name,start))
	event_func(name,start)

#######################
#主函数入口
#######################
def mymain(inc=10):
	"""
	入口主函数
	"""
	start = time.time()
	print('START:',time.ctime(start))
	#设置调度
	e1 = s.enter(inc,1,perform,(inc,u'获取每天实时行情',start))  #调度设置
	t=threading.Thread(target=s.run)  #通过构造函数例化线程
	t.start()                                                                                    #线程启动                                                                            #取消任务调度e2
	t.join()                                                                                     #阻塞线程



if __name__ == "__main__":
	mymain() 