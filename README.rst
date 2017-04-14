Zipline－一个正在成长的项目
=======
Zipline:当前在线的量化平台基本都是基于zipline进行开发，使用这些平台，首先，自己的
策略会泄密，其次，这些平台速度慢，而且不够灵活。
然而，A股并不能直接使用zipline，需要对数据,基准，交易日期，手续费等部分做修改。
本项目修改zipline平台，以使得其能适用于A股市场。


项目文档：

https://github.com/zhanghan1990/zipline/wiki

安装方法
========
运行环境：linux, OSX，建议不要使用windows,因为zipline涉及到gcc的编译，windows可能编译过程中有一些问题。

（1）windows 用户可以下载virtual box，在这个镜像中，集成了数据，和开发环境，以及ipython notebook,virtual box 虚拟机地址： https://pan.baidu.com/s/1bp5roxL

虚拟机密码为:zipline

具体使用方法：打开virtual box ,输入以下命令

- sudo service mongodb start

- source zipline/zip_env/bin/activate

- sudo jupyter notebook

- ifconfig

得到虚拟机的IP 地址，例如IP为：192.168.1.120，则在windows浏览器输入 192.168.1.120:8888


(2)对于本地安装，以ubuntu 为例：

- git clone https://github.com/zhanghan1990/zipline
- cd zipline
- sudo apt-get install python-pip
- sudo apt-get install mongodb
- sudo pip install virtualenv
- sudo apt-get install python-tk
- virtualenv zipline_env
- source zipline_env/bin/activate
- pip install -r requirements.txt
- python setup.py install
- sudo service mongodb start
- pip install xlrd
- pip3 install jupyter
- pip install apscheduler



version_1.1 完成的主要工作
=======================
- 对history函数纠正，history[0]代表今天的价格，history[1]代表昨天价格...
- 增加每天行情数据更新，每天行情数据更新，可以使用tushare或者使用本地数据（从tushare 得到的行情数据，是没有每天市场数据，如PE等，因此，我上传了一个数据源，里面包含每天的行情数据，注意，这里的数据还是原始数据)


version_1.0 完成的主要工作
=======================

- 交易日历纠正，从1990年开始的所有有效交易日都包含其中，剔除非交易时段
- A股数据源，把数据写入mongodb中，每次从mongodb中读取需要的数据
- benchmark，使用A股的几个标准（HS300指数等）
- return 计算，计算alpha和beta当前使用中国国债作为基准
- 手续费模型设定


关于数据
========

- 您可以使用自己的数据，也可以使用我配置的数据源，数据源我已经配置好，如果自己配置，需要修改文件 data/constants.py 下的IP和PORT
- 本版本的数据源,只更新到2017.02.28,后面我会每天更新数据

本地数据导入
===========
- 交易数据地址：
http://pan.baidu.com/s/1i4GZWFF

- 关于数据导入：
脚本 https://github.com/zhanghan1990/zipline/blob/master/zipline/data/mongodb.py 提供数据导入，修改line 29为您数据解压缩位置
然后执行脚本python mongodb.py


关于例子
========

-在examples下面有3个例子，这3个例子可以满足基本的回测需求，这三个例子我和joinquant做了比对，差距很小（ps，完全一样还是很难，手续费那里有问题，我会继续修改)

联系方式
========

欢迎感兴趣的朋友加入到这个项目来，有问题请给我发邮件：
zganghanhan@foxmail.com

加入我们
=======
欢迎有兴趣的朋友伙伴加入我们的开源讨论群：


QQ群：556125593
