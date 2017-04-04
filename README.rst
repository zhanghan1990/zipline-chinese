Zipline
=======

Zipline:当前在线的量化平台基本都是基于zipline进行开发，使用这些平台，首先，自己的
策略会泄密，其次，这些平台速度慢，而且不够灵活。
然而，A股并不能直接使用zipline，需要对数据,基准，交易日期，手续费等部分做修改。
本项目修改zipline平台，以使得其能适用于A股市场。

安装方法
========

运行环境：linux, OSX，建议不要使用windows,因为zipline涉及到gcc的编译，windows可能编译过程中有一些问题


以ubuntu 为例：

- sudo pip install virtualenv
- git clone https://github.com/zhanghan1990/zipline
- cd zipline
- . venv/bin/activate
- python setup.py install


安装过程中出现的问题
==============
(1) bottleneck 和 numpy版本问题：  

build_ext = self.get_finalized_command('build_ext')
  File "/usr/lib/python2.7/distutils/cmd.py", line 312, in get_finalized_command
    cmd_obj.ensure_finalized()
  File "/usr/lib/python2.7/distutils/cmd.py", line 109, in ensure_finalized
    self.finalize_options()
  File "/tmp/easy_install-TJlXeR/Bottleneck-1.2.0/setup.py", line 24, in finalize_options
    join,
AttributeError: 'dict' object has no attribute '__NUMPY_SETUP__'


解决办法:pip install bottleneck==1.0.0，然后继续执行
python setup.py install

(2)

```/tmp/easy_install-nGdcR9/scipy-0.19.0/setup.py:323: UserWarning: Unrecognized setuptools command, proceeding with generating Cython sources and expanding templates
  warnings.warn("Unrecognized setuptools command, proceeding with "
/home/zhanghan/zipline_2/zipline/venv/local/lib/python2.7/site-packages/numpy/distutils/system_info.py:572: UserWarning: 
    Atlas (http://math-atlas.sourceforge.net/) libraries not found.
    Directories to search for the libraries can be specified in the
    numpy/distutils/site.cfg file (section [atlas]) or by setting
    the ATLAS environment variable.
  self.calc_info()
/home/zhanghan/zipline_2/zipline/venv/local/lib/python2.7/site-packages/numpy/distutils/system_info.py:572: UserWarning: 
    Lapack (http://www.netlib.org/lapack/) libraries not found.
    Directories to search for the libraries can be specified in the
    numpy/distutils/site.cfg file (section [lapack]) or by setting
    the LAPACK environment variable.
  self.calc_info()
/home/zhanghan/zipline_2/zipline/venv/local/lib/python2.7/site-packages/numpy/distutils/system_info.py:572: UserWarning: 
    Lapack (http://www.netlib.org/lapack/) sources not found.
    Directories to search for the sources can be specified in the
    numpy/distutils/site.cfg file (section [lapack_src]) or by setting
    the LAPACK_SRC environment variable.
  self.calc_info()
Running from scipy source directory.
non-existing path in 'scipy/integrate': 'quadpack.h'
error: no lapack/blas resources found```


解决办法:pip install pip install scipy==0.15.1，然后继续执行
python setup.py install



本版本完成的主要工作
========

- 交易日历纠正，从1990年开始的所有有效交易日都包含其中，剔除非交易时段
- A股数据源，把数据写入mongodb中，每次从mongodb中读取需要的数据
- benchmark，使用A股的几个标准（HS300指数等）
- return 计算，计算alpha和beta当前使用中国国债作为基准
- 手续费模型设定


关于数据
========

- 您可以使用自己的数据，也可以使用我配置的数据源，数据源我已经配置好，如果自己配置，需要修改文件 data/constants.py 下的IP和PORT
- 本版本的数据源,只更新到2017.02.28,后面我会每天更新数据

关于例子
========

-在examples下面有3个例子，这3个例子可以满足基本的回测需求，这三个例子我和joinquant做了比对，差距很小（ps，完全一样还是很难，手续费那里有问题，我会继续修改)

联系方式
========

欢迎感兴趣的朋友加入到这个项目来，有问题请给我发邮件：
zganghanhan@foxmail.com


