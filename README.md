# arrow_test

## 环境准备


* clone 代码

```
git clone git@github.com:lvheyang/arrow.git
```

* Idea 安装JMH插件，运行benchmark

* 安装Gandiva需要的依赖环境

```
# protobuf 3.11.4 需要源码编译cpp
# 从 https://github.com/protocolbuffers/protobuf/releases/latest 找对应版本下载
# 注意下载3.11.4版本，最新版本无法使用

./configure --prefix=/usr/local/opt/protobuf/
make -j 4 && make install

# lz4 
brew install lz4

# zstd
brew install zstd

# re2
brew install re2

``` 

## 代码说明

```
AddTotal2Bench 是直接裸计算a+b+c的速度 大致是 4-6ms 每4百万条数据。这个数值作为基准测试。
AddTotalBenchmark 是使用arrow，循环用get获取几个值，并且使用a+b+c，大致处理速度是 100-120ms 每4百万条数据。
GandivaBench 是使用arrow+gandiva，用jni的方式，向量计算a+b+c，大致处理速度是8-10ms 每四百万条数据。
```