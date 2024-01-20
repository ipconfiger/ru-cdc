# ru-cdc

用Rust实现的CDC工具

[English README](./README.md)

##  首先为什么要启动这个项目

首先，为什么要启动这个项目，一开始是一个学习项目，在使用 Canal 的时候遇到了一些奇怪的问题，平时挺稳定的，但是当有一次一次性往数据库导入了300万条数据后，Canal内存爆栈了。然后停止了响应并且无法恢复，但是进程坚持存活，这让我的保活机制也落空了。所以我唯一的选择就是重启Canal，但是重启后Canal的日志一切正常，但是就是不响应任何数据库下发的binlog了。我在抓瞎了个把小时后通过一个绝望的尝试解决了问题：先停掉Canal服务，然后把实例目录用mv命令改个名字，然后修改 server.properties 配置文件里实例目录为刚才mv的目标名字，再重新启动Canal进程，然后就神奇的恢复了。然后在我写这个文档的时候的前一天晚上，这个事情又发生了，这次只导入了20万条，并且已经把 xmx参数已经提高到了4GB，我很从容的用上一次的方法，2分钟内就恢复了服务。但是这种黑箱子里摸大象的感觉十分不好，所以我决定从源头上搞清楚，而现在正在又一次开始学习Rust语言，所以我用Rust从头实现了这个CDC工具。
一开始并没有多少能参考的资料，我打算用 nom 来解析二进制数据，所以搜索 nom 的资料的时候发现了：
[boxercrab](https://github.com/PrivateRookie/boxercrab )
这个项目，于是参考了一下实现了从裸socket编程开始自己实现 MySQL的握手协议，切换登录方式，用sha加密密码后登录，然后编码文本协议的SQL命令，解析返回的结果集。最后在解析binlog格式的时候卡住了，这个库只支持对Query模式的解析，在这个模式下，MySQL订阅下发的是执行的SQL文本，而Canal是通过ROW模式来运行的，所以我只能去看MySQL的官方文档，这么多的文字浩若烟海，最后又找到了一个Python实现的mysql复制库：
[python-mysql-replication](https://github.com/julien-duponchelle/python-mysql-replication)
，这个库就比较全面了，但是Python代码因为没有类型定义的关系，对二进制的解析看起来就很魔幻了，但是对比这官网文档，还是很艰难的在两周业余时间完成了大部分的功能。其中还遇到很多的坑，如果有时间的话，我会写一篇Blog详细记录下来。

## 如何使用

### 第一步：创建配置文件

通过命令：

    ru-cdc --config 配置文件地址 --gen
    //实际例子
    :#ru-cdc --config /etc/cdc-config.json --gen

这样就会把配置文件模版创建到指定的地方。

配置文件内容如下：

    {
        "db_ip": "127.0.0.1",          // 数据库地址
        "db_port": 3306,               // 数据库端口
        "max_packages": 4294967295,    // 最大数据包大小
        "user_name": "canal",          // 订阅binlog的账号
        "passwd": "canal",             // 该账号的密码
        "workers": 8                   // 工作线程的数量
        "mqs": [                       // 消息队列的列表
            {
                "mq_name": "the_kafka",   // 消息队列的名字，用于指定使用该消息队列
                "mq_cfg": {               
                    "KAFKA": {          
                        "brokers": "192.168.1.222:9099",  // Kakfa的Broker列表，多个用逗号隔开
                        "queue_buffering_max": 300        // 批次发送的最大等待毫秒数
                    }
                }
            }
        ],
        "instances": [                // 实例列表
            {
                "mq": "the_kafka",    // 使用哪一个消息队列配置
                "schemas": "test*",   // 数据库过滤器，支持*通配符
                "tables": "s*",       // 表名过滤器，支持*通配符
                "topic": "test"       // 消息发送到哪个主题
            }
        ]
    }

当按照自己的架构修改好配置后，通过下面的命令启动服务

    :#ru-cdc --config /etc/cdc-config.json --serve


## 压测压测看看

![20240119210946066](https://github.com/ipconfiger/ru-cdc/assets/950968/cae55600-0e4c-4512-b2d3-ec6362131cad)





