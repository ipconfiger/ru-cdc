# ru-cdc
Another CDC Tool Written by Rust

[中文文档](./README_ZH.md)

## First of all...

First of all, why start this project? 
It was initially a learning project. I encountered some strange issues when using Canal. 
It was usually stable, but when I once imported 3 million records into the database at once, Canal's memory stack overflowed. 
It stopped responding and couldn't recover, but the process remained alive, rendering my keep-alive mechanism useless. 
So my only choice was to restart Canal. However, after the restart, Canal's logs were normal, but it didn't respond to any database-issued binlogs. After struggling for a few hours, I solved the problem through a desperate attempt: I stopped the Canal service, renamed the instance directory using the mv command, modified the server.properties configuration file to point to the new directory name, and then restarted the Canal process. It miraculously recovered. The night before I wrote this document, the same thing happened again. This time, I only imported 200,000 records, and I had already increased the xmx parameter to 4GB. I calmly used the same method and restored the service within 2 minutes. However, this trial-and-error approach felt very uncomfortable, so I decided to thoroughly understand the root cause. I am currently learning Rust from scratch, so I implemented this CDC tool in Rust.

Initially, there wasn't much reference material. I planned to use nom to parse binary data. While searching for nom documentation, I came across the project [boxercrab](https://github.com/PrivateRookie/boxercrab ). I referenced it and implemented the MySQL handshake protocol from raw socket programming, switched login methods, logged in after encrypting the password with SHA, encoded SQL commands in text protocol, and parsed the returned result set. However, I got stuck when parsing the binlog format. This library only supports parsing in Query mode, where MySQL subscribes to executed SQL texts, while Canal operates in ROW mode. 
So, I had to resort to the MySQL official documentation, which was overwhelming. Finally, I found a Python implementation of a MySQL replication library: [python-mysql-replication](https://github.com/julien-duponchelle/python-mysql-replication). This library was more comprehensive, but due to the lack of type definitions in Python code, the binary parsing seemed quite magical. Despite the challenges, I managed to complete most of the functionality in my spare time over two weeks. I encountered many pitfalls, and if time permits, I will write a detailed blog post about it.


## How to Use

### Step 1: Create Configuration File

Use the following command:

    ru-cdc --config <config_file_path> --gen
    // Example
    :#ru-cdc --config /etc/cdc-config.json --gen

This will generate a configuration file template at the specified location.

The configuration file contains the following:

    {
        "db_ip": "127.0.0.1",          // Database address
        "db_port": 3306,               // Database port
        "max_packages": 4294967295,    // Maximum package size
        "user_name": "canal",          // Account for subscribing to binlog
        "passwd": "canal",             // Password for the account
        "mqs": [                       // List of message queues
            {
                "mq_name": "the_kafka",   // Name of the message queue, used to specify the use of this message queue
                "mq_cfg": {               
                    "KAFKA": {          
                        "brokers": "192.168.1.222:9099"  // List of Kafka brokers, separated by commas for multiple brokers
                    }
                }
            }
        ],
        "instances": [                // List of instances
            {
                "mq": "the_kafka",    // Use which message queue configuration
                "schemas": "test*",   // Database filter, supports * wildcard
                "tables": "s*",       // Table name filter, supports * wildcard
                "topic": "test"       // Send messages to which topic
            }
        ]
    }

After modifying the configuration according to your architecture, start the service with the following command

    :#ru-cdc --config /etc/cdc-config.json --serve