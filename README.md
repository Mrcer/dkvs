# DKVS

使用 Python 实现的分布式键值存储系统（Distributed Key-Value Store, dkvs）
支持以下功能：

- 基本的读、写、删除功能
- 支持多用户并发操作数据库
- 在客户端实现了LRU缓存
- 存储节点支持在线线性扩展
- 提供读一致和写一致
- 提供完备的集成测试

系统正常运行需要以下假设：

- 无异常：不能出现网络分区、节点掉线等情况
- 可信网络：所有请求必须来自dkvs，无法防御网络攻击

# 使用

启动集群

```shell
python main.py cluster
```

启动客户端

```shell
python main.py cli
```

客户端操作

```
=== KV Store Client ===
Commands: PUT <key> <value> | GET <key> | DEL <key> | EXIT
```

# 测试

需要确保 8000 端口不被占用，且 10000 到 20000 端口至少有 4 个可用。

```
python -m unittest tests.test_dkvs
```