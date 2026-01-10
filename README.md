我正在完成分布式系统大作业，想实现一个分布式键值存储系统。

题目
设计并实现一个分布式键值（key-value）存储系统，可以是
基于磁盘的存储系统，也可以是基于内存的存储系统，可以是主从
结构的集中式分布式系统，也可以是 P2P 式的非集中式分布式系
统。能够完成基本的读、写、删除等功能，支持缓存、多用户和数
据一致性保证, 提交时间为考试之后一周内。
2. 要求
 1）、必须是分布式的键值存储系统，至少在两个节点或者两个
进程中测试；
 2）、可以是集中式的也可以是非集中式；
 3）、能够完成基本的操作如：PUT、GET、DEL 等；
 4）、支持多用户同时操作；
 5）、至少实现一种面向客户的一致性如单调写；
 6）、需要完整的功能测试用例；
 7）、涉及到节点通信时须采用 RPC 机制；

权衡作业要求和实现复杂度后，我做出一下设计决策v2：

1. 使用python编程，不同进程使用xmlrpc通信，并发操作要封装成 threadpool 避免资源争用
2. 一些假设
   1. 可能会出现进程掉线
   2. 不考虑网络分区，所有进程可以互相通信
   3. 所有进程彼此可信
3. 集中式架构：总共三种进程Orche、Store、Client
   1. Orche：
      1. 启动时有固定的地址，为 Client 和 Store 提供统一的通信地点
      2. 通过心跳监测 Store 集群状态，在 Store 状态异常时要重新分配数据分布（采用一致性哈希）
      3. 向 Client 返回 key 对应的 Store；为了性能考虑，不确保返回结果完全准确
      4. 多个 Orche 形成面向 Client 和 Store 的负载均衡和容错，相互共享 Store 集群状态，保证最终一致
         1. 使用 Raft 选举 leader（https://cloud.tencent.com/developer/article/2347149）
      5. Orche 掉线时，其负责的 Store 监测任务需要分配到其他 Orche，需要管理员或其他监测程序重新启动 Orche，其会自动加入网络
   2. Store：
      1. 启动时需要向 Orche 注册服务，并定期发送心跳包
      2. 受 Orche 控制，负责哈希环中一段数据的存储和返回，以及 R 段数据的备份存储
         1. 零初始化：当涉及存储的段没有数据时，不需要做任何事
         2. 拉取初始化：当涉及存储有数据时，需要向负责的 Store 批量拉取数据
      3. 接受 Client 的 put/get/del 请求，如果与负责区间相符，则处理请求
      4. 数据存储包括版本号，使用锁保证键写入串行化
      5. 所有修改请求转发到备份存储节点同步修改
   3. Client
      1. 启动时选择一个有效的 Orche 通信，随后除非无法通信，否则固定
      2. 通过 API 或 cli 前端获得数据库操作
      3. put/get/del 请求先查 Orche 得到负责节点，写入缓存，再请求对应 Store
      4. 缓存负责节点，可能会失效，则联系 Orche 更新缓存
      5. 缓存数据和版本号，get 请求如果发现数据已经是最新，则不返回 val（302）
4. 集成测试
   1. 基本 put/get/del 操作
   2. Orche 下线后系统依然稳健
   3. Store 下线后系统依然稳健
5. 面向客户的一致性
   1. 所有缓存都需要向 Store 确认，且使用版本号保持一致性
   2. 读已所写/单调读：每个数据由 Store 唯一负责，在无错误时可确保一致性；在有错误时，可能会读到未同步数据，这可以通过要求同步写备份保持一致性
   3. 单调写：所有写入操作同步返回，同一 client 的写请求串行执行，因此满足单调写
6. 容错性：支持 Orche、Store 掉线自动恢复
7. 可扩展性：
   1. Orche：当 Client 或 Store 增加时，可以扩展 Orche 保持吞吐
   2. Store：当请求增加、存储增加时，可以扩展 Store

流程设计：

Client 操作：
1. put
   1. Client：向Orche请求Store集群数据
   2. Orche：返回最新Store集群数据
   3. Client：查找主Store，向主Store请求写
   4. Store：判断是否为主Store，如果是则同步写入数据和备份节点，返回成功
   5. Client：完成写入
2. get
   1. Client：向Orche请求Store集群数据
   2. Orche：返回最新Store集群数据
   3. Client：随机选一个可行的Store，该Store请求读
   4. Store：判断是否包含数据，如果是则返回数据
   5. Client：获得数据
3. del
   1. 同 put

Store 操作：
1. register
   1. Store：向Orche注册该节点
   2. Orche：返回负责区间和区间负责节点列表
   3. Store：对于负责区间的每个子区间，向对应负责节点拉取数据，然后向Orche报告Ready
   4. Orche：将节点正式加入集群
   5. Store：立即heartbeat并定时heartbeat
2. heartbeat
   1. Store：向Orche请求更新状态
   2. Orche：返回ok

类设计：

1. Orche：
   1. OrcheMain
      1. add_store(name: str)
      2. del_store(name: str)
      3. get_stores() -> dict[str, str]
      4. task_stale_store()
   2. OrcheClientRPC
      1. get_stores() -> dict[str, str]
   3. OrcheStoreRPC
      1. register(addr, port) -> str
      2. heartbeat(name: str) -> bool
2. Store
   1. StoreDB
      1. put(key, val)
      2. get(key)
      3. del(key)
      4. get_segment(from, to)
      5. del_segment(from, to)
      6. put_segment(seg)
   2. StoreMain
      1. task_heartbeat()
      2. clear()
      3. put(key, val)
      4. get(key)
      5. del(key)
      6. get_inteval() -> list[int]
      7. get_segment(from, to)
      8. split_segment(from, to, pos)
      9. merge_segment(from, to)
   3. StoreClientRPC
      1. put(key, val)
      2. get(key)
      3. del(key)
   4. StoreOrcheRPC
      1. update_inteval(inteval: list[int])
   5. StoreStoreRPC
      1. get_segment(from, to)
      2. downsync_put(key, val)
      3. downsync_del(key)
3. Client
   1. ClientMain
      1. put
      2. get
      3. del
   2. ClientAPI
      1. put
      2. get
      3. del
   3. ClientCIL
      1. repl