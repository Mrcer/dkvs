# tests/test_dkvs.py
import unittest
import time
from unittest.mock import patch
from uuid import uuid4, UUID
import dkvs
from multiprocessing import Process
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from dkvs.hash import ConsistentHash, StoreInfo, StoreState

class TestConsistentHashLogic(unittest.TestCase):
    """测试一致性哈希的逻辑正确性"""
    
    def setUp(self):
        """测试前置准备"""
        self.replicas = 2
        self.consistent_hash = ConsistentHash(replicas=self.replicas)
        
        # 创建固定的测试节点ID
        self.node1_id = UUID('11111111-1111-1111-1111-111111111111')
        self.node2_id = UUID('22222222-2222-2222-2222-222222222222')
        self.node3_id = UUID('33333333-3333-3333-3333-333333333333')
        self.node4_id = UUID('44444444-4444-4444-4444-444444444444')
        
        self.node1 = StoreInfo(
            store_id=self.node1_id,
            addr="127.0.0.1:8001",
            state=StoreState.READY,
            heartbeat=time.time()
        )
        
        self.node2 = StoreInfo(
            store_id=self.node2_id,
            addr="127.0.0.1:8002",
            state=StoreState.READY,
            heartbeat=time.time()
        )
        
        self.node3 = StoreInfo(
            store_id=self.node3_id,
            addr="127.0.0.1:8003",
            state=StoreState.INITIALIZING,
            heartbeat=time.time()
        )
        
        self.node4 = StoreInfo(
            store_id=self.node4_id,
            addr="127.0.0.1:8004",
            state=StoreState.LOCK,
            heartbeat=time.time()
        )

    def create_mock_hash_func(self, mapping: dict[str, int]):
        """创建模拟哈希函数
        
        Args:
            mapping: 字符串到哈希值的映射字典
        """
        def mock_hash(key: str) -> int:
            # 如果在映射中，返回预设值，否则使用默认哈希
            if key in mapping:
                return mapping[key]
            # 对于不在映射中的key，可以返回一个默认值或引发异常
            raise KeyError(f"Key '{key}' not in mock hash mapping")
        
        return mock_hash

    @patch.object(ConsistentHash, '_hash')
    def test_ring_order_and_node_selection(self, mock_hash):
        """测试环的顺序和节点选择逻辑"""
        # 设置模拟哈希值，确保我们可以控制节点在环上的位置
        # 我们设置节点哈希值为：node1->100, node2->200, node3->300, node4->400
        hash_mapping = {
            str(self.node1_id): 100,
            str(self.node2_id): 200, 
            str(self.node3_id): 300,
            str(self.node4_id): 400,
            "key1": 50,     # 在node1之前
            "key2": 150,    # 在node1和node2之间
            "key3": 250,    # 在node2和node3之间
            "key4": 350,    # 在node3和node4之间
            "key5": 450,    # 在node4之后，应该回到node1
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加节点到环中
        self.consistent_hash.add_node(self.node1)
        self.consistent_hash.add_node(self.node2)
        self.consistent_hash.add_node(self.node3)
        self.consistent_hash.add_node(self.node4)
        
        # 验证环的顺序
        self.assertEqual(self.consistent_hash.ring, [100, 200, 300, 400])
        
        # 测试get_next_node：key1(50)的下一个节点应该是node1(100)
        node = self.consistent_hash.get_next_node("key1", lambda n: True)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node1_id) # type: ignore
        
        # 测试get_next_node：key2(150)的下一个节点应该是node2(200)
        node = self.consistent_hash.get_next_node("key2", lambda n: True)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node2_id) # type: ignore
        
        # 测试get_next_node：key5(450)的下一个节点应该是node1(100)（绕回）
        node = self.consistent_hash.get_next_node("key5", lambda n: True)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node1_id) # type: ignore
        
        # 测试get_prev_node：key1(50)的前一个节点应该是node4(400)（反向绕回）
        node = self.consistent_hash.get_prev_node("key1", lambda n: True)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node4_id) # type: ignore
        
        # 测试get_prev_node：key2(150)的前一个节点应该是node1(100)
        node = self.consistent_hash.get_prev_node("key2", lambda n: True)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node1_id) # type: ignore

    @patch.object(ConsistentHash, '_hash')
    def test_primary_node_selection_with_state_filter(self, mock_hash):
        """测试主节点选择时的状态过滤逻辑"""
        # 设置模拟哈希值
        hash_mapping = {
            str(self.node1_id): 100,  # READY
            str(self.node2_id): 200,  # READY
            str(self.node3_id): 300,  # INITIALIZING
            str(self.node4_id): 400,  # UNWRITABLE
            "test_key": 150,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加节点
        self.consistent_hash.add_node(self.node1)
        self.consistent_hash.add_node(self.node2)
        self.consistent_hash.add_node(self.node3)
        self.consistent_hash.add_node(self.node4)
        
        # test_key(150)的下一个节点按顺序是node2(200)，但get_primary_node应该跳过INITIALIZING
        # 而node2是READY状态，所以应该返回node2
        primary = self.consistent_hash.get_primary_node("test_key")
        self.assertIsNotNone(primary)
        self.assertEqual(primary.store_id, self.node2_id) # type: ignore
        
        # 测试如果所有后续节点都是INITIALIZING
        hash_mapping2 = {
            str(self.node1_id): 100,
            str(self.node2_id): 200,
            str(self.node3_id): 300,
            "test_key": 250,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping2)
        
        ch2 = ConsistentHash(replicas=2)
        ch2.add_node(self.node1)
        ch2.add_node(self.node2)
        ch2.add_node(self.node3)  # INITIALIZING
        
        # test_key(250)的下一个节点是node3(300)，但node3是INITIALIZING
        # 继续查找：下一个是node1(100)，node1是READY，所以应该返回node1
        primary = ch2.get_primary_node("test_key")
        self.assertIsNotNone(primary)
        self.assertEqual(primary.store_id, self.node1_id) # type: ignore

    @patch.object(ConsistentHash, '_hash')
    def test_get_nodes_replica_logic(self, mock_hash):
        """测试获取副本节点的逻辑"""
        # 设置模拟哈希值，创建5个节点
        nodes = []
        node_ids = []
        hash_mapping = {}
        
        for i in range(5):
            node_id = UUID(f'{i:032}')
            node = StoreInfo(
                store_id=node_id,
                addr=f"127.0.0.1:{8000 + i}",
                state=StoreState.READY,
                heartbeat=time.time()
            )
            nodes.append(node)
            node_ids.append(node_id)
            hash_mapping[str(node_id)] = i * 100  # 0, 100, 200, 300, 400
        
        # 测试key的哈希值
        hash_mapping["test_key"] = 250
        
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加所有节点
        for node in nodes:
            self.consistent_hash.add_node(node)
        
        # 获取节点列表
        # test_key(250)的主节点应该是300（node4），然后是400，然后是0（绕回）
        # replicas=2，所以返回3个节点：主节点+2个副本
        result = self.consistent_hash.get_nodes("test_key")
        
        # 验证返回的节点数
        self.assertEqual(len(result), min(3, len(nodes)))
        
        # 验证节点顺序（根据我们的哈希值）
        expected_order = [node_ids[3], node_ids[4], node_ids[0]]  # 300, 400, 0
        for i, node in enumerate(result[:3]):
            self.assertEqual(node.store_id, expected_order[i])

    @patch.object(ConsistentHash, '_hash')
    def test_is_in_range_logic(self, mock_hash):
        """测试区间判断逻辑"""
        # 设置模拟哈希值，创建一个简单的环
        hash_mapping = {
            str(self.node1_id): 200,    # READY
            str(self.node2_id): 400,    # READY
            str(self.node3_id): 600,    # INITIALIZING
            "test_hash_100": 100,
            "test_hash_300": 300,
            "test_hash_500": 500,
            "test_hash_700": 700,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加节点
        self.consistent_hash.add_node(self.node1)  # 200
        self.consistent_hash.add_node(self.node2)  # 400
        self.consistent_hash.add_node(self.node3)  # 600
        
        # 测试node1负责的区间：(prev_node.hash, node1.hash]
        # node3 还在初始化，不负责区间
        # prev_node是node2(400)，所以区间是(400, 200]（跨越边界）
        # 即：h > 400 或 h <= 200
        
        # h=100 应该在node1的区间内
        self.assertTrue(self.consistent_hash.is_in_range(100, self.node1_id))
        
        # h=300 应该在node2的区间内，不在node1区间
        self.assertFalse(self.consistent_hash.is_in_range(300, self.node1_id))
        
        # h=700 应该在node1的区间内（跨越边界）
        self.assertTrue(self.consistent_hash.is_in_range(700, self.node1_id))
        
        # h=500 应该在node3的区间内
        self.assertTrue(self.consistent_hash.is_in_range(500, self.node3_id))
        
        # 测试node2负责的区间：(node1.hash, node2.hash] = (200, 400]
        # h=300 应该在node2区间内
        self.assertTrue(self.consistent_hash.is_in_range(300, self.node2_id))
        
        # h=100 不在node2区间内
        self.assertFalse(self.consistent_hash.is_in_range(100, self.node2_id))
        
        # h=500 不在node2区间内
        self.assertFalse(self.consistent_hash.is_in_range(500, self.node2_id))

    @patch.object(ConsistentHash, '_hash')
    def test_node_pair_in_state(self, mock_hash):
        """测试获取前后节点对"""
        # 设置模拟哈希值
        hash_mapping = {
            str(self.node1_id): 100,    # READY
            str(self.node2_id): 200,    # READY
            str(self.node3_id): 300,    # INITIALIZING
            str(self.node4_id): 400,    # UNWRITABLE
            "test_key": 250,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加节点
        self.consistent_hash.add_node(self.node1)
        self.consistent_hash.add_node(self.node2)
        self.consistent_hash.add_node(self.node3)
        self.consistent_hash.add_node(self.node4)
        
        # 获取test_key(250)前后的READY状态节点
        # 前一个：200(node2)，后一个：100(node1)
        pair = self.consistent_hash.get_node_pair_in_state(
            "test_key",
            lambda n: n.state == StoreState.READY
        )
        
        self.assertIsNotNone(pair)
        prev, next_node = pair # type: ignore
        self.assertEqual(prev.store_id, self.node2_id)  # 前一个
        self.assertEqual(next_node.store_id, self.node1_id)  # 后一个

    @patch.object(ConsistentHash, '_hash')
    def test_remove_node_and_ring_integrity(self, mock_hash):
        """测试移除节点后环的完整性"""
        # 设置模拟哈希值
        hash_mapping = {
            str(self.node1_id): 100,
            str(self.node2_id): 200,
            str(self.node3_id): 300,
            "key1": 50,
            "key2": 150,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加节点
        self.consistent_hash.add_node(self.node1)
        self.consistent_hash.add_node(self.node2)
        self.consistent_hash.add_node(self.node3)
        
        # 验证初始状态
        self.assertEqual(self.consistent_hash.ring, [100, 200, 300])
        
        # 移除node2
        result = self.consistent_hash.remove_node(self.node2_id)
        self.assertTrue(result)
        
        # 验证环的状态
        self.assertEqual(self.consistent_hash.ring, [100, 300])
        self.assertEqual(self.consistent_hash.size(), 2)
        
        # 验证key2(150)现在的下一个节点是node3(300)而不是node2
        node = self.consistent_hash.get_next_node("key2", lambda n: True)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node3_id) # type: ignore

    @patch.object(ConsistentHash, '_hash')
    def test_edge_case_single_node_ring(self, mock_hash):
        """测试单节点环的特殊情况"""
        # 设置模拟哈希值
        hash_mapping = {
            str(self.node1_id): 100,
            "key_before": 50,
            "key_after": 150,
            "key_same": 100,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 只添加一个节点
        self.consistent_hash.add_node(self.node1)
        
        # 测试各种key都应该返回同一个节点
        for key in ["key_before", "key_after", "key_same"]:
            node = self.consistent_hash.get_next_node(key, lambda n: True)
            self.assertIsNotNone(node)
            self.assertEqual(node.store_id, self.node1_id) # type: ignore
            
            node = self.consistent_hash.get_prev_node(key, lambda n: True)
            self.assertIsNotNone(node)
            self.assertEqual(node.store_id, self.node1_id) # type: ignore
        
        # 测试get_nodes应该只返回一个节点
        nodes = self.consistent_hash.get_nodes("key_after")
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].store_id, self.node1_id)

    @patch.object(ConsistentHash, '_hash')
    def test_state_based_filtering(self, mock_hash):
        """测试基于状态的筛选"""
        # 设置模拟哈希值，混合状态节点
        hash_mapping = {
            str(self.node1_id): 100,  # READY
            str(self.node2_id): 200,  # READY
            str(self.node3_id): 300,  # INITIALIZING
            str(self.node4_id): 400,  # UNWRITABLE
            "test_key": 50,
        }
        mock_hash.side_effect = self.create_mock_hash_func(hash_mapping)
        
        # 添加所有节点
        self.consistent_hash.add_node(self.node1)
        self.consistent_hash.add_node(self.node2)
        self.consistent_hash.add_node(self.node3)
        self.consistent_hash.add_node(self.node4)
        
        # 测试只选择READY节点
        selector = lambda n: n.state == StoreState.READY
        node = self.consistent_hash.get_next_node("test_key", selector)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node1_id) # type: ignore
        
        # 测试只选择UNWRITABLE节点
        selector = lambda n: n.state == StoreState.LOCK
        node = self.consistent_hash.get_next_node("test_key", selector)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node4_id) # type: ignore
        
        # 测试只选择INITIALIZING节点
        selector = lambda n: n.state == StoreState.INITIALIZING
        node = self.consistent_hash.get_next_node("test_key", selector)
        self.assertIsNotNone(node)
        self.assertEqual(node.store_id, self.node3_id) # type: ignore


class TestStoreInfo(unittest.TestCase):
    """测试StoreInfo类"""
    
    def test_serialization_deserialization(self):
        """测试序列化和反序列化"""
        original = StoreInfo(
            store_id=UUID('12345678-1234-1234-1234-123456789abc'),
            addr="127.0.0.1:8000",
            state=StoreState.LOCK,
            heartbeat=123456.789
        )
        
        # 序列化
        data = original.to_dict()
        
        # 验证序列化结果
        self.assertEqual(data['store_id'], '12345678-1234-1234-1234-123456789abc')
        self.assertEqual(data['addr'], "127.0.0.1:8000")
        self.assertEqual(data['state'], StoreState.LOCK.value)
        self.assertEqual(data['heartbeat'], 123456.789)
        
        # 反序列化
        restored = StoreInfo.from_dict(data)
        
        # 验证反序列化结果
        self.assertEqual(restored.store_id, original.store_id)
        self.assertEqual(restored.addr, original.addr)
        self.assertEqual(restored.state, original.state)
        self.assertEqual(restored.heartbeat, original.heartbeat)

class TestIntegration(unittest.TestCase):
    """轻量级集成测试"""

    def setUp(self):
        self.stores = []
        self.orche = Process(target=dkvs.run_orche, args=[2, 8000], daemon=True)
        self.orche.start()
        for _ in range(3):
            self.add_store()
        time.sleep(3)

    def add_store(self):
        store = Process(target=dkvs.run_store, args=['http://localhost:8000/'], daemon=True)
        store.start()
        self.stores.append(store)

    @staticmethod
    def terminate_proc(p):
        if p.is_alive():
            p.terminate()
            p.join(timeout=2)
            if p.is_alive():
                p.kill()

    def tearDown(self):
        # 关闭 server
        self.terminate_proc(self.orche)
        for s in self.stores:
            self.terminate_proc(s)

    def append_data(self, client: dkvs.Client, sim_db: dict, n=100):
        for _ in range(n):
            k, v = str(uuid4()), str(uuid4())
            sim_db[k] = v
            ret = client.put(k, v)
            self.assertTrue(ret)
            ret = client.get(k)
            self.assertEqual(ret, v)

    def del_random_data(self, client: dkvs.Client, sim_db: dict, n=50):
        if n > len(sim_db):
            n = len(sim_db)
        del_items = random.sample(list(sim_db.items()), k=n)
        for k, v in del_items:
            ret = client.get(k)
            self.assertEqual(ret, v)
            del sim_db[k]
            ret = client.delete(k)
            self.assertTrue(ret)
            ret = client.get(k)
            self.assertIsNone(ret)

    def check_integrity(self, client: dkvs.Client, sim_db: dict, sample_k=None):
        if sample_k:
            sample_k = sample_k if sample_k <= len(sim_db) else len(sim_db)
            items = random.sample(list(sim_db.items()), k=sample_k)
        else:
            items = sim_db.items()
        for k, v in items:
            ret = client.get(k)
            self.assertEqual(ret, v)

    def test_appand(self):
        client = dkvs.Client()
        self.append_data(client, {})

    def test_appand_and_del(self):
        client = dkvs.Client()
        db = {}
        self.append_data(client, db)
        self.del_random_data(client, db)
        self.check_integrity(client, db)

    def test_scale(self):
        client = dkvs.Client()
        db = {}
        self.append_data(client, db)
        self.add_store()
        time.sleep(3)
        self.append_data(client, db)
        self.del_random_data(client, db)
        self.check_integrity(client, db)

    def test_stream10s_integrity(self):
        running = threading.Event()
        fail = threading.Event()
        running.set()

        def stream():
            client = dkvs.Client()
            try:
                db = {}
                self.append_data(client, db)
                while running.is_set():
                    op = random.choice(['put', 'get', 'del'])
                    if op == 'put':
                        self.append_data(client, db, n=1)
                    if op == 'get':
                        self.check_integrity(client, db, sample_k=1)
                    if op == 'del':
                        self.del_random_data(client, db, n=1)
            except:
                fail.set()
                raise
        
        t = threading.Thread(target=stream)
        t.start()
        
        time.sleep(10)
        
        running.clear()
        t.join()
        self.assertFalse(fail.is_set())

    def test_2streams10s_integrity(self):
        USER_N      = 4               # 并发“用户”数
        RUN_SEC     = 10              # 并发时长

        running = threading.Event()
        running.set()
        
        def user_worker(user_id: int):
            # 每个用户一份独立 db
            client = dkvs.Client()
            db = {}
            self.append_data(client, db)

            while running.is_set():
                op = random.choice(['put', 'get', 'del'])
                if op == 'put':
                    self.append_data(client, db, n=1)
                elif op == 'get':
                    self.check_integrity(client, db, sample_k=1)
                elif op == 'del':
                    self.del_random_data(client, db, n=1)

        # 启动线程池
        with ThreadPoolExecutor(max_workers=USER_N) as pool:
            futures = [pool.submit(user_worker, i) for i in range(USER_N)]

            # 主线程 sleep 足够时间
            time.sleep(RUN_SEC)
            running.clear()          # 通知所有线程结束

            # 等待全部线程完成，并“重放”异常
            for f in as_completed(futures):
                f.result()

    def test_2streams10s_integrity_with_scale(self):
        USER_N      = 4               # 并发“用户”数
        RUN_SEC     = 10              # 并发时长

        running = threading.Event()
        running.set()
        
        def user_worker(user_id: int):
            # 每个用户一份独立 db
            client = dkvs.Client()
            db = {}
            self.append_data(client, db)

            while running.is_set():
                op = random.choice(['put', 'get', 'del'])
                if op == 'put':
                    self.append_data(client, db, n=1)
                elif op == 'get':
                    self.check_integrity(client, db, sample_k=1)
                elif op == 'del':
                    self.del_random_data(client, db, n=1)

        # 启动线程池
        with ThreadPoolExecutor(max_workers=USER_N) as pool:
            futures = [pool.submit(user_worker, i) for i in range(USER_N)]

            # 主线程 sleep 足够时间
            time.sleep(RUN_SEC / 2)
            # 增加两个存储节点
            self.add_store()
            self.add_store()
            time.sleep(RUN_SEC / 2)
            running.clear()          # 通知所有线程结束

            # 等待全部线程完成，并“重放”异常
            for f in as_completed(futures):
                f.result()

    def test_monotonic_read(self):
        RUN_TIME    = 5          # 秒
        KEY_RANGE   = 3          # 模拟 10 个 key

        running = threading.Event()
        running.set()

        def read_worker():
            client = dkvs.Client()
            last_seen = [0 for _ in range(KEY_RANGE)]
            while running.is_set():
                read_no = random.randint(0, KEY_RANGE - 1)
                ret: str = client.get(str(read_no)) # type: ignore
                self.assertIsNotNone(ret)
                seq_r = int(ret)
                self.assertGreaterEqual(seq_r, last_seen[read_no])
            
        def write_worker():
            client = dkvs.Client()
            while running.is_set():
                write_no = random.randint(0, KEY_RANGE - 1)
                ret1: str = client.get(str(write_no)) # type: ignore
                self.assertIsNotNone(ret1)
                seq = int(ret1)
                seq += 1
                ret2 = client.put(str(write_no), str(seq))
                self.assertTrue(ret2)
        
        # 写入初始值
        client = dkvs.Client()
        for i in range(KEY_RANGE):
            ret = client.put(str(i), '0')
            self.assertTrue(ret)

        # 启动并发
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(read_worker), pool.submit(write_worker)]
            time.sleep(RUN_TIME)
            running.clear()
            # 把异常带回主线程
            for f in as_completed(futures):
                f.result()          # 有异常会在这里重新抛

if __name__ == "__main__":
    unittest.main()