# tests/test_dkvs.py
import unittest
import threading
import time
import xmlrpc.server
from xmlrpc.client import ServerProxy

from dkvs.orche import Orchestrator
from dkvs.store import Store
from dkvs.client import Client


class TestStore(unittest.TestCase):
    """单节点 Store 的单元测试（本地调用，不走 RPC）"""

    def setUp(self):
        self.store = Store(store_id=99)

    def test_put_get(self):
        self.assertTrue(self.store.put("k1", "v1"))
        self.assertEqual(self.store.get("k1"), "v1")

    def test_get_nonexist(self):
        self.assertEqual(self.store.get("no-such-key"), "")

    def test_delete(self):
        self.store.put("k2", "v2")
        self.assertTrue(self.store.delete("k2"))
        self.assertEqual(self.store.get("k2"), "")
        self.assertFalse(self.store.delete("k2"))  # 再次删除返回 False


class TestOrchestrator(unittest.TestCase):
    """Orchestrator 的本地测试，用 MockStore 避免真实 RPC"""

    class MockStore:
        def __init__(self):
            self.d = {}

        def put(self, k, v):
            self.d[k] = v
            return True

        def get(self, k):
            return self.d.get(k, "")

        def delete(self, k):
            return bool(self.d.pop(k, None))

    def setUp(self):
        self.orch = Orchestrator(num_stores=2)
        # 手动注入 mock store
        self.orch.stores[0] = self.MockStore()  # type: ignore
        self.orch.stores[1] = self.MockStore()  # type: ignore

    def test_hash_distribution(self):
        """简单验证哈希分片"""
        self.assertEqual(self.orch._get_store_id("aaa"),
                         hash("aaa") % 2)

    def test_put_get(self):
        self.orch.put("x", "123")
        time.sleep(0.1)  # 等异步 worker 写完
        self.assertEqual(self.orch.get("x"), "123")

    def test_delete(self):
        self.orch.put("del-key", "val")
        time.sleep(0.1)
        self.assertTrue(self.orch.delete("del-key"))
        self.assertEqual(self.orch.get("del-key"), "")

    def test_monotonic_read(self):
        """单调读：写完再读，一定能读到"""
        self.orch.put("mr", "first")
        time.sleep(0.1)
        self.assertEqual(self.orch.get("mr"), "first")
        self.orch.put("mr", "second")
        time.sleep(0.1)
        self.assertEqual(self.orch.get("mr"), "second")

    def test_queue_dedup(self):
        """同一个 key 连续 put 两次，worker 应能正确处理"""
        self.orch.put("dup", "v1")
        self.orch.put("dup", "v2")
        time.sleep(0.2)
        # 最终值应为 v2
        self.assertEqual(self.orch.get("dup"), "v2")


class TestClient(unittest.TestCase):
    """Client 的单元测试，用 MockOrchestrator 避免真实 RPC"""

    class MockOrchestrator:
        def __init__(self):
            self.data = {}

        def put(self, k, v):
            self.data[k] = v
            return True

        def get(self, k):
            return self.data.get(k, "")

        def delete(self, k):
            return bool(self.data.pop(k, None))

    def setUp(self):
        self.client = Client()
        # 替换掉真实的 orchestrator
        self.client.orchestrator = self.MockOrchestrator()  # type: ignore

    def test_put_get_delete(self):
        self.assertTrue(self.client.put("ck", "cv"))
        self.assertEqual(self.client.get("ck"), "cv")
        self.assertTrue(self.client.delete("ck"))
        self.assertEqual(self.client.get("ck"), "")


class TestIntegration(unittest.TestCase):
    """轻量级集成测试：真实 XML-RPC，但全部在本机随机端口完成"""

    @staticmethod
    def _pick_free_port():
        import socket
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def setUp(self):
        # 1. 启动 Orchestrator
        self.orch_port = self._pick_free_port()
        self.orch_server = xmlrpc.server.SimpleXMLRPCServer(
            ("localhost", self.orch_port), allow_none=True
        )
        self.orch = Orchestrator(num_stores=2)
        self.orch_server.register_instance(self.orch)
        self.orch_thread = threading.Thread(target=self.orch_server.serve_forever)
        self.orch_thread.daemon = True
        self.orch_thread.start()

        # 2. 启动两个 Store
        self.store_servers = []
        for _ in range(2):
            port = self._pick_free_port()
            srv = xmlrpc.server.SimpleXMLRPCServer(("localhost", port), allow_none=True)
            store = Store(store_id=-1)  # id 将由 register 返回
            srv.register_instance(store)
            th = threading.Thread(target=srv.serve_forever)
            th.daemon = True
            th.start()
            self.store_servers.append((srv, store, th))
            # 注册到 orchestrator
            self.orch.register("localhost", port)

        # 3. 等注册完成
        time.sleep(0.2)

        # 4. 启动 Client
        self.client = Client(self.orch_port)

    def tearDown(self):
        # 关闭 server
        self.orch_server.shutdown()
        for srv, _, _ in self.store_servers:
            srv.shutdown()

    def test_end_to_end(self):
        self.assertTrue(self.client.put("integrate", "test"))
        time.sleep(0.2)  # 等异步写
        self.assertEqual(self.client.get("integrate"), "test")
        self.assertTrue(self.client.delete("integrate"))
        self.assertEqual(self.client.get("integrate"), "")


if __name__ == "__main__":
    unittest.main()