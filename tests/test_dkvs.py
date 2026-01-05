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
    """轻量级集成测试"""

    def setUp(self):
        import dkvs
        from multiprocessing import Process
        self.stores = []
        self.orche = Process(target=dkvs.run_orchestrator, args=[2, 8000], daemon=True)
        self.orche.start()
        for i in range(2):
            store = Process(target=dkvs.run_store, args=['http://localhost:8000/'], daemon=True)
            store.start()
            self.stores.append(store)
        time.sleep(0.2)
        self.client = dkvs.Client(8000)

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

    def test_end_to_end(self):
        self.assertTrue(self.client.put("integrate", "test"))
        self.assertEqual(self.client.get("integrate"), "test")
        self.assertTrue(self.client.delete("integrate"))
        self.assertEqual(self.client.get("integrate"), "")


if __name__ == "__main__":
    unittest.main()