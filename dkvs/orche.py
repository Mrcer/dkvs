"""
需要解决缓存问题
"""
import threading
from queue import Queue
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
import logging
logger = logging.getLogger('orche')
logging.basicConfig(level=logging.DEBUG)

class Orchestrator:
    def __init__(self, num_stores=2):
        # Store节点代理
        self.stores_lock = threading.Lock()
        self.store_urls: dict[int, str] = {}
        self.num_stores = num_stores
        
        # 提交过程：请求加入队列write_queue异步处理，数据保存在commit_table
        # 单调读：读提交保证多次读取不会读到更旧的数据
        # 单调写：带锁的提交表保证多次写只会保留最新版本
        self.commit_table_lock = threading.Lock()           # 提交表全局锁
        self.commit_table: dict[str, str] = {}         # 提交表，None表示数据提交中，但不脏，否则为脏
        self.write_queue: Queue[str] = Queue()
        
        # Worker线程
        self.worker_thread = threading.Thread(target=self._writer_worker, daemon=True)
        self.worker_thread.start()
    
    def _get_store_id(self, key: str):
        """简单哈希取模"""
        return hash(key) % self.num_stores
    
    def _writer_worker(self):
        """顺序处理写入队列"""
        while True:
            key = self.write_queue.get()
            store_id = self._get_store_id(key)
            
            try:
                with self.commit_table_lock:
                    if key not in self.commit_table:
                        # 有两种可能：
                        # 1. 队列中有多个写请求，而先前的请求已经处理了该请求的提交，因此可以跳过
                        # 2. 在完成持久化前，数据就被删除了
                        continue
                    else:
                        value = self.commit_table.pop(key)
                proxy = ServerProxy(self.store_urls[store_id], allow_none=True)
                success = proxy.put(key, value)
                if not success:
                    logger.error(f"Failed to write: key[:16]={key[:16]}")
            except Exception as e:
                logger.error(f"Error writing to Store-{store_id}: {e}")
    
    # RPC接口（供Client调用）
    def put(self, key, value) -> bool:
        """客户端写入：只入队，不等待"""
        if len(self.store_urls) != self.num_stores:
            logger.warning(f"Store not ready!")

        with self.commit_table_lock:
            self.commit_table[key] = value
        self.write_queue.put(key)
        
        return True  # 已接受，异步写入
    
    def get(self, key) -> str:
        """客户端读取：先查缓存，保证读到最新修改"""
        if len(self.store_urls) != self.num_stores:
            logger.warning(f"Store not ready!")

        store_id = self._get_store_id(key)
        proxy = ServerProxy(self.store_urls[store_id], allow_none=True)
        with self.commit_table_lock:
            if key in self.commit_table:
                return self.commit_table[key]
        try:
            value = proxy.get(key)
            return value    # type: ignore
        except Exception as e:
            logger.error(f"Error reading from Store-{store_id}: {e}")
            return ''
    
    def delete(self, key):
        """客户端删除：删除缓存和远程存储"""
        if len(self.store_urls) != self.num_stores:
            logger.warning(f"Store not ready!")

        success = False
        with self.commit_table_lock:
            if key in self.commit_table:
                del self.commit_table[key]
                success = True
        store_id = self._get_store_id(key)
        proxy = ServerProxy(self.store_urls[store_id], allow_none=True)
        try:
            ret = proxy.delete(key)
            success = ret or success
            return success
        except Exception as e:
            logger.error(f"Error deleting from Store-{store_id}: {e}")
            return False

    # RPC接口（供Store调用）  
    def register(self, addr, port):
        """存储节点注册服务"""
        if len(self.store_urls) == self.num_stores:
            logger.warning("Extra store is abandoned!")
            return None
        with self.stores_lock:
            id = len(self.store_urls)
            self.store_urls[id] = f'http://{addr}:{port}'
            logger.info(f"Store-{id} ready!")
            if len(self.store_urls) == self.num_stores:
                logger.info("All stores ready!")
            return id

def run_orchestrator(num_stores=2, port=8000):
    orchestrator = Orchestrator(num_stores)
    
    server = SimpleXMLRPCServer(('localhost', port), allow_none=True)
    server.register_instance(orchestrator)
    
    logger.info(f"Started on port {port}, waiting for {num_stores} stores...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-stores', type=int, default=2)
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    
    run_orchestrator(args.num_stores, args.port)