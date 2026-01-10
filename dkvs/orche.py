import threading
from queue import Queue
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
import logging
from dkvs.hash import ConsistentHash
import time
logger = logging.getLogger('orche')
logging.basicConfig(level=logging.DEBUG)

class OrcheMain:
    """协调节点
    暂时只做单协调节点
      1. 启动时有固定的地址，为 Client 和 Store 提供统一的通信地点
      2. 通过心跳监测 Store 集群状态，在 Store 状态异常时要重新分配数据分布（采用一致性哈希）
      3. 向 Client 返回 key 对应的 Store 地址列表（主存储 + 备份存储）
    """
    def __init__(self, replicas=2):
        # Store节点代理
        self.stores_lock = threading.Lock()
        self.store_info: dict[int, tuple[str, float]] = {} # store_id -> (addr, last_heartbeat)

        self.consistent_hash = ConsistentHash(replicas=replicas)

        # 启动心跳检测线程
        self.running = True
        hb_thread = threading.Thread(target=self.task_stale_store_checker, daemon=True)
        hb_thread.start()

    def add_store(self, store_id: int, addr: str):
        """添加Store节点"""
        with self.stores_lock:
            self.store_info[store_id] = (addr, time.time())
            self.consistent_hash.add_node(store_id)
            logger.info(f"Store {store_id} added at {addr}")

    def remove_store(self, store_id: int):
        """移除Store节点"""
        with self.stores_lock:
            if store_id in self.store_info:
                del self.store_info[store_id]
                self.consistent_hash.remove_node(store_id)
                logger.info(f"Store {store_id} removed")

    def get_store_nodes(self, key: str) -> list[tuple[int, str]]:
        """获取key对应的Store节点地址列表"""
        store_ids = self.consistent_hash.get_nodes(key)
        with self.stores_lock:
            result = []
            for store_id in store_ids:
                if store_id in self.store_info:
                    addr, _ = self.store_info[store_id]
                    result.append((store_id, addr))
            return result
        
    def task_stale_store_checker(self, interval=10, timeout=30):
        """定期检查Store节点心跳，移除超时节点"""
        while self.running:
            time.sleep(interval)
            now = time.time()
            with self.stores_lock:
                stale_stores = [store_id for store_id, (_, last_hb) in self.store_info.items() if now - last_hb > timeout]
            for store_id in stale_stores:
                logger.warning(f"Store {store_id} is stale, removing...")
                # TODO: 需要重新分配数据
                self.remove_store(store_id)

class OrcheClientRPC:
    """协调节点客户端RPC封装"""
    def __init__(self, orche: OrcheMain):
        self.orche = orche
    
    def get_store_nodes(self, key: str) -> list[tuple[int, str]]:
        """获取key对应的Store节点列表"""
        return self.orche.get_store_nodes(key)
    
class OrcheStoreRPC:
    """协调节点Store端RPC封装"""
    def __init__(self, orche: OrcheMain):
        self.orche = orche
    
    def register(self, addr: str, port: int) -> int | None:
        """注册Store节点，返回Store ID"""
        # TODO：分配 ID，但Store还没Ready，应当加入待命列表
        store_id = len(self.orche.store_info) + 1
        full_addr = f"http://{addr}:{port}"
        self.orche.add_store(store_id, full_addr)
        return store_id
    
    def ready(self, store_id: int) -> bool:
        """Store节点完成备份拉取，正式加入集群"""
        # TODO: 实现正式加入集群的逻辑
        return True

    def heartbeat(self, store_id: int) -> bool:
        """心跳检测"""
        with self.orche.stores_lock:
            if store_id in self.orche.store_info:
                addr, _ = self.orche.store_info[store_id]
                self.orche.store_info[store_id] = (addr, time.time())
                return True
            else:
                logger.warning(f"Unknown Store ID: {store_id}")
                return False

        
def run_orchestrator(replicas, port=8000):
    server = SimpleXMLRPCServer(('localhost', port), allow_none=True)
    orche = OrcheMain(replicas=replicas)

    client_rpc = OrcheClientRPC(orche)
    store_rpc = OrcheStoreRPC(orche)
    
    server.register_instance(client_rpc, allow_dotted_names=True)
    server.register_instance(store_rpc, allow_dotted_names=True)
    
    logger.info(f"[Orchestrator] Running on port {port}...")
    server.serve_forever()

if __name__ == '__main__':
    # 启动协调节点
    parser = argparse.ArgumentParser()
    parser.add_argument('--replicas', type=int, default=2, required=False)
    parser.add_argument('--port', type=str, default=8000, required=True)
    args = parser.parse_args()
    run_orchestrator(replicas=args.replicas, port=args.port)