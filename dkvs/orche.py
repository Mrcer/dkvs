import threading
from queue import Queue
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
import logging
from dkvs.hash import ConsistentHash, StoreInfo, StoreState
import time
from uuid import UUID, uuid4
logger = logging.getLogger('orche')
logging.basicConfig(level=logging.DEBUG)

class OrcheMain:
    """协调节点
    暂时只做单协调节点
      1. 启动时有固定的地址，为 Client 和 Store 提供统一的通信地点
      2. 通过心跳监测 Store 集群状态，在 Store 状态异常时要重新分配数据分布（采用一致性哈希）
      3. 向 Client 返回 key 对应的 Store 地址列表（主存储 + 备份存储）
    """
    def __init__(self):
        # Store节点代理
        self.store_states_lock = threading.RLock()
        self.store_states = ConsistentHash(replicas=0)
        self.store_states_changed = False

        # 启动心跳检测线程
        self.running = True
        hb_thread = threading.Thread(target=self.task_stale_store_checker, daemon=True)
        hb_thread.start()

        ring_thread = threading.Thread(target=self.task_update_ring, daemon=True)
        ring_thread.start()

    def add_store(self, store_id: UUID, addr: str, state):
        """添加Store节点"""
        with self.store_states_lock:
            self.store_states.add_node(StoreInfo(store_id, addr, state, time.time()))
            self.store_states_changed = True
            logger.info(f"[Orche] Store {str(store_id)[:5]} added at {addr}, state={state}")

    def remove_store(self, store_id: UUID):
        """移除Store节点"""
        with self.store_states_lock:
            if self.store_states.remove_node(store_id):
                self.store_states_changed = True
                logger.info(f"Store {str(store_id)[:5]} removed")
            else:
                logger.warning(f"[Orche] Attempted to remove unknown Store ID: {str(store_id)[:5]}")
        
    def task_stale_store_checker(self, interval=10, timeout=30):
        """定期检查Store节点心跳，移除超时节点
        TODO：当前Store还没完成这部分逻辑处理
        """
        while self.running:
            time.sleep(interval)
            now = time.time()
            with self.store_states_lock:
                stale_stores = []
                for store_id, store_info in self.store_states.get_all_nodes().items():
                    if now - store_info.heartbeat > timeout:
                        stale_stores.append(store_id)
                for store_id in stale_stores:
                    logger.warning(f"[Orche] Store {str(store_id)[:5]} is stale, removing...")
                    self.remove_store(store_id)

    def task_update_ring(self):
        """更新哈希环"""
        while self.running and self.store_states_changed:
            logger.debug(f"[Orche] updating ring to stores")
            with self.store_states_lock:
                infos = [n.to_dict() for n in self.store_states.get_all_nodes().values()]
            for store_info in infos:
                with ServerProxy(store_info['addr'], allow_none=True) as proxy:
                    proxy.update_ring(infos)
    
class OrcheRPC:
    """协调节点RPC封装"""
    def __init__(self, orche: OrcheMain):
        self.orche = orche
    
    def get_ring(self) -> list[dict]:
        """获取当前Store节点列表"""
        return [n.to_dict() for n in self.orche.store_states.get_all_nodes().values()]

    def register(self, addr: str, port: int) -> str | None:
        """注册Store节点，返回Store ID"""
        store_id = uuid4()
        if self.orche.store_states.get_node_by_id(store_id):
            logger.warning("[Orche] uuid duplicated, register failed")
            return None
        full_addr = f"http://{addr}:{port}"
        self.orche.add_store(store_id, full_addr, state=StoreState.INITIALIZING)
        return str(store_id)

    def set_store_state(self, store_id: str, state: int) -> bool:
        """Store节点完成备份拉取，正式加入集群"""
        with self.orche.store_states_lock:
            store_info = self.orche.store_states.get_node_by_id(UUID(store_id))
            if store_info:
                store_info.state = StoreState(state)
                logger.info(f"[Orche] Store {store_id[:5]} is now in state {store_info.state.name}")
                return True
            else:
                logger.warning(f"[Orche] Unknown Store ID: {store_id[:5]}")
                return False
        self.orche.update_ring()

    def heartbeat(self, store_id: str) -> bool:
        """心跳检测"""
        with self.orche.store_states_lock:
            store_info = self.orche.store_states.get_node_by_id(UUID(store_id))
            if store_info:
                store_info.heartbeat = time.time()
                logger.debug(f"[Orche] Heartbeat received from Store {store_id[:5]}")
                return True
            else:
                logger.warning(f"[Orche] Heartbeat from unknown Store ID: {store_id[:5]}")
                return False
        
def run_orche(replicas, port=8000):
    server = SimpleXMLRPCServer(('localhost', port), allow_none=True)
    orche = OrcheMain()

    rpc = OrcheRPC(orche)
    server.register_instance(rpc, allow_dotted_names=True)
    
    logger.info(f"[Orche] Running on port {port}...")
    server.serve_forever()

if __name__ == '__main__':
    # 启动协调节点
    parser = argparse.ArgumentParser()
    parser.add_argument('--replicas', type=int, default=2, required=False)
    parser.add_argument('--port', type=str, default=8000, required=True)
    args = parser.parse_args()
    run_orche(replicas=args.replicas, port=args.port)