import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
from dkvs.hash import ConsistentHash
import logging
logger = logging.getLogger('store')
logging.basicConfig(level=logging.DEBUG)

class StoreDB:
    """Store的实际存储数据库（每个区间一个）
    """
    def __init__(self, db_id: int):
        self.db_id = db_id
        self.data = {}
        self.lock = threading.Lock()
    
    def put(self, key, value):
        """存储键值对"""
        with self.lock:
            self.data[key] = value
            logger.debug(f"[StoreDB-{self.db_id}] PUT `{key}` `{value}`")
            return True
    
    def get(self, key):
        """获取值"""
        with self.lock:
            if key in self.data:
                val = self.data[key]
                logger.debug(f"[StoreDB-{self.db_id}] GET `{key}`, ok")
                return val
            else:
                logger.debug(f"[StoreDB-{self.db_id}] GET `{key}`, not found")
            return ''
    
    def delete(self, key):
        """删除键值对"""
        with self.lock:
            if key in self.data:
                del self.data[key]
                logger.debug(f"[StoreDB-{self.db_id}] DEL `{key}`, ok")
                return True
            logger.debug(f"[StoreDB-{self.db_id}] DEL `{key}`, not found")
            return False

    def merge(self, data: dict):
        """合并数据（用于备份恢复）"""
        with self.lock:
            self.data.update(data)
            logger.debug(f"[StoreDB-{self.db_id}] MERGE data, size={len(data)}")
            return True

class StoreMain:
    """Store主类
    """
    def __init__(self, store_id: int, replicas=2):
        
        # 一致性哈希，结构为：[-1, backup-R, ..., backup-1, primary]
        self.consistent_hash = ConsistentHash(replicas=replicas)
        self.store_id = store_id
        self.store_info: dict[int, str] = {} # store_id -> addr

        # 每个区间一个数据库
        self.dbs: dict[int, StoreDB] = {}
        self.dbs[store_id] = StoreDB(store_id)

    def primary_put(self, key: str, value: str) -> bool:
        """主节点存储键值对，并同步到备份节点"""
        db_id = self.consistent_hash.get_nodes(key)
        if db_id[0] != self.store_id:
            logger.warning(f"[Store-{self.store_id}] PUT key `{key}` to non-primary node `{db_id[0]}`")
            return False
        # local put
        self.dbs[self.store_id].put(key, value)
        # backup put
        for backup_id in db_id[1:]:
            with ServerProxy(self.store_info[backup_id], allow_none=True) as proxy:
                proxy.backup_put(key, value)
        return True
    
    def backup_put(self, key: str, value: str) -> bool:
        """备份节点存储键值对"""
        db_id = self.consistent_hash.get_primary_node(key)
        if db_id == self.store_id:
            logger.warning(f"[Store-{self.store_id}] BACKUP PUT key `{key}` to primary node `{db_id}`")
            return False
        if self.store_id not in self.dbs:
                self.dbs[self.store_id] = StoreDB(self.store_id)
        self.dbs[self.store_id].put(key, value)
        return True
    
    def get(self, key: str) -> str | None:
        """从本地存储获取值，包括主存储和备份存储"""
        db_id = self.consistent_hash.get_primary_node(key)
        if db_id not in self.dbs:
            logger.warning(f"[Store-{self.store_id}] GET key `{key}` from non-local node `{db_id}`")
            return None
        return self.dbs[db_id].get(key)

    def primary_delete(self, key: str) -> bool:
        """主节点删除键值对，并同步到备份节点"""
        db_id = self.consistent_hash.get_nodes(key)
        if db_id[0] != self.store_id:
            logger.warning(f"[Store-{self.store_id}] DEL key `{key}` to non-primary node `{db_id[0]}`")
            return False
        # local delete
        self.dbs[self.store_id].delete(key)
        # backup delete
        for backup_id in db_id[1:]:
            with ServerProxy(self.store_info[backup_id], allow_none=True) as proxy:
                proxy.backup_delete(key)
        return True
    
    def backup_delete(self, key: str) -> bool:
        """备份节点删除键值对"""
        db_id = self.consistent_hash.get_primary_node(key)
        if db_id == self.store_id:
            logger.warning(f"[Store-{self.store_id}] BACKUP DEL key `{key}` to primary node `{db_id}`")
            return False
        if self.store_id not in self.dbs:
            logger.warning(f"[Store-{self.store_id}] BACKUP DEL key `{key}` but no local DB")
            return False
        self.dbs[self.store_id].delete(key)
        return True
    
    def backup_recover(self, data: dict) -> bool:
        """备份恢复数据"""
        if self.store_id not in self.dbs:
            self.dbs[self.store_id] = StoreDB(self.store_id)
        self.dbs[self.store_id].merge(data)
        return True
    
class StoreClientRPC:
    """Store客户端RPC封装"""
    def __init__(self, store: StoreMain):
        self.store = store

    def put(self, key: str, value: str) -> bool:
        """存储键值对"""
        return self.store.primary_put(key, value)

    def get(self, key: str) -> str:
        """获取值"""
        return self.store.get(key) or ''

    def delete(self, key: str) -> bool:
        """删除键值对"""
        return self.store.primary_delete(key)
    
class StoreOrcheRPC:
    """Store协调节点RPC封装"""
    def __init__(self, store: StoreMain):
        self.store = store

    def update_ring(self, nodes: list[tuple[int, str]]) -> bool:
        """异步更新数据库信息"""
        logger.info(f"[Store-{self.store.store_id}] update_ring called with nodes: {nodes}")
        for store_id, addr in nodes:
            self.store.store_info[store_id] = addr
        self.store.consistent_hash.reset_ring([store_id for store_id, _ in nodes])
        return True

class StoreStoreRPC:
    """Store间RPC封装"""
    def __init__(self, store: StoreMain):
        self.store = store

    def backup_put(self, key: str, value: str) -> bool:
        """备份节点存储键值对"""
        return self.store.backup_put(key, value)
    
    def backup_delete(self, key: str) -> bool:
        """备份节点删除键值对"""
        return self.store.backup_delete(key)

    def fetch_range_data(self, hash_from: int, hash_to: int) -> dict:
        """获取指定区间的数据（用于备份恢复）"""
        result = {}
        for db in self.store.dbs.values():
            with db.lock:
                for key, value in db.data.items():
                    h = self.store.consistent_hash._hash(key)
                    if hash_from <= h < hash_to:
                        result[key] = value
        logger.debug(f"[Store-{self.store.store_id}] fetch_range_data from {hash_from} to {hash_to}, size={len(result)}")
        return result

def register(orche_dest, store_addr, store_port):
    orche = ServerProxy(orche_dest, allow_none=True)
    return orche.register(store_addr, store_port)

def start_server(port_range: range) -> tuple[SimpleXMLRPCServer, int] | None:
    for port in port_range:
        try:
            server = SimpleXMLRPCServer(('localhost', port), allow_none=True)
        except OSError:
            continue
        else:
            return server, port
    return None
    

def run_store(orche_dest, port_range=range(10000, 20000)):
    res = start_server(port_range)
    if not res:
        logger.info("[Store] No available port, exiting...")
        return
    server, port = res
    store_id = register(orche_dest, 'localhost', port)
    if store_id is None:
        logger.info("[Store] register failed.")
        return
    store = Store(store_id)
    server.register_instance(store)
    logger.info(f"Store-{store_id} started on port {port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info(f"Store-{store_id} shutting down...")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--orche-dest', type=str, default=f'http://localhost:8000/', required=True)
    args = parser.parse_args()
    run_store(args.orche_dest)