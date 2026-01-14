import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
from dkvs.hash import ConsistentHash, StoreInfo, StoreState
import logging
from uuid import UUID
import time
import asyncio
import random
from typing import Literal
from dataclasses import dataclass

logger = logging.getLogger("store")
logging.basicConfig(level=logging.DEBUG)


@dataclass
class StoreData:
    value: str
    version: int

    def set(self, val: str):
        self.version += 1
        self.value = val

    def to_dict(self):
        return {"value": self.value, "version": self.version}

    @classmethod
    def from_dict(cls, d: dict):
        return StoreData(d["value"], d["version"])


class StoreDB:
    """Store的实际存储数据库（每个区间一个）"""

    def __init__(self, db_id: UUID):
        self.db_id = db_id
        self.data: dict[str, StoreData] = {}
        self.lock = threading.Lock()

    def put(self, key: str, value: str) -> tuple[bool, int]:
        """存储键值对"""
        with self.lock:
            if key not in self.data:
                self.data[key] = StoreData("", 0)
            item = self.data[key]
            item.set(value)
            logger.debug(
                f"[StoreDB-{str(self.db_id)[:5]}] PUT `{key}` `{value}`, version={item.version}"
            )
            return True, item.version

    def is_up_to_date(self, key: str, version: int) -> bool:
        with self.lock:
            if key not in self.data:
                return False
            db_v = self.data[key].version
        if db_v < version:
            # 当前一致性的数据库不应出现这种情况
            logger.warning(
                f"[StoreDB-{str(self.db_id)[:5]}] Client version of {key}({version}) is newer than db version({db_v})"
            )
            return True
        elif db_v == version:
            return True
        else:
            return False

    def get(self, key) -> tuple[str, int] | None:
        """获取值"""
        with self.lock:
            if key in self.data:
                item = self.data[key]
                logger.debug(f"[StoreDB-{str(self.db_id)[:5]}] GET `{key}`, ok")
                return item.value, item.version
            else:
                logger.debug(f"[StoreDB-{str(self.db_id)[:5]}] GET `{key}`, not found")
            return None

    def delete(self, key) -> bool:
        """删除键值对"""
        with self.lock:
            if key in self.data:
                del self.data[key]
                logger.debug(f"[StoreDB-{str(self.db_id)[:5]}] DEL `{key}`, ok")
                return True
            logger.debug(f"[StoreDB-{str(self.db_id)[:5]}] DEL `{key}`, not found")
            return False

    def merge(self, data: dict[str, StoreData]):
        """合并数据（用于备份恢复）"""
        with self.lock:
            for k, v in data.items():
                if k in self.data:
                    # TODO：当前系统不应出现这种情况，未定义行为
                    logger.warning(
                        f"[StoreDB-{str(self.db_id)[:5]}] Conflict key in MERGE data, do nothing"
                    )
                else:
                    self.data[k] = v
            logger.debug(
                f"[StoreDB-{str(self.db_id)[:5]}] MERGE data, size={len(data)}"
            )
            return True


class StoreMain:
    """Store主类"""

    def __init__(self, store_id: UUID, orche_addr: str):
        # 一致性哈希
        # TODO: 允许备份
        self.store_states_lock = threading.Lock()
        self.store_states = ConsistentHash(replicas=0)
        self.store_id = store_id

        # 每个区间一个数据库
        self.dbs: dict[UUID, StoreDB] = {}
        self.dbs[store_id] = StoreDB(store_id)

        self.orche_addr = orche_addr
        hb_thread = threading.Thread(target=self.task_heartbeat, daemon=True)
        hb_thread.start()
        self.lock_count = 0

        up_thread = threading.Thread(target=self.task_bringup, daemon=True)
        up_thread.start()

    def is_readable(self) -> bool:
        """检查Store是否准备好"""
        store_state = self.store_states.get_node_by_id(self.store_id)
        return (
            store_state.state == StoreState.READY
            or store_state.state == StoreState.LOCK
            if store_state
            else False
        )

    def is_writable(self) -> bool:
        """检查Store是否可写"""
        store_state = self.store_states.get_node_by_id(self.store_id)
        return store_state.state == StoreState.READY if store_state else False

    def primary_put(self, key: str, value: str) -> tuple[bool, int]:
        """主节点存储键值对，并同步到备份节点"""
        # 只有主节点才能处理PUT请求
        with self.store_states_lock:
            if not self.is_writable():
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] Not ready to serve requests"
                )
                return False, 0
            nodes = self.store_states.get_nodes(key)
            pnode = nodes[0]
            if pnode.store_id != self.store_id:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] PUT key `{key}` to non-primary node"
                )
                return False, 0
            db = self.dbs[self.store_id]
        # local put
        ret = db.put(key, value)
        # backup put
        # TODO：现在还不会发生，可能有错误
        # for backup_id in db_id[1:]:
        #     with ServerProxy(self.store_info[backup_id], allow_none=True) as proxy:
        #         proxy.backup_put(key, value)
        return ret

    def backup_put(self, key: str, value: str) -> bool:
        """备份节点存储键值对
        TODO：不应被调用
        """
        logging.error(
            f"[Store-{str(self.store_id)[:5]}] Not implemented: backup_put called"
        )
        # db_id = self.consistent_hash.get_primary_node(key)
        # if db_id == self.store_id:
        #     logger.warning(f"[Store-{str(self.store_id)[:5]}] BACKUP PUT key `{key}` to primary node `{db_id}`")
        #     return False
        # if self.store_id not in self.dbs:
        #         self.dbs[self.store_id] = StoreDB(self.store_id)
        # self.dbs[self.store_id].put(key, value)
        return True

    def is_up_to_date(self, key: str, version: int) -> bool:
        with self.store_states_lock:
            if not self.is_readable():
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] Not ready to serve requests"
                )
                return False
            pstore = self.store_states.get_primary_node(key)
            if pstore is None:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] GET key `{key}` but no Store nodes"
                )
                return False
            db_id = pstore.store_id
            if db_id not in self.dbs:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] GET key `{key}` from non-local node"
                )
                return False
            db = self.dbs[db_id]
        return db.is_up_to_date(key, version)

    def get(self, key: str, version: int) -> tuple[
        Literal["NOT_FOUND", "WRONG_NODE", "SUCCESS", "TEMPORARY_ERROR", "UP_TO_DATE"],
        str,
        int,
    ]:
        """从本地存储获取值，包括主存储和备份存储"""
        # 从任意StoreDB获取值
        with self.store_states_lock:
            if not self.is_readable():
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] Not ready to serve requests"
                )
                return "TEMPORARY_ERROR", "", 0
            pstore = self.store_states.get_primary_node(key)
            if pstore is None:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] GET key `{key}` but no Store nodes"
                )
                return "TEMPORARY_ERROR", "", 0
            db_id = pstore.store_id
            if db_id not in self.dbs:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] GET key `{key}` from non-local node"
                )
                return "WRONG_NODE", "", 0
            db = self.dbs[db_id]
        if db.is_up_to_date(key, version):
            logger.info(f"[Store-{str(self.store_id)[:5]}] GET key `{key}` and it's up to date")
            return "UP_TO_DATE", "", version
        v = db.get(key)
        if v:
            return "SUCCESS", v[0], v[1]
        else:
            return "NOT_FOUND", "", 0

    def get_range_data(
        self, hash_from: int, hash_to: int
    ) -> dict[str, StoreData] | None:
        """获取指定区间的数据"""
        if self.store_id not in self.dbs:
            logger.warning(
                f"[Store-{str(self.store_id)[:5]}] GET RANGE DATA but no local DB"
            )
            return None
        db = self.dbs[self.store_id]
        result = {}
        with db.lock:
            for key, value in db.data.items():
                h = self.store_states._hash(key)
                if hash_from < hash_to:
                    if hash_from < h <= hash_to:
                        result[key] = value
                else:
                    # 环形区间
                    if h > hash_from or h <= hash_to:
                        result[key] = value
        logger.info(
            f"[Store-{str(self.store_id)[:5]}] GET RANGE DATA from {hash_from} to {hash_to}, size={len(result)}"
        )
        return result

    def primary_delete(self, key: str) -> bool:
        """主节点删除键值对，并同步到备份节点"""
        # 只有主节点才能处理DELETE请求
        with self.store_states_lock:
            if not self.is_writable():
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] Not ready to serve requests"
                )
                return False
            nodes = self.store_states.get_nodes(key)
            pnode = nodes[0]
            if pnode.store_id != self.store_id:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] DEL key `{key}` to non-primary node"
                )
                return False
            db = self.dbs[self.store_id]
        # local delete
        ret = db.delete(key)
        # backup delete
        # TODO：现在还不会发生，可能有错误
        # for backup_id in db_id[1:]:
        #     with ServerProxy(self.store_info[backup_id], allow_none=True) as proxy:
        #         proxy.backup_delete(key)
        return ret

    def backup_delete(self, key: str) -> bool:
        """备份节点删除键值对
        TODO：不应被调用
        """
        logging.error(
            f"[Store-{str(self.store_id)[:5]}] Not implemented: backup_delete called"
        )
        # db_id = self.consistent_hash.get_primary_node(key)
        # if db_id == self.store_id:
        #     logger.warning(f"[Store-{str(self.store_id)[:5]}] BACKUP DEL key `{key}` to primary node `{db_id}`")
        #     return False
        # if self.store_id not in self.dbs:
        #     logger.warning(f"[Store-{str(self.store_id)[:5]}] BACKUP DEL key `{key}` but no local DB")
        #     return False
        # self.dbs[self.store_id].delete(key)
        return True

    def task_heartbeat(self, interval=10):
        """定期发送心跳到协调节点"""
        while True:
            time.sleep(interval)
            if self.store_states_lock.locked():
                self.lock_count += 1
                if self.lock_count > 1:
                    logging.warning(
                        f"[Store-{str(self.store_id)[:5]}] Lock detected for {self.lock_count} times, maybe dead lock"
                    )
            else:
                self.lock_count = 0
            with ServerProxy(self.orche_addr, allow_none=True) as proxy:
                proxy.heartbeat(str(self.store_id))

    def task_bringup(self):
        store_up = False
        while not store_up:
            time.sleep(random.random() * 2)  # 随机休眠一段时间重试
            store_up = self.bringup_store()
        self.update_ring()

    def update_ring(self):
        """主动更新哈希环"""
        with ServerProxy(self.orche_addr, allow_none=True) as proxy:
            ring: list[StoreInfo] = list(map(StoreInfo.from_dict, proxy.get_ring()))  # type: ignore
        with self.store_states_lock:
            logger.debug(f"[Store-{str(self.store_id)[:5]}] Force ring update")
            self.store_states.reset_ring(ring)

    async def shrink_store(self):
        """清除不在哈希环内的数据"""
        logger.info(
            f"[Store-{str(self.store_id)[:5]}] Shrinking store data not in ring"
        )
        with self.store_states_lock:
            # 计算本节点负责的区间
            prev_node, next_node = self.store_states.get_node_pair_in_state(
                str(self.store_id),
                lambda info: info.state is not StoreState.INITIALIZING,
            ) or (None, None)
            if not prev_node or not next_node:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] Cannot shrink store, no valid ring"
                )
                return
            hash_from = self.store_states._hash(str(prev_node.store_id))
            hash_to = self.store_states._hash(str(self.store_id))
            # 清理数据
            if self.store_id not in self.dbs:
                logger.error(f"[Store-{str(self.store_id)[:5]}] No local DB to shrink")
                return
            db = self.dbs[self.store_id]
            keys_to_delete = []
            with db.lock:
                for key in db.data.keys():
                    h = self.store_states._hash(key)
                    if hash_from < hash_to:
                        if not (hash_from < h <= hash_to):
                            keys_to_delete.append(key)
                    else:
                        # 环形区间
                        if not (h > hash_from or h <= hash_to):
                            keys_to_delete.append(key)
            for key in keys_to_delete:
                db.delete(key)
            logger.info(
                f"[Store-{str(self.store_id)[:5]}] Shrunk store data, deleted {len(keys_to_delete)} keys"
            )

    def bringup_store(self) -> bool:
        """异步初始化Store节点为ready状态，尝试拉取备份数据"""
        logger.debug(f"[Store-{str(self.store_id)[:5]}] Tring to bring up")
        self.update_ring()
        # TODO：锁优化
        with self.store_states_lock:
            prev_node, next_node = self.store_states.get_node_pair_in_state(
                str(self.store_id),
                lambda info: info.state is not StoreState.INITIALIZING,
            ) or (None, None)
            if not prev_node or not next_node:
                # 没有主节点，直接设置为ready
                logger.info(
                    f"[Store-{str(self.store_id)[:5]}] No backup store, set to READY"
                )
                with ServerProxy(self.orche_addr, allow_none=True) as proxy:
                    proxy.set_store_state(str(self.store_id), StoreState.READY.value)  # type: ignore
                return True
            # 检查备份节点是否上锁
            if next_node.state is not StoreState.READY:
                return False
            # 拉取备份数据
            logger.info(
                f"[Store-{str(self.store_id)[:5]}] Fetching backup data from {str(next_node.store_id)[:5]}"
            )
            with ServerProxy(next_node.addr, allow_none=True) as proxy:
                # 目前仅拉取主存储区间的数据
                data: dict[str, dict] | None = proxy.fetch_range_data(
                    hex(self.store_states._hash(str(prev_node.store_id))),
                    hex(self.store_states._hash(str(self.store_id))),
                )  # type: ignore
            if data is not None:
                logger.info(
                    f"[Store-{str(self.store_id)[:5]}] Merging backup data, size={len(data)}"
                )
                if self.store_id not in self.dbs:
                    self.dbs[self.store_id] = StoreDB(self.store_id)
                self.dbs[self.store_id].merge(
                    {k: StoreData.from_dict(v) for k, v in data.items()}
                )
            else:
                logger.warning(
                    f"[Store-{str(self.store_id)[:5]}] Backup data fetch failed from {str(next_node.store_id)[:5]}"
                )
                return False
            # 初始化成功，设置为ready
            with ServerProxy(self.orche_addr, allow_none=True) as proxy:
                proxy.set_store_state(str(self.store_id), StoreState.READY.value)  # type: ignore
            # 通知旧主节点主从切换
            with ServerProxy(next_node.addr, allow_none=True) as proxy:
                proxy.fetch_range_data_callback(str(self.store_id))  # type: ignore
            return True


class StoreRPC:
    """StoreRPC封装"""

    def __init__(self, store: StoreMain):
        self.store = store

    def client_put(self, key: str, value: str) -> tuple[bool, int]:
        """存储键值对"""
        return self.store.primary_put(key, value)

    def client_get(self, key: str, version: int) -> tuple[
        Literal["NOT_FOUND", "WRONG_NODE", "SUCCESS", "TEMPORARY_ERROR", "UP_TO_DATE"],
        str,
        int,
    ]:
        """获取值"""
        return self.store.get(key, version)

    def client_delete(self, key: str) -> bool:
        """删除键值对"""
        return self.store.primary_delete(key)

    def backup_put(self, key: str, value: str) -> bool:
        """备份节点存储键值对"""
        return self.store.backup_put(key, value)

    def backup_delete(self, key: str) -> bool:
        """备份节点删除键值对"""
        return self.store.backup_delete(key)

    def fetch_range_data(self, _hash_from: str, _hash_to: str) -> dict | None:
        """获取指定区间的数据
        会导致此节点不可写，直到主从切换完成
        如果数据不在本节点存储区间内，返回 None
        TODO：处理异常情况
        TODO：这些内容应该放在Main
        """
        hash_from = int(_hash_from, 16)
        hash_to = int(_hash_to, 16)
        # 检查区间是否在本节点负责范围内
        logger.info(
            f"[Store-{str(self.store.store_id)[:5]}] Backup data fetching from {_hash_from} to {_hash_to}"
        )
        with self.store.store_states_lock:
            # 节点区间不能为锁状态
            if not self.store.is_writable():
                logger.warning(
                    f"[Store-{str(self.store.store_id)[:5]}] Store is not writable, fetch failed"
                )
                return None
            store_states = self.store.store_states
            # 左开右闭区间，因此hash_from + 1属于所属区间即可
            is_from_in_range = store_states.is_in_range(
                hash_from + 1, self.store.store_id
            )
            is_to_in_range = store_states.is_in_range(hash_to, self.store.store_id)
            if not (is_from_in_range and is_to_in_range):
                logger.warning(
                    f"[Store-{str(self.store.store_id)[:5]}] Fetch out of range"
                )
                return None
        # 为节点区间上锁
        with ServerProxy(self.store.orche_addr, allow_none=True) as proxy:
            proxy.set_store_state(str(self.store.store_id), StoreState.LOCK.value)  # type: ignore
        self.store.update_ring()  # 立即让本节点不可写
        # TODO：这与 Orche 无法保证返回最新状态的假设矛盾，需要改用其他机制
        logger.info(f"[Store-{str(self.store.store_id)[:5]}] Store locked")
        # 获取数据
        data = self.store.get_range_data(hash_from, hash_to)
        if data is None:
            return None
        else:
            return {k: v.to_dict() for k, v in data.items()}

    def fetch_range_data_callback(self, node_id: str) -> bool:
        """备份数据回调接口
        当新节点完成数据拉取且ready后调用此接口以完成主从切换
        TODO：这些内容应该放在Main
        """
        logger.info(
            f"[Store-{str(self.store.store_id)[:5]}] fetch_range_data_callback called from {node_id[:5]}"
        )
        # 同步完成，设置为ready
        with ServerProxy(self.store.orche_addr, allow_none=True) as proxy:
            proxy.set_store_state(str(self.store.store_id), StoreState.READY.value)  # type: ignore
        self.store.update_ring()
        logger.info(
            f"[Store-{str(self.store.store_id)[:5]}] Backup Synced, store unlocked"
        )
        # 异步清理数据
        asyncio.run(self.store.shrink_store())
        return True

    def update_ring(self, ring):
        """被动更新哈希环"""
        with self.store.store_states_lock:
            logger.debug(f"[Store-{str(self.store.store_id)[:5]}] Force ring update")
            self.store.store_states.reset_ring(ring)


def start_rpc_on_random_port(
    port_range: range,
) -> tuple[SimpleXMLRPCServer, int] | None:
    for port in port_range:
        try:
            server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        except OSError:
            continue
        else:
            return server, port
    return None


def run_store(orche_dest, port_range=range(10000, 20000)):
    # 启动 RPC 服务器
    res = start_rpc_on_random_port(port_range)
    if not res:
        logger.info("No available port, exiting...")
        return
    server, port = res
    # 注册到协调节点同时启动 Store 实例
    orche = ServerProxy(orche_dest, allow_none=True)
    store_id: str | None = orche.register("localhost", port)  # type: ignore
    if store_id is None:
        logger.info("register failed.")
        return
    store = StoreMain(UUID(store_id), orche_dest)
    rpc = StoreRPC(store)
    server.register_instance(rpc)
    logger.info(f"Store-{store_id[:5]} started on port {port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info(f"Store-{store_id[:5]} shutting down...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--orche-dest", type=str, default=f"http://localhost:8000/", required=True
    )
    args = parser.parse_args()
    run_store(args.orche_dest)
