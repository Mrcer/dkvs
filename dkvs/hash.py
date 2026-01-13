import hashlib
import bisect
from uuid import UUID
from dataclasses import dataclass
from enum import Enum
from typing import Callable

class StoreState(Enum):
    """Store节点状态"""
    INITIALIZING = 0
    READY = 1
    LOCK = 2      # 只能读不能写，当正在迁移数据时，保证一致性使用

class StoreInfo:
    """Store节点信息"""
    def __init__(self, store_id: UUID, addr: str, state: StoreState, heartbeat: float):
        self.store_id = store_id
        self.addr = addr
        self.state = state
        self.heartbeat = heartbeat

    def to_dict(self) -> dict:
        """转换为字典（可序列化）"""
        return {
            'store_id': str(self.store_id),
            'addr': self.addr,
            'state': self.state.value,  # 使用 Enum 的值
            'heartbeat': self.heartbeat
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StoreInfo':
        """从字典反序列化"""
        return cls(
            store_id=UUID(data['store_id']),
            addr=data['addr'],
            state=StoreState(data['state']),  # 从值恢复 Enum
            heartbeat=data['heartbeat']
        )

class ConsistentHash:
    """一致性哈希实现
    暂时不实现虚拟节点
    每个Store负责一个区间的主存储，以及 R 个区间的备份存储
    一致性哈希提供key到主Store和备份Store的映射管理
    """
    def __init__(self, replicas: int):
        self.replicas = replicas
        self.ring: list[int] = []
        self.nodes: dict[int, StoreInfo] = {}
    
    def clear(self):
        """清空哈希环"""
        self.ring.clear()
        self.nodes.clear()

    def reset_ring(self, nodes: list[StoreInfo]):
        """重置哈希环"""
        self.clear()
        for node in nodes:
            self.add_node(node)
    
    def _hash(self, key: str) -> int:
        """计算哈希值"""
        h = hashlib.md5(key.encode()).hexdigest()
        return int(h, 16) % (2**32)

    def add_node(self, store_info: StoreInfo):
        """添加节点"""
        h = self._hash(str(store_info.store_id))
        bisect.insort(self.ring, h)
        self.nodes[h] = store_info

    def remove_node(self, node_id: UUID) -> bool:
        """移除节点"""
        h = self._hash(str(node_id))
        index = bisect.bisect_left(self.ring, h)
        if index < len(self.ring) and self.ring[index] == h:
            self.ring.pop(index)
            del self.nodes[h]
            return True
        else:
            return False

    def get_next_node(self, key: str, selector: Callable[[StoreInfo], bool]) -> StoreInfo | None:
        """获取key对应的下一个节点"""
        if not self.ring:
            return None

        h = self._hash(key)
        index = bisect.bisect(self.ring, h) % len(self.ring)
        for i in range(len(self.ring)):
            node_index = (index + i) % len(self.ring)
            node_hash = self.ring[node_index]
            node_info = self.nodes[node_hash]
            if selector(node_info):
                return node_info
        return None

    def get_prev_node(self, key: str, selector: Callable[[StoreInfo], bool]) -> StoreInfo | None:
        """获取key对应的上一个节点"""
        if not self.ring:
            return None
        
        h = self._hash(key)
        index = bisect.bisect_left(self.ring, h) - 1
        for i in range(len(self.ring)):
            node_index = (index - i) % len(self.ring)   # amazing python negative modulo
            node_hash = self.ring[node_index]
            node_info = self.nodes[node_hash]
            if selector(node_info):
                return node_info
        return None

    def get_node_pair_in_state(self, key: str, selector: Callable[[StoreInfo], bool]) -> tuple[StoreInfo, StoreInfo] | None:
        """获取key前后的指定状态节点"""
        prev = self.get_prev_node(key, selector)
        if not prev:
            return None
        next = self.get_next_node(key, selector)
        if not next:
            return None
        return (prev, next)

    def get_primary_node(self, key: str) -> StoreInfo | None:
        """获取key对应的主节点"""
        pnode = self.get_next_node(key, lambda node: node.state is not StoreState.INITIALIZING)
        return pnode

    def get_nodes(self, key: str) -> list[StoreInfo]:
        """获取key对应的主节点和备份节点列表"""
        if not self.ring:
            return []
        
        h = self._hash(key)
        primary_index = bisect.bisect(self.ring, h) % len(self.ring)
        result = []
        for i in range(len(self.ring)):
            node_index = (primary_index + i) % len(self.ring)
            node_hash = self.ring[node_index]
            node_info = self.nodes[node_hash]
            if node_info.state is not StoreState.INITIALIZING:
                result.append(node_info)
            if len(result) == self.replicas + 1:
                break

        return result
    
    def get_node_by_id(self, node_id: UUID) -> StoreInfo | None:
        """通过节点ID获取节点信息"""
        h = self._hash(str(node_id))
        return self.nodes.get(h, None)

    def get_all_nodes(self) -> dict[UUID, StoreInfo]:
        """获取所有节点信息"""
        return {info.store_id: info for info in self.nodes.values()}

    def is_in_range(self, h: int, pnode: UUID) -> bool:
        """判断hash值是否在某节点负责的区间内"""
        node_info = self.get_node_by_id(pnode)
        if not node_info:
            return False
        # 计算区间
        node_hash_to = self._hash(str(pnode))
        prev_node = self.get_prev_node(str(pnode), lambda n: n.state is not StoreState.INITIALIZING)
        if not prev_node:
            return False
        node_hash_from = self._hash(str(prev_node.store_id))
        # 判断区间包含关系
        if node_hash_from < node_hash_to:
            return node_hash_from < h <= node_hash_to
        else:
            return h > node_hash_from or h <= node_hash_to

    def size(self) -> int:
        return len(self.ring)