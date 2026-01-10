import hashlib
import bisect

class ConsistentHash:
    """一致性哈希实现
    暂时不实现虚拟节点
    每个Store负责一个区间的主存储，以及 R 个区间的备份存储
    一致性哈希提供key到主Store和备份Store的映射管理
    """
    def __init__(self, replicas=2):
        self.replicas = replicas
        self.ring: list[int] = []
        self.nodes: dict[int, int] = {}
    
    def clear(self):
        """清空哈希环"""
        self.ring.clear()
        self.nodes.clear()

    def reset_ring(self, nodes: list[int]):
        """重置哈希环"""
        self.clear()
        for node_id in nodes:
            self.add_node(node_id)

    def _hash(self, key: str) -> int:
        """计算哈希值"""
        h = hashlib.md5(key.encode()).hexdigest()
        return int(h, 16)
    
    def add_node(self, node_id: int):
        """添加节点"""
        h = self._hash(str(node_id))
        bisect.insort(self.ring, h)
        self.nodes[h] = node_id

    def remove_node(self, node_id: int):
        """移除节点"""
        h = self._hash(str(node_id))
        index = bisect.bisect_left(self.ring, h)
        if index < len(self.ring) and self.ring[index] == h:
            self.ring.pop(index)
            del self.nodes[h]
    
    def get_primary_node(self, key: str) -> int:
        """获取key对应的主节点"""
        if not self.ring:
            return -1
        
        h = self._hash(key)
        index = bisect.bisect(self.ring, h) % len(self.ring)
        node_hash = self.ring[index]
        return self.nodes[node_hash]

    def get_nodes(self, key: str) -> list[int]:
        """获取key对应的主节点和备份节点列表"""
        if not self.ring:
            return []
        
        h = self._hash(key)
        primary_node = self.get_primary_node(key)
        primary_index = self.ring.index(self._hash(str(primary_node)))
        
        result = []
        for i in range(self.replicas + 1):
            replica_index = (primary_index + i) % len(self.ring)
            if replica_index == primary_index and i != 0:
                break   # size() < replicas + 1，避免重复添加
            node_hash = self.ring[replica_index]
            result.append(self.nodes[node_hash])
        
        return result
    
    def size(self) -> int:
        return len(self.ring)