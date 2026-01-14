from xmlrpc.client import ServerProxy
from dkvs.hash import ConsistentHash, StoreInfo
import argparse
import logging
import time
from typing import Literal
logger = logging.getLogger('client')
logging.basicConfig(level=logging.DEBUG)

class Client:
    def __init__(self, orche_port=8000):
        self.orche_addr = f'http://localhost:{orche_port}'
        self.store_states = ConsistentHash(replicas=0)
        self.update_ring()
    
    def update_ring(self):
        logger.info("Updating ring")
        with ServerProxy(self.orche_addr, allow_none=True) as proxy:
            store_infos: list[StoreInfo] = list(map(StoreInfo.from_dict, proxy.get_ring()))  # type: ignore
            self.store_states.reset_ring(store_infos)
    
    def put(self, key: str, value: str) -> bool:
        # 重试 3 次
        for _ in range(3):
            self.update_ring()
            store_info = self.store_states.get_primary_node(key)
            if not store_info:
                logger.error("No available Store for key")
                return False
            with ServerProxy(store_info.addr, allow_none=True) as proxy:
                ret = proxy.client_put(key, value)    # type: ignore
            if ret:
                return True
            else:
                logger.warning("Put failed, retring in 1 sec")
                time.sleep(1)
        return False

    def get(self, key: str) -> str | None:
        # TODO：应该更好地报错
        # 重试 3 次
        for _ in range(3):
            self.update_ring()
            store_info = self.store_states.get_primary_node(key)
            if not store_info:
                logger.error("No available Store for key")
                continue
            with ServerProxy(store_info.addr, allow_none=True) as proxy:
                ret: Literal['NOT_FOUND', 'WRONG_NODE', 'SUCCESS', 'TEMPORARY_ERROR'] = proxy.client_get(key)           # type: ignore
                status, val = ret
            if status == 'SUCCESS':
                return val
            elif status == 'NOT_FOUND':
                return None
            else:
                logger.warning("Put failed, retring in 1 sec")
                time.sleep(1)
        return None

    def delete(self, key: str) -> bool:
        # 重试 3 次
        for _ in range(3):
            self.update_ring()
            store_info = self.store_states.get_primary_node(key)
            if not store_info:
                logger.error("No available Store for key")
                return False
            with ServerProxy(store_info.addr, allow_none=True) as proxy:
                ret = proxy.client_delete(key)        # type: ignore
            if ret:
                return True
            else:
                logger.warning("Put failed, retring in 1 sec")
                time.sleep(1)
        return False

def client_repl(client):
    """交互式命令行"""
    print("=== KV Store Client ===")
    print("Commands: PUT <key> <value> | GET <key> | DEL <key> | EXIT")
    
    while True:
        try:
            cmd = input("> ").strip()
            if not cmd:
                continue
            
            parts = cmd.split(maxsplit=2)
            op = parts[0].upper()
            
            if op == 'PUT' and len(parts) == 3:
                key, value = parts[1], parts[2]
                ret = client.put(key, value)
                print(f"OK: Put '{key}'" if ret else "Failed.")
            
            elif op == 'GET' and len(parts) == 2:
                value = client.get(parts[1])
                if value:
                    print(f"OK: '{value}'")
                else:
                    print('Key not found.')
            
            elif op == 'DEL' and len(parts) == 2:
                result = client.delete(parts[1])
                print(f"OK: Deleted" if result else 'Key not found.')
            
            elif op == 'EXIT':
                break
            else:
                print("ERROR: Invalid command")
        
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"ERROR: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    
    client_repl(Client(args.port))