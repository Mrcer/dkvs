import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
import logging
logger = logging.getLogger('store')
logging.basicConfig(level=logging.DEBUG)

class Store:
    def __init__(self, store_id):
        self.store_id = store_id
        self.data = {}
        self.lock = threading.Lock()
    
    def put(self, key, value):
        """存储键值对（XML-RPC会自动包装成Binary对象）"""
        with self.lock:
            self.data[key] = value
            logger.debug(f"[Store-{self.store_id}] PUT `{key}` `{value}`")
            return True
    
    def get(self, key):
        """获取值"""
        with self.lock:
            if key in self.data:
                val = self.data[key]
                logger.debug(f"[Store-{self.store_id}] GET `{key}`, ok")
                return val
            else:
                logger.debug(f"[Store-{self.store_id}] GET `{key}`, not found")
            return ''
    
    def delete(self, key):
        """删除键值对"""
        with self.lock:
            if key in self.data:
                del self.data[key]
                logger.debug(f"[Store-{self.store_id}] DEL `{key}`, ok")
                return True
            logger.debug(f"[Store-{self.store_id}] DEL `{key}`, not found")
            return False

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