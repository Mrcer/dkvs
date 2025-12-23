from xmlrpc.client import ServerProxy
import argparse

class Client:
    def __init__(self, orchestrator_port=8000):
        self.orchestrator = ServerProxy(f'http://localhost:{orchestrator_port}', allow_none=True)
    
    def put(self, key: str, value: str) -> bool:
        """支持bytes或str"""
        return self.orchestrator.put(key, value)    # type: ignore
    
    def get(self, key: str) -> str:
        """返回值：bytes"""
        return self.orchestrator.get(key)           # type: ignore
    
    def delete(self, key: str) -> bool:
        return self.orchestrator.delete(key)  # type: ignore
    
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
                client.put(key, value)
                print(f"OK: Put '{key}'")
            
            elif op == 'GET' and len(parts) == 2:
                value = client.get(parts[1])
                print(f"OK: '{value}'")
            
            elif op == 'DEL' and len(parts) == 2:
                result = client.delete(parts[1])
                print(f"OK: Deleted" if result else "NOT_FOUND")
            
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