"""
Coordinator Server for Distributed Hospital System
"""
import socket
import threading
import pickle
import json
import time
import logging
from typing import Dict
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from transaction_manager import TransactionManager, TransactionState
from lock_manager import LockManager, LockType
from failure_detector import FailureDetector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoordinatorServer:
    """Main coordinator server handling transactions and node coordination"""
    
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.transaction_manager = TransactionManager(host, port)
        self.lock_manager = LockManager()
        self.failure_detector = FailureDetector()
        self.nodes = {}  # Registered nodes
        self.running = False
        self.server_socket = None
        
    def start(self):
        """Start the coordinator server"""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.server_socket.settimeout(1)
        
        logger.info(f"Coordinator server started on {self.host}:{self.port}")
        
        # Start failure detector
        self.failure_detector.start()
        
        # Start HTTP API server in separate thread
        api_thread = threading.Thread(target=self.start_http_server, daemon=True)
        api_thread.start()
        
        # Start deadlock detection thread
        deadlock_thread = threading.Thread(target=self.deadlock_detection_loop, daemon=True)
        deadlock_thread.start()
        
        # Main server loop
        try:
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    logger.error(f"Error accepting connection: {e}")
                    continue
        except KeyboardInterrupt:
            logger.info("Shutting down coordinator server...")
        finally:
            self.stop()
    
    def start_http_server(self):
        """Start HTTP API server for web interface"""
        handler = make_http_handler(self)
        httpd = HTTPServer((self.host, 5050), handler)
        logger.info(f"HTTP API server started on {self.host}:5050")
        httpd.serve_forever()
    
    def handle_client(self, client_socket, address):
        """Handle client connection"""
        try:
            # Receive message length
            length_bytes = client_socket.recv(4)
            if not length_bytes:
                return
            
            message_length = int.from_bytes(length_bytes, 'big')
            
            # Receive message data
            message_data = b''
            while len(message_data) < message_length:
                chunk = client_socket.recv(min(4096, message_length - len(message_data)))
                if not chunk:
                    break
                message_data += chunk
            
            if len(message_data) < message_length:
                logger.error("Incomplete message received")
                return
            
            # Parse message
            message = pickle.loads(message_data)
            
            # Process message
            response = self.process_message(message, address)
            
            # Send response
            response_data = pickle.dumps(response)
            client_socket.sendall(len(response_data).to_bytes(4, 'big') + response_data)
            
        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
            response = {'success': False, 'error': str(e)}
            try:
                response_data = pickle.dumps(response)
                client_socket.sendall(len(response_data).to_bytes(4, 'big') + response_data)
            except:
                pass
        finally:
            client_socket.close()
    
    def process_message(self, message: Dict, address) -> Dict:
        """Process incoming message"""
        msg_type = message.get('type')
        
        if msg_type == 'REGISTER_NODE':
            return self.handle_node_registration(message, address)
        
        elif msg_type == 'HEARTBEAT':
            return self.handle_heartbeat(message)
        
        elif msg_type == 'BEGIN_TXN':
            return self.handle_begin_transaction(message)
        
        elif msg_type == 'PREPARE_RESPONSE':
            return self.handle_prepare_response(message)
        
        elif msg_type == 'COMMIT_RESPONSE':
            return self.handle_commit_response(message)
        
        elif msg_type == 'ABORT_RESPONSE':
            return self.handle_abort_response(message)
        
        elif msg_type == 'LOCK_REQUEST':
            return self.handle_lock_request(message)
        
        elif msg_type == 'LOCK_RELEASE':
            return self.handle_lock_release(message)
        
        elif msg_type == 'QUERY_STATUS':
            return self.handle_status_query()
        
        else:
            return {'success': False, 'error': f'Unknown message type: {msg_type}'}
    
    def handle_node_registration(self, message: Dict, address) -> Dict:
        """Handle node registration"""
        node_id = message.get('node_id')
        node_info = message.get('node_info', {})
        
        if not node_id:
            return {'success': False, 'error': 'Missing node_id'}
        
        self.nodes[node_id] = {
            **node_info,
            'address': address[0],
            'last_heartbeat': time.time(),
            'status': 'active'
        }
        
        # Register with failure detector
        self.failure_detector.register_node(node_id, node_info.get('host'), node_info.get('port'))
        
        logger.info(f"Node registered: {node_id} from {address}")
        
        return {
            'success': True,
            'message': f'Node {node_id} registered successfully',
            'coordinator_info': {
                'host': self.host,
                'port': self.port
            }
        }
    
    def handle_heartbeat(self, message: Dict) -> Dict:
        """Handle heartbeat from node"""
        node_id = message.get('node_id')
        
        if node_id in self.nodes:
            self.nodes[node_id]['last_heartbeat'] = time.time()
            self.nodes[node_id]['status'] = 'active'
            
            # Update failure detector
            self.failure_detector.update_heartbeat(node_id)
        
        return {'success': True}
    
    def handle_begin_transaction(self, message: Dict) -> Dict:
        """Handle begin transaction request"""
        operations = message.get('operations', [])
        
        if not operations:
            return {'success': False, 'error': 'No operations provided'}
        
        # Request locks for all operations
        transaction_id = self.transaction_manager.generate_transaction_id()
        
        # Acquire locks for all resources
        all_locks_acquired = True
        for op in operations:
            resource_id = f"{op['node_id']}_{op.get('resource', 'slots')}"
            lock_type = LockType.EXCLUSIVE if op.get('requires_exclusive', True) else LockType.SHARED
            
            if not self.lock_manager.request_lock(transaction_id, resource_id, lock_type):
                all_locks_acquired = False
                break
        
        if not all_locks_acquired:
            # Release any acquired locks
            self.lock_manager.release_all_locks(transaction_id)
            return {'success': False, 'error': 'Could not acquire all locks'}
        
        # Begin transaction
        txn_id = self.transaction_manager.begin_transaction(operations)
        
        return {
            'success': True,
            'transaction_id': txn_id,
            'message': 'Transaction begun, locks acquired'
        }
    
    def handle_prepare_response(self, message: Dict) -> Dict:
        """Handle prepare response from node"""
        transaction_id = message.get('transaction_id')
        node_id = message.get('node_id')
        vote = message.get('vote', False)
        
        # Record vote in transaction manager
        with self.transaction_manager.lock:
            if transaction_id in self.transaction_manager.transactions:
                transaction = self.transaction_manager.transactions[transaction_id]
                transaction.record_vote(node_id, vote)
        
        return {'success': True, 'vote_recorded': True}
    
    def handle_lock_request(self, message: Dict) -> Dict:
        """Handle lock request"""
        transaction_id = message.get('transaction_id')
        resource_id = message.get('resource_id')
        lock_type_str = message.get('lock_type', 'EXCLUSIVE')
        
        lock_type = LockType.EXCLUSIVE if lock_type_str == 'EXCLUSIVE' else LockType.SHARED
        
        granted = self.lock_manager.request_lock(transaction_id, resource_id, lock_type)
        
        return {'success': True, 'granted': granted}
    
    def handle_lock_release(self, message: Dict) -> Dict:
        """Handle lock release"""
        transaction_id = message.get('transaction_id')
        resource_id = message.get('resource_id', None)
        
        if resource_id:
            self.lock_manager.release_lock(transaction_id, resource_id)
        else:
            self.lock_manager.release_all_locks(transaction_id)
        
        return {'success': True}
    
    def handle_status_query(self) -> Dict:
        """Handle status query"""
        status = {
            'coordinator': {
                'host': self.host,
                'port': self.port,
                'status': 'running'
            },
            'nodes': self.nodes,
            'active_transactions': len(self.transaction_manager.transactions),
            'lock_status': self.lock_manager.get_lock_status(),
            'failure_detector': self.failure_detector.get_status()
        }
        
        return {'success': True, 'status': status}
    
    def deadlock_detection_loop(self):
        """Periodic deadlock detection"""
        while self.running:
            time.sleep(30)  # Check every 30 seconds
            deadlocks = self.lock_manager.detect_deadlocks()
            if deadlocks:
                logger.warning(f"Detected {len(deadlocks)} deadlock(s)")
                # In a real system, would abort transactions to resolve deadlocks
    
    def stop(self):
        """Stop the coordinator server"""
        self.running = False
        self.failure_detector.stop()
        
        if self.server_socket:
            self.server_socket.close()
        
        logger.info("Coordinator server stopped")

def make_http_handler(coordinator):
    """Create HTTP request handler with coordinator reference"""
    
    class CoordinatorHTTPHandler(BaseHTTPRequestHandler):
        
        def do_GET(self):
            parsed_path = urlparse(self.path)
            path = parsed_path.path
            
            if path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                
                html = """
                <html>
                <head><title>GROUP 4 MATIBABU REFERRAL HOSPITAL - Coordinator</title></head>
                <body>
                    <h1>GROUP 4 MATIBABU REFERRAL HOSPITAL</h1>
                    <h2>Distributed System Coordinator</h2>
                    <p><a href="/status">System Status</a></p>
                    <p><a href="/transactions">Active Transactions</a></p>
                    <p><a href="/locks">Lock Status</a></p>
                </body>
                </html>
                """
                self.wfile.write(html.encode())
                
            elif path == '/status':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                status = coordinator.handle_status_query()
                self.wfile.write(json.dumps(status, indent=2).encode())
                
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'Not Found')
        
        def do_POST(self):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            try:
                data = json.loads(post_data.decode())
                response = coordinator.process_message(data, (self.client_address[0], 0))
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode())
        
        def log_message(self, format, *args):
            logger.info(f"HTTP {format % args}")
    
    return CoordinatorHTTPHandler

if __name__ == "__main__":
    coordinator = CoordinatorServer()
    coordinator.start()