"""
Hospital Department Node Server
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

from data_store import DataStore, SlotStatus

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NodeServer:
    """Server for a hospital department node"""
    
    def __init__(self, node_id: str, node_name: str, host: str, port: int, 
                 total_slots: int, coordinator_host: str, coordinator_port: int):
        self.node_id = node_id
        self.node_name = node_name
        self.host = host
        self.port = port
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.data_store = DataStore(node_id, node_name, total_slots)
        self.running = False
        self.server_socket = None
        self.heartbeat_thread = None
        self.registered = False
        
    def start(self):
        """Start the node server"""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.server_socket.settimeout(1)
        
        logger.info(f"{self.node_name} server started on {self.host}:{self.port}")
        
        # Register with coordinator
        self.register_with_coordinator()
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
        # Start HTTP server for web interface
        http_thread = threading.Thread(target=self.start_http_server, daemon=True)
        http_thread.start()
        
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
            logger.info(f"Shutting down {self.node_name} server...")
        finally:
            self.stop()
    
    def start_http_server(self):
        """Start HTTP server for web interface"""
        handler = make_node_http_handler(self)
        httpd = HTTPServer((self.host, self.port + 100), handler)
        logger.info(f"HTTP server started on {self.host}:{self.port + 100}")
        httpd.serve_forever()
    
    def register_with_coordinator(self):
        """Register this node with the coordinator"""
        try:
            message = {
                'type': 'REGISTER_NODE',
                'node_id': self.node_id,
                'node_info': {
                    'name': self.node_name,
                    'host': self.host,
                    'port': self.port,
                    'total_slots': self.data_store.total_slots,
                    'available_slots': self.data_store.available_slots
                }
            }
            
            response = self.send_to_coordinator(message)
            
            if response.get('success'):
                self.registered = True
                logger.info(f"Successfully registered with coordinator")
            else:
                logger.error(f"Failed to register with coordinator: {response.get('error')}")
                
        except Exception as e:
            logger.error(f"Error registering with coordinator: {e}")
    
    def heartbeat_loop(self):
        """Send periodic heartbeats to coordinator"""
        while self.running:
            time.sleep(5)  # Send heartbeat every 5 seconds
            
            if self.registered:
                try:
                    message = {
                        'type': 'HEARTBEAT',
                        'node_id': self.node_id,
                        'timestamp': time.time(),
                        'status': self.data_store.get_department_status()
                    }
                    
                    self.send_to_coordinator(message)
                    
                except Exception as e:
                    logger.error(f"Error sending heartbeat: {e}")
                    self.registered = False
                    # Try to re-register
                    self.register_with_coordinator()
    
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
        
        if msg_type == 'PREPARE':
            return self.handle_prepare(message)
        
        elif msg_type == 'COMMIT':
            return self.handle_commit(message)
        
        elif msg_type == 'ABORT':
            return self.handle_abort(message)
        
        elif msg_type == 'QUERY_SLOTS':
            return self.handle_query_slots()
        
        elif msg_type == 'BOOK_SLOT':
            return self.handle_book_slot(message)
        
        elif msg_type == 'NODE_STATUS':
            return self.handle_node_status()
        
        else:
            return {'success': False, 'error': f'Unknown message type: {msg_type}'}
    
    def handle_prepare(self, message: Dict) -> Dict:
        """Handle prepare request from coordinator"""
        transaction_id = message.get('transaction_id')
        operations = message.get('operations', [])
        
        if not transaction_id:
            return {'success': False, 'error': 'Missing transaction_id', 'vote': False}
        
        # Begin transaction locally
        if not self.data_store.begin_transaction(transaction_id):
            return {'success': False, 'error': 'Transaction already exists', 'vote': False}
        
        # Apply operations
        all_operations_successful = True
        for op in operations:
            if op.get('action') == 'BOOK':
                slot_id = op.get('slot_id')
                patient_id = op.get('patient_id')
                patient_name = op.get('patient_name')
                
                if not self.data_store.book_appointment(transaction_id, slot_id, patient_id, patient_name):
                    all_operations_successful = False
                    break
        
        if all_operations_successful:
            # Prepare transaction
            if self.data_store.prepare_transaction(transaction_id):
                return {'success': True, 'vote': True, 'message': 'Prepared successfully'}
            else:
                return {'success': False, 'vote': False, 'error': 'Prepare failed'}
        else:
            # Abort locally
            self.data_store.abort_transaction(transaction_id)
            return {'success': False, 'vote': False, 'error': 'Operations failed'}
    
    def handle_commit(self, message: Dict) -> Dict:
        """Handle commit request from coordinator"""
        transaction_id = message.get('transaction_id')
        
        if not transaction_id:
            return {'success': False, 'error': 'Missing transaction_id'}
        
        if self.data_store.commit_transaction(transaction_id):
            return {'success': True, 'message': 'Committed successfully'}
        else:
            return {'success': False, 'error': 'Commit failed'}
    
    def handle_abort(self, message: Dict) -> Dict:
        """Handle abort request from coordinator"""
        transaction_id = message.get('transaction_id')
        
        if not transaction_id:
            return {'success': False, 'error': 'Missing transaction_id'}
        
        if self.data_store.abort_transaction(transaction_id):
            return {'success': True, 'message': 'Aborted successfully'}
        else:
            return {'success': False, 'error': 'Abort failed'}
    
    def handle_query_slots(self) -> Dict:
        """Handle slot query"""
        slots = self.data_store.get_available_slots()
        status = self.data_store.get_department_status()
        
        return {
            'success': True,
            'slots': slots,
            'status': status
        }
    
    def handle_book_slot(self, message: Dict) -> Dict:
        """Handle direct slot booking request"""
        slot_id = message.get('slot_id')
        patient_id = message.get('patient_id')
        patient_name = message.get('patient_name')
        
        if not all([slot_id, patient_id, patient_name]):
            return {'success': False, 'error': 'Missing required fields'}
        
        # Request lock from coordinator
        transaction_id = f"DIRECT_{int(time.time())}"
        
        lock_request = {
            'type': 'LOCK_REQUEST',
            'transaction_id': transaction_id,
            'resource_id': f"{self.node_id}_slots",
            'lock_type': 'EXCLUSIVE'
        }
        
        lock_response = self.send_to_coordinator(lock_request)
        
        if not lock_response.get('granted', False):
            return {'success': False, 'error': 'Could not acquire lock'}
        
        # Begin transaction
        if not self.data_store.begin_transaction(transaction_id):
            self.send_to_coordinator({
                'type': 'LOCK_RELEASE',
                'transaction_id': transaction_id
            })
            return {'success': False, 'error': 'Transaction failed'}
        
        # Book slot
        if not self.data_store.book_appointment(transaction_id, slot_id, patient_id, patient_name):
            self.data_store.abort_transaction(transaction_id)
            self.send_to_coordinator({
                'type': 'LOCK_RELEASE',
                'transaction_id': transaction_id
            })
            return {'success': False, 'error': 'Slot not available'}
        
        # Prepare and commit
        if self.data_store.prepare_transaction(transaction_id) and \
           self.data_store.commit_transaction(transaction_id):
            
            self.send_to_coordinator({
                'type': 'LOCK_RELEASE',
                'transaction_id': transaction_id
            })
            
            return {'success': True, 'message': 'Slot booked successfully'}
        else:
            self.data_store.abort_transaction(transaction_id)
            self.send_to_coordinator({
                'type': 'LOCK_RELEASE',
                'transaction_id': transaction_id
            })
            return {'success': False, 'error': 'Booking failed'}
    
    def handle_node_status(self) -> Dict:
        """Handle node status query"""
        status = self.data_store.get_department_status()
        
        return {
            'success': True,
            'node_id': self.node_id,
            'node_name': self.node_name,
            'status': status,
            'registered': self.registered
        }
    
    def send_to_coordinator(self, message: Dict) -> Dict:
        """Send message to coordinator"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.coordinator_host, self.coordinator_port))
                
                # Send message
                data = pickle.dumps(message)
                s.sendall(len(data).to_bytes(4, 'big') + data)
                
                # Receive response
                response_length = int.from_bytes(s.recv(4), 'big')
                response_data = b''
                while len(response_data) < response_length:
                    chunk = s.recv(min(4096, response_length - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                return pickle.loads(response_data)
                
        except Exception as e:
            logger.error(f"Error communicating with coordinator: {e}")
            return {'success': False, 'error': str(e)}
    
    def stop(self):
        """Stop the node server"""
        self.running = False
        
        if self.server_socket:
            self.server_socket.close()
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        
        logger.info(f"{self.node_name} server stopped")

def make_node_http_handler(node):
    """Create HTTP request handler for node"""
    
    class NodeHTTPHandler(BaseHTTPRequestHandler):
        
        def do_GET(self):
            parsed_path = urlparse(self.path)
            path = parsed_path.path
            
            if path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                
                status = node.data_store.get_department_status()
                slots = node.data_store.get_available_slots()
                
                html = f"""
                <html>
                <head>
                    <title>GROUP 4 MATIBABU REFERRAL HOSPITAL - {node.node_name}</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        h1 {{ color: #2c3e50; }}
                        h2 {{ color: #3498db; }}
                        .slot {{ 
                            background: #ecf0f1; 
                            padding: 10px; 
                            margin: 5px; 
                            border-radius: 5px;
                            display: inline-block;
                        }}
                        .available {{ background: #2ecc71; color: white; }}
                        .booked {{ background: #e74c3c; color: white; }}
                        .pending {{ background: #f39c12; color: white; }}
                    </style>
                </head>
                <body>
                    <h1>GROUP 4 MATIBABU REFERRAL HOSPITAL</h1>
                    <h2>{node.node_name}</h2>
                    
                    <div>
                        <h3>Department Status</h3>
                        <p>Total Slots: {status['total_slots']}</p>
                        <p>Available Slots: {status['available_slots']}</p>
                        <p>Booked Slots: {status['booked_slots']}</p>
                        <p>Pending Slots: {status['pending_slots']}</p>
                    </div>
                    
                    <div>
                        <h3>Available Slots</h3>
                        {"".join(f'<div class="slot available">{s["time"]} - {s["date"]}</div>' for s in slots)}
                    </div>
                </body>
                </html>
                """
                self.wfile.write(html.encode())
                
            elif path == '/status':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                status = node.handle_node_status()
                self.wfile.write(json.dumps(status, indent=2).encode())
                
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'Not Found')
        
        def log_message(self, format, *args):
            logger.info(f"HTTP {format % args}")
    
    return NodeHTTPHandler

# Individual node starter scripts would go here
# node1_cardiology.py, node2_radiology.py, etc.