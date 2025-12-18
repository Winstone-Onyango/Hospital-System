"""
Failure Detector for monitoring node health
"""
import threading
import time
import logging
from typing import Dict, Set
import socket
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FailureDetector:
    """Monitors node health and detects failures"""
    
    def __init__(self, heartbeat_interval: int = 5, timeout: int = 10):
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout
        self.nodes: Dict[str, Dict] = {}  # node_id -> node info
        self.last_heartbeats: Dict[str, float] = {}
        self.suspected_nodes: Set[str] = set()
        self.running = False
        self.monitor_thread = None
        self.lock = threading.Lock()
        
    def register_node(self, node_id: str, host: str, port: int):
        """Register a node for monitoring"""
        with self.lock:
            self.nodes[node_id] = {
                'host': host,
                'port': port,
                'status': 'active',
                'registered_at': time.time()
            }
            self.last_heartbeats[node_id] = time.time()
            if node_id in self.suspected_nodes:
                self.suspected_nodes.remove(node_id)
            
        logger.info(f"Registered node {node_id} for failure detection")
    
    def update_heartbeat(self, node_id: str):
        """Update heartbeat for a node"""
        with self.lock:
            if node_id in self.last_heartbeats:
                self.last_heartbeats[node_id] = time.time()
                if node_id in self.suspected_nodes:
                    self.suspected_nodes.remove(node_id)
                self.nodes[node_id]['status'] = 'active'
    
    def start(self):
        """Start failure detection monitoring"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self.monitor_nodes, daemon=True)
        self.monitor_thread.start()
        logger.info("Failure detector started")
    
    def stop(self):
        """Stop failure detection"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Failure detector stopped")
    
    def monitor_nodes(self):
        """Monitor nodes for failures"""
        while self.running:
            time.sleep(self.heartbeat_interval)
            self.check_nodes()
    
    def check_nodes(self):
        """Check all registered nodes for heartbeats"""
        current_time = time.time()
        nodes_to_check = []
        
        with self.lock:
            for node_id in list(self.nodes.keys()):
                last_heartbeat = self.last_heartbeats.get(node_id, 0)
                time_since_heartbeat = current_time - last_heartbeat
                
                if time_since_heartbeat > self.timeout:
                    if node_id not in self.suspected_nodes:
                        self.suspected_nodes.add(node_id)
                        logger.warning(f"Node {node_id} suspected of failure. "
                                     f"No heartbeat for {time_since_heartbeat:.1f}s")
                    else:
                        # Node already suspected, check if it should be declared dead
                        if time_since_heartbeat > self.timeout * 2:
                            self.declare_node_dead(node_id)
                else:
                    if node_id in self.suspected_nodes:
                        self.suspected_nodes.remove(node_id)
                        logger.info(f"Node {node_id} recovered")
                
                nodes_to_check.append((node_id, self.nodes[node_id]))
        
        # Try to ping suspected nodes
        for node_id, node_info in nodes_to_check:
            if node_id in self.suspected_nodes:
                self.ping_node(node_id, node_info['host'], node_info['port'])
    
    def ping_node(self, node_id: str, host: str, port: int):
        """Ping a node to check if it's really dead"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((host, port))
                
                # Send ping message
                message = {'type': 'PING', 'timestamp': time.time()}
                data = pickle.dumps(message)
                s.sendall(len(data).to_bytes(4, 'big') + data)
                
                # Try to receive response (even if we don't process it)
                try:
                    s.recv(4)  # Just check if we can receive
                except:
                    pass
                
                # Node is actually alive
                with self.lock:
                    if node_id in self.suspected_nodes:
                        self.suspected_nodes.remove(node_id)
                    self.update_heartbeat(node_id)
                    logger.info(f"Node {node_id} responded to ping, marking as active")
                
        except Exception as e:
            # Node is likely dead
            logger.debug(f"Node {node_id} ping failed: {e}")
    
    def declare_node_dead(self, node_id: str):
        """Declare a node as dead and initiate recovery"""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id]['status'] = 'dead'
                self.nodes[node_id]['died_at'] = time.time()
                logger.error(f"Node {node_id} declared dead")
                
                # Remove from monitoring
                del self.nodes[node_id]
                if node_id in self.last_heartbeats:
                    del self.last_heartbeats[node_id]
                if node_id in self.suspected_nodes:
                    self.suspected_nodes.remove(node_id)
                
                # TODO: Notify coordinator to initiate recovery for transactions
                # involving this node
    
    def get_status(self) -> Dict:
        """Get current status of all monitored nodes"""
        with self.lock:
            status = {
                'total_nodes': len(self.nodes),
                'active_nodes': sum(1 for n in self.nodes.values() if n['status'] == 'active'),
                'suspected_nodes': list(self.suspected_nodes),
                'nodes': {}
            }
            
            for node_id, node_info in self.nodes.items():
                last_heartbeat = self.last_heartbeats.get(node_id, 0)
                time_since_heartbeat = time.time() - last_heartbeat
                
                status['nodes'][node_id] = {
                    **node_info,
                    'last_heartbeat': last_heartbeat,
                    'time_since_heartbeat': time_since_heartbeat,
                    'is_suspected': node_id in self.suspected_nodes
                }
            
            return status
    
    def is_node_alive(self, node_id: str) -> bool:
        """Check if a node is considered alive"""
        with self.lock:
            if node_id not in self.nodes:
                return False
            
            if node_id in self.suspected_nodes:
                return False
            
            last_heartbeat = self.last_heartbeats.get(node_id, 0)
            time_since_heartbeat = time.time() - last_heartbeat
            
            return time_since_heartbeat <= self.timeout