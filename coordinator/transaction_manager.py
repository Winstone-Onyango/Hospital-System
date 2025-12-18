"""
Distributed Transaction Manager with Two-Phase Commit Protocol
"""
import threading
import time
import json
import socket
import logging
from enum import Enum
from typing import Dict, List, Optional
import pickle

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionState(Enum):
    """Transaction states for 2PC protocol"""
    INITIALIZED = "INITIALIZED"
    PREPARE_SENT = "PREPARE_SENT"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    TIMEOUT = "TIMEOUT"

class Transaction:
    """Represents a distributed transaction"""
    
    def __init__(self, transaction_id: str, operations: List[Dict]):
        self.transaction_id = transaction_id
        self.operations = operations  # List of operations to perform on nodes
        self.state = TransactionState.INITIALIZED
        self.start_time = time.time()
        self.votes = {}  # Node -> vote (True for yes, False for no)
        self.locks = []
        self.participants = set()
        
    def add_participant(self, node_id: str):
        """Add a participant node to the transaction"""
        self.participants.add(node_id)
        
    def record_vote(self, node_id: str, vote: bool):
        """Record a vote from a participant"""
        self.votes[node_id] = vote
        
    def all_votes_received(self) -> bool:
        """Check if all participants have voted"""
        return len(self.votes) == len(self.participants)
    
    def all_votes_yes(self) -> bool:
        """Check if all votes are yes"""
        return all(vote for vote in self.votes.values())
    
    def is_expired(self, timeout: int = 30) -> bool:
        """Check if transaction has expired"""
        return time.time() - self.start_time > timeout

class TransactionManager:
    """Manages distributed transactions using Two-Phase Commit"""
    
    def __init__(self, coordinator_host: str, coordinator_port: int):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.transactions: Dict[str, Transaction] = {}
        self.transaction_counter = 0
        self.lock = threading.Lock()
        self.recovery_log = "transaction_recovery.log"
        
    def generate_transaction_id(self) -> str:
        """Generate unique transaction ID"""
        with self.lock:
            self.transaction_counter += 1
            return f"TXN{self.transaction_counter:08d}"
    
    def begin_transaction(self, operations: List[Dict]) -> str:
        """Begin a new distributed transaction"""
        transaction_id = self.generate_transaction_id()
        transaction = Transaction(transaction_id, operations)
        
        with self.lock:
            self.transactions[transaction_id] = transaction
            
        logger.info(f"Began transaction {transaction_id} with {len(operations)} operations")
        
        # Log for recovery
        self._log_transaction(transaction, "BEGIN")
        
        return transaction_id
    
    def prepare_phase(self, transaction_id: str, nodes: List[Dict]) -> bool:
        """Execute prepare phase of 2PC"""
        with self.lock:
            if transaction_id not in self.transactions:
                return False
            
            transaction = self.transactions[transaction_id]
            transaction.state = TransactionState.PREPARE_SENT
            
        logger.info(f"Starting prepare phase for transaction {transaction_id}")
        
        # Send prepare messages to all nodes
        prepare_results = []
        threads = []
        
        def send_prepare(node_info, results):
            try:
                response = self._send_to_node(
                    node_info['host'], 
                    node_info['port'], 
                    {
                        'type': 'PREPARE',
                        'transaction_id': transaction_id,
                        'operations': [op for op in transaction.operations 
                                     if op['node_id'] == node_info['id']]
                    }
                )
                results.append((node_info['id'], response.get('vote', False)))
                
                with self.lock:
                    transaction.record_vote(node_info['id'], response.get('vote', False))
                    
            except Exception as e:
                logger.error(f"Error sending prepare to {node_info['id']}: {e}")
                results.append((node_info['id'], False))
                
                with self.lock:
                    transaction.record_vote(node_info['id'], False)
        
        # Send prepare requests in parallel
        for node in nodes:
            thread = threading.Thread(target=send_prepare, args=(node, prepare_results))
            threads.append(thread)
            thread.start()
            transaction.add_participant(node['id'])
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=10)
        
        # Check results
        with self.lock:
            if transaction.all_votes_received() and transaction.all_votes_yes():
                transaction.state = TransactionState.PREPARED
                logger.info(f"All nodes prepared for transaction {transaction_id}")
                self._log_transaction(transaction, "PREPARED")
                return True
            else:
                transaction.state = TransactionState.ABORTED
                logger.warning(f"Transaction {transaction_id} aborted during prepare phase")
                self._log_transaction(transaction, "ABORTED")
                # Send abort to all nodes
                self._send_abort_to_all(transaction_id, nodes)
                return False
    
    def commit_phase(self, transaction_id: str, nodes: List[Dict]) -> bool:
        """Execute commit phase of 2PC"""
        with self.lock:
            if transaction_id not in self.transactions:
                return False
            
            transaction = self.transactions[transaction_id]
            
            if transaction.state != TransactionState.PREPARED:
                logger.error(f"Transaction {transaction_id} not in PREPARED state")
                return False
            
            transaction.state = TransactionState.COMMITTED
            
        logger.info(f"Starting commit phase for transaction {transaction_id}")
        
        # Send commit messages to all nodes
        commit_results = []
        threads = []
        
        def send_commit(node_info, results):
            try:
                response = self._send_to_node(
                    node_info['host'], 
                    node_info['port'], 
                    {
                        'type': 'COMMIT',
                        'transaction_id': transaction_id
                    }
                )
                results.append((node_info['id'], response.get('success', False)))
                
            except Exception as e:
                logger.error(f"Error sending commit to {node_info['id']}: {e}")
                results.append((node_info['id'], False))
        
        # Send commit requests in parallel
        for node in nodes:
            thread = threading.Thread(target=send_commit, args=(node, commit_results))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=10)
        
        # Check if all commits succeeded
        all_committed = all(result[1] for result in commit_results)
        
        if all_committed:
            logger.info(f"Transaction {transaction_id} successfully committed")
            self._log_transaction(transaction, "COMMITTED")
            
            # Clean up transaction after successful commit
            with self.lock:
                del self.transactions[transaction_id]
                
            return True
        else:
            logger.error(f"Transaction {transaction_id} failed during commit phase")
            # Need to initiate recovery
            self._initiate_recovery(transaction_id, nodes)
            return False
    
    def abort_transaction(self, transaction_id: str, nodes: List[Dict]):
        """Abort a transaction"""
        with self.lock:
            if transaction_id in self.transactions:
                transaction = self.transactions[transaction_id]
                transaction.state = TransactionState.ABORTED
        
        logger.info(f"Aborting transaction {transaction_id}")
        
        # Send abort to all nodes
        self._send_abort_to_all(transaction_id, nodes)
        
        with self.lock:
            if transaction_id in self.transactions:
                del self.transactions[transaction_id]
        
        self._log_transaction(transaction, "ABORTED")
    
    def _send_abort_to_all(self, transaction_id: str, nodes: List[Dict]):
        """Send abort message to all participating nodes"""
        threads = []
        
        def send_abort(node_info):
            try:
                self._send_to_node(
                    node_info['host'], 
                    node_info['port'], 
                    {
                        'type': 'ABORT',
                        'transaction_id': transaction_id
                    }
                )
            except Exception as e:
                logger.error(f"Error sending abort to {node_info['id']}: {e}")
        
        for node in nodes:
            thread = threading.Thread(target=send_abort, args=(node,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join(timeout=5)
    
    def _initiate_recovery(self, transaction_id: str, nodes: List[Dict]):
        """Initiate recovery procedure for failed transaction"""
        logger.warning(f"Initiating recovery for transaction {transaction_id}")
        
        # In a real system, this would query nodes about transaction state
        # and decide whether to commit or abort based on majority
        
        # For simplicity, we'll abort the transaction
        self.abort_transaction(transaction_id, nodes)
    
    def _send_to_node(self, host: str, port: int, message: Dict) -> Dict:
        """Send message to a node and get response"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)
                s.connect((host, port))
                
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
            logger.error(f"Error communicating with node {host}:{port}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _log_transaction(self, transaction: Transaction, action: str):
        """Log transaction for recovery purposes"""
        try:
            with open(self.recovery_log, 'a') as f:
                log_entry = {
                    'timestamp': time.time(),
                    'transaction_id': transaction.transaction_id,
                    'state': transaction.state.value,
                    'action': action,
                    'participants': list(transaction.participants)
                }
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            logger.error(f"Error logging transaction: {e}")
    
    def recover_transactions(self):
        """Recover transactions from log after coordinator failure"""
        logger.info("Starting transaction recovery...")
        
        try:
            with open(self.recovery_log, 'r') as f:
                lines = f.readlines()
                
            # Find transactions that were in prepare phase
            prepared_txns = []
            for line in lines:
                try:
                    log_entry = json.loads(line.strip())
                    if log_entry['state'] == TransactionState.PREPARED.value:
                        prepared_txns.append(log_entry['transaction_id'])
                except:
                    continue
            
            logger.info(f"Found {len(prepared_txns)} transactions to recover")
            
        except FileNotFoundError:
            logger.info("No recovery log found")
            
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict]:
        """Get status of a transaction"""
        with self.lock:
            if transaction_id in self.transactions:
                transaction = self.transactions[transaction_id]
                return {
                    'transaction_id': transaction.transaction_id,
                    'state': transaction.state.value,
                    'operations': transaction.operations,
                    'votes': transaction.votes,
                    'participants': list(transaction.participants),
                    'age': time.time() - transaction.start_time
                }
        return None