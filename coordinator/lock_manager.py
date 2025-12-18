"""
Distributed Lock Manager for Concurrency Control
"""
import threading
import time
import logging
from typing import Dict, List, Optional, Set
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LockType(Enum):
    """Types of locks"""
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"

class Lock:
    """Represents a lock on a resource"""
    
    def __init__(self, resource_id: str, lock_type: LockType, transaction_id: str):
        self.resource_id = resource_id
        self.lock_type = lock_type
        self.transaction_id = transaction_id
        self.timestamp = time.time()
        self.granted = False

class LockManager:
    """Manages locks for concurrency control"""
    
    def __init__(self):
        self.locks: Dict[str, List[Lock]] = {}  # resource_id -> list of locks
        self.waiting_queue: Dict[str, List[Lock]] = {}  # resource_id -> waiting locks
        self.lock = threading.Lock()
        self.deadlock_detection_interval = 30  # seconds
        self.last_deadlock_check = time.time()
        
    def request_lock(self, transaction_id: str, resource_id: str, 
                    lock_type: LockType = LockType.EXCLUSIVE) -> bool:
        """Request a lock for a transaction"""
        with self.lock:
            lock = Lock(resource_id, lock_type, transaction_id)
            
            # Initialize if resource not in dictionary
            if resource_id not in self.locks:
                self.locks[resource_id] = []
            if resource_id not in self.waiting_queue:
                self.waiting_queue[resource_id] = []
            
            # Check if lock can be granted
            if self._can_grant_lock(resource_id, lock):
                lock.granted = True
                self.locks[resource_id].append(lock)
                logger.info(f"Lock granted: {transaction_id} on {resource_id} ({lock_type.value})")
                return True
            else:
                # Add to waiting queue
                self.waiting_queue[resource_id].append(lock)
                logger.info(f"Lock queued: {transaction_id} on {resource_id} ({lock_type.value})")
                return False
    
    def release_lock(self, transaction_id: str, resource_id: str):
        """Release a lock held by a transaction"""
        with self.lock:
            if resource_id in self.locks:
                # Remove all locks for this transaction on this resource
                self.locks[resource_id] = [
                    lock for lock in self.locks[resource_id] 
                    if lock.transaction_id != transaction_id
                ]
                
                logger.info(f"Lock released: {transaction_id} on {resource_id}")
                
                # Check waiting queue for locks that can now be granted
                self._grant_waiting_locks(resource_id)
    
    def release_all_locks(self, transaction_id: str):
        """Release all locks held by a transaction"""
        with self.lock:
            released_resources = []
            for resource_id in list(self.locks.keys()):
                original_count = len(self.locks.get(resource_id, []))
                self.locks[resource_id] = [
                    lock for lock in self.locks[resource_id] 
                    if lock.transaction_id != transaction_id
                ]
                
                if len(self.locks[resource_id]) < original_count:
                    released_resources.append(resource_id)
            
            # Grant waiting locks for all released resources
            for resource_id in released_resources:
                self._grant_waiting_locks(resource_id)
            
            logger.info(f"Released all locks for transaction {transaction_id}")
    
    def _can_grant_lock(self, resource_id: str, new_lock: Lock) -> bool:
        """Check if a lock can be granted"""
        existing_locks = self.locks.get(resource_id, [])
        
        if not existing_locks:
            return True
        
        # If requesting exclusive lock
        if new_lock.lock_type == LockType.EXCLUSIVE:
            # Can only grant if no other locks exist
            return len(existing_locks) == 0
        
        # If requesting shared lock
        elif new_lock.lock_type == LockType.SHARED:
            # Can grant if no exclusive locks exist
            return all(lock.lock_type == LockType.SHARED for lock in existing_locks)
        
        return False
    
    def _grant_waiting_locks(self, resource_id: str):
        """Grant locks from waiting queue if possible"""
        if resource_id not in self.waiting_queue:
            return
        
        granted_locks = []
        for lock in self.waiting_queue[resource_id][:]:  # Iterate over copy
            if self._can_grant_lock(resource_id, lock):
                lock.granted = True
                self.locks[resource_id].append(lock)
                granted_locks.append(lock)
                self.waiting_queue[resource_id].remove(lock)
                logger.info(f"Granted queued lock: {lock.transaction_id} on {resource_id}")
        
        # Remove resource from waiting queue if empty
        if not self.waiting_queue[resource_id]:
            del self.waiting_queue[resource_id]
    
    def detect_deadlocks(self) -> List[List[str]]:
        """Detect deadlocks using wait-for graph"""
        with self.lock:
            # Build wait-for graph
            wait_for_graph = {}
            
            # For each waiting lock, find which transactions are blocking it
            for resource_id, waiting_locks in self.waiting_queue.items():
                for waiting_lock in waiting_locks:
                    if waiting_lock.transaction_id not in wait_for_graph:
                        wait_for_graph[waiting_lock.transaction_id] = set()
                    
                    # Find transactions holding locks on this resource
                    holding_locks = self.locks.get(resource_id, [])
                    for holding_lock in holding_locks:
                        wait_for_graph[waiting_lock.transaction_id].add(holding_lock.transaction_id)
            
            # Find cycles in the graph (deadlocks)
            deadlocks = self._find_cycles(wait_for_graph)
            
            if deadlocks:
                logger.warning(f"Detected {len(deadlocks)} deadlock(s)")
                
                # Resolve deadlocks by aborting the youngest transaction
                for cycle in deadlocks:
                    if cycle:
                        # Find transaction with latest timestamp to abort
                        youngest_txn = max(cycle, key=lambda x: self._get_transaction_timestamp(x))
                        logger.info(f"Resolving deadlock by aborting transaction {youngest_txn}")
                        # In a real system, this would trigger transaction abort
            
            return deadlocks
    
    def _find_cycles(self, graph: Dict[str, Set[str]]) -> List[List[str]]:
        """Find cycles in directed graph using DFS"""
        def dfs(node, visited, stack, path, cycles):
            visited.add(node)
            stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    dfs(neighbor, visited, stack, path, cycles)
                elif neighbor in stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:].copy())
            
            stack.remove(node)
            path.pop()
        
        visited = set()
        stack = set()
        cycles = []
        
        for node in graph:
            if node not in visited:
                dfs(node, visited, stack, [], cycles)
        
        # Remove duplicate cycles
        unique_cycles = []
        for cycle in cycles:
            cycle_set = set(cycle)
            if cycle_set not in [set(c) for c in unique_cycles]:
                unique_cycles.append(cycle)
        
        return unique_cycles
    
    def _get_transaction_timestamp(self, transaction_id: str) -> float:
        """Get timestamp of most recent lock for transaction"""
        latest = 0
        for resource_locks in self.locks.values():
            for lock in resource_locks:
                if lock.transaction_id == transaction_id and lock.timestamp > latest:
                    latest = lock.timestamp
        return latest
    
    def get_lock_status(self) -> Dict:
        """Get current lock status for monitoring"""
        with self.lock:
            status = {
                'granted_locks': {},
                'waiting_locks': {},
                'deadlocks': self.detect_deadlocks()
            }
            
            for resource_id, locks in self.locks.items():
                status['granted_locks'][resource_id] = [
                    {
                        'transaction_id': lock.transaction_id,
                        'type': lock.lock_type.value,
                        'timestamp': lock.timestamp
                    }
                    for lock in locks
                ]
            
            for resource_id, locks in self.waiting_queue.items():
                status['waiting_locks'][resource_id] = [
                    {
                        'transaction_id': lock.transaction_id,
                        'type': lock.lock_type.value,
                        'timestamp': lock.timestamp
                    }
                    for lock in locks
                ]
            
            return status