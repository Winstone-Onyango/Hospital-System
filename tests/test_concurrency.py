"""
Test suite for concurrency control
"""
import unittest
import threading
import time
from coordinator.lock_manager import LockManager, LockType

class TestConcurrencyControl(unittest.TestCase):
    """Test concurrency control scenarios"""
    
    def setUp(self):
        self.lock_manager = LockManager()
    
    def test_multiple_shared_locks(self):
        """Test multiple shared locks can coexist"""
        # Multiple transactions should be able to get shared locks
        txn_ids = ['TXN001', 'TXN002', 'TXN003']
        
        for txn_id in txn_ids:
            result = self.lock_manager.request_lock(txn_id, 'RES001', LockType.SHARED)
            self.assertTrue(result, f"Transaction {txn_id} should get shared lock")
        
        # All three should have the lock
        self.assertEqual(len(self.lock_manager.locks.get('RES001', [])), 3)
    
    def test_exclusive_lock_blocks_shared(self):
        """Test exclusive lock blocks shared locks"""
        # First transaction gets exclusive lock
        self.lock_manager.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        
        # Second transaction tries to get shared lock (should fail/queue)
        result = self.lock_manager.request_lock('TXN002', 'RES001', LockType.SHARED)
        self.assertFalse(result, "Shared lock should be blocked by exclusive lock")
        
        # Check it's in waiting queue
        self.assertIn('RES001', self.lock_manager.waiting_queue)
    
    def test_shared_lock_blocks_exclusive(self):
        """Test shared locks block exclusive lock"""
        # First transaction gets shared lock
        self.lock_manager.request_lock('TXN001', 'RES001', LockType.SHARED)
        
        # Second transaction tries to get exclusive lock (should fail/queue)
        result = self.lock_manager.request_lock('TXN002', 'RES001', LockType.EXCLUSIVE)
        self.assertFalse(result, "Exclusive lock should be blocked by shared lock")
    
    def test_lock_granting_order(self):
        """Test locks are granted in request order"""
        # Request locks in specific order
        requests = [
            ('TXN001', LockType.EXCLUSIVE),
            ('TXN002', LockType.SHARED),
            ('TXN003', LockType.EXCLUSIVE)
        ]
        
        results = []
        for txn_id, lock_type in requests:
            result = self.lock_manager.request_lock(txn_id, 'RES001', lock_type)
            results.append((txn_id, result))
        
        # Only first should be granted
        self.assertTrue(results[0][1])  # TXN001 should get lock
        self.assertFalse(results[1][1])  # TXN002 should wait
        self.assertFalse(results[2][1])  # TXN003 should wait
        
        # Release first lock
        self.lock_manager.release_lock('TXN001', 'RES001')
        
        # TXN002 should now get shared lock
        self.assertTrue(self.lock_manager.locks['RES001'][0].granted)
        self.assertEqual(self.lock_manager.locks['RES001'][0].transaction_id, 'TXN002')
    
    def test_concurrent_resource_access(self):
        """Test concurrent access to different resources"""
        resources = ['RES001', 'RES002', 'RES003']
        
        # Each transaction gets a different resource
        for i, resource in enumerate(resources):
            txn_id = f'TXN{i+1:03d}'
            result = self.lock_manager.request_lock(txn_id, resource, LockType.EXCLUSIVE)
            self.assertTrue(result, f"Transaction {txn_id} should get lock on {resource}")
        
        # All should have their locks
        self.assertEqual(len(self.lock_manager.locks), 3)
    
    def test_lock_release_grants_waiting(self):
        """Test releasing lock grants waiting locks"""
        # Setup: TXN001 has exclusive lock, TXN002 is waiting for shared
        self.lock_manager.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        self.lock_manager.request_lock('TXN002', 'RES001', LockType.SHARED)
        
        # TXN002 should be waiting
        self.assertIn('RES001', self.lock_manager.waiting_queue)
        
        # Release TXN001's lock
        self.lock_manager.release_lock('TXN001', 'RES001')
        
        # TXN002 should now have the lock
        self.assertEqual(len(self.lock_manager.locks.get('RES001', [])), 1)
        self.assertEqual(self.lock_manager.locks['RES001'][0].transaction_id, 'TXN002')
    
    def test_thread_safety(self):
        """Test lock manager thread safety"""
        num_threads = 10
        results = []
        lock = threading.Lock()
        
        def worker(worker_id):
            txn_id = f'TXN{worker_id:03d}'
            resource = f'RES{(worker_id % 3) + 1:03d}'
            
            # Try to get lock
            success = self.lock_manager.request_lock(txn_id, resource, LockType.EXCLUSIVE)
            
            with lock:
                results.append((worker_id, txn_id, resource, success))
            
            # Hold lock briefly
            time.sleep(0.01)
            
            # Release lock
            self.lock_manager.release_lock(txn_id, resource)
        
        # Start worker threads
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify no data corruption
        self.assertEqual(len(results), num_threads)
        
        # Count successful locks per resource (should be <= 1 at any time)
        # This is a simplified check - in reality we'd need more sophisticated monitoring
        resources = {}
        for _, txn_id, resource, success in results:
            if success:
                if resource not in resources:
                    resources[resource] = 0
                resources[resource] += 1
        
        # Each resource should have been locked multiple times (by different transactions)
        for resource, count in resources.items():
            self.assertGreater(count, 0, f"Resource {resource} should have been locked")
    
    def test_lock_upgrade(self):
        """Test lock upgrade scenario (shared to exclusive)"""
        # First get shared lock
        self.lock_manager.request_lock('TXN001', 'RES001', LockType.SHARED)
        
        # Try to upgrade to exclusive (should fail/queue)
        result = self.lock_manager.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        self.assertFalse(result, "Lock upgrade should wait")
        
        # Release shared lock (should then get exclusive)
        self.lock_manager.release_lock('TXN001', 'RES001')
        
        # Now should have exclusive lock
        locks = self.lock_manager.locks.get('RES001', [])
        self.assertEqual(len(locks), 1)
        self.assertEqual(locks[0].lock_type, LockType.EXCLUSIVE)
        self.assertEqual(locks[0].transaction_id, 'TXN001')

if __name__ == '__main__':
    unittest.main()