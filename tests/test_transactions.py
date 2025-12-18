"""
Test suite for distributed transactions
"""
import unittest
import threading
import time
from coordinator.transaction_manager import TransactionManager, Transaction, TransactionState
from coordinator.lock_manager import LockManager, LockType

class TestTransactionManager(unittest.TestCase):
    """Test transaction manager functionality"""
    
    def setUp(self):
        self.tm = TransactionManager('localhost', 5000)
    
    def test_generate_transaction_id(self):
        """Test transaction ID generation"""
        txn_id1 = self.tm.generate_transaction_id()
        txn_id2 = self.tm.generate_transaction_id()
        
        self.assertTrue(txn_id1.startswith('TXN'))
        self.assertTrue(txn_id2.startswith('TXN'))
        self.assertNotEqual(txn_id1, txn_id2)
    
    def test_begin_transaction(self):
        """Test beginning a transaction"""
        operations = [
            {'node_id': 'N001', 'action': 'BOOK', 'slot_id': 'SLOT001'}
        ]
        
        txn_id = self.tm.begin_transaction(operations)
        
        self.assertIn(txn_id, self.tm.transactions)
        self.assertEqual(self.tm.transactions[txn_id].state, TransactionState.INITIALIZED)
        self.assertEqual(len(self.tm.transactions[txn_id].operations), 1)
    
    def test_transaction_expiry(self):
        """Test transaction expiration"""
        operations = [{'node_id': 'N001', 'action': 'BOOK'}]
        txn_id = self.tm.begin_transaction(operations)
        
        transaction = self.tm.transactions[txn_id]
        transaction.start_time = time.time() - 35  # Set start time 35 seconds ago
        
        self.assertTrue(transaction.is_expired(30))

class TestLockManager(unittest.TestCase):
    """Test lock manager functionality"""
    
    def setUp(self):
        self.lm = LockManager()
    
    def test_request_exclusive_lock(self):
        """Test requesting exclusive lock"""
        result = self.lm.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        self.assertTrue(result)
        
        # Check lock is granted
        self.assertIn('RES001', self.lm.locks)
        self.assertEqual(len(self.lm.locks['RES001']), 1)
        self.assertTrue(self.lm.locks['RES001'][0].granted)
    
    def test_request_shared_lock(self):
        """Test requesting shared lock"""
        result = self.lm.request_lock('TXN001', 'RES001', LockType.SHARED)
        self.assertTrue(result)
    
    def test_conflicting_locks(self):
        """Test conflicting lock requests"""
        # First transaction gets exclusive lock
        self.lm.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        
        # Second transaction tries to get exclusive lock (should fail)
        result = self.lm.request_lock('TXN002', 'RES001', LockType.EXCLUSIVE)
        self.assertFalse(result)
        
        # Check second lock is in waiting queue
        self.assertIn('RES001', self.lm.waiting_queue)
        self.assertEqual(len(self.lm.waiting_queue['RES001']), 1)
    
    def test_release_lock(self):
        """Test releasing locks"""
        # Acquire lock
        self.lm.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        
        # Release lock
        self.lm.release_lock('TXN001', 'RES001')
        
        # Check lock is released
        self.assertEqual(len(self.lm.locks.get('RES001', [])), 0)
    
    def test_deadlock_detection(self):
        """Test deadlock detection"""
        # Create a simple deadlock scenario
        self.lm.request_lock('TXN001', 'RES001', LockType.EXCLUSIVE)
        self.lm.request_lock('TXN002', 'RES002', LockType.EXCLUSIVE)
        
        # TXN001 waits for RES002, TXN002 waits for RES001
        self.lm.request_lock('TXN001', 'RES002', LockType.EXCLUSIVE)  # Should wait
        self.lm.request_lock('TXN002', 'RES001', LockType.EXCLUSIVE)  # Should wait
        
        # Detect deadlocks
        deadlocks = self.lm.detect_deadlocks()
        
        # Should find at least one deadlock
        self.assertGreater(len(deadlocks), 0)

class TestConcurrentTransactions(unittest.TestCase):
    """Test concurrent transaction scenarios"""
    
    def test_concurrent_lock_requests(self):
        """Test concurrent lock requests from multiple threads"""
        lm = LockManager()
        results = []
        
        def request_lock_thread(thread_id):
            txn_id = f'TXN{thread_id:03d}'
            result = lm.request_lock(txn_id, 'RES001', LockType.EXCLUSIVE)
            results.append((thread_id, result))
        
        # Start multiple threads trying to get the same lock
        threads = []
        for i in range(5):
            thread = threading.Thread(target=request_lock_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Only one should have gotten the lock
        granted = sum(1 for _, result in results if result)
        self.assertEqual(granted, 1)

if __name__ == '__main__':
    unittest.main()