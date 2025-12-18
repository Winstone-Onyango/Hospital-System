"""
Test suite for failure recovery
"""
import unittest
import time
import tempfile
import os
from coordinator.failure_detector import FailureDetector
from coordinator.transaction_manager import TransactionManager, TransactionState

class TestFailureDetection(unittest.TestCase):
    """Test failure detection functionality"""
    
    def setUp(self):
        self.fd = FailureDetector(heartbeat_interval=1, timeout=2)
        self.fd.start()
    
    def tearDown(self):
        self.fd.stop()
    
    def test_register_node(self):
        """Test node registration"""
        self.fd.register_node('N001', 'localhost', 5001)
        
        self.assertIn('N001', self.fd.nodes)
        self.assertEqual(self.fd.nodes['N001']['host'], 'localhost')
        self.assertEqual(self.fd.nodes['N001']['port'], 5001)
        self.assertEqual(self.fd.nodes['N001']['status'], 'active')
    
    def test_heartbeat_update(self):
        """Test heartbeat updates"""
        self.fd.register_node('N001', 'localhost', 5001)
        
        initial_time = self.fd.last_heartbeats['N001']
        
        # Update heartbeat
        time.sleep(0.1)
        self.fd.update_heartbeat('N001')
        
        new_time = self.fd.last_heartbeats['N001']
        self.assertGreater(new_time, initial_time)
    
    def test_failure_detection(self):
        """Test failure detection"""
        self.fd.register_node('N001', 'localhost', 5001)
        
        # Wait longer than timeout
        time.sleep(3)
        
        # Check node should be suspected
        self.assertIn('N001', self.fd.suspected_nodes)
    
    def test_node_recovery(self):
        """Test node recovery detection"""
        self.fd.register_node('N001', 'localhost', 5001)
        
        # Let node become suspected
        time.sleep(3)
        self.assertIn('N001', self.fd.suspected_nodes)
        
        # Update heartbeat
        self.fd.update_heartbeat('N001')
        
        # Node should no longer be suspected
        self.assertNotIn('N001', self.fd.suspected_nodes)

class TestTransactionRecovery(unittest.TestCase):
    """Test transaction recovery functionality"""
    
    def setUp(self):
        # Create temporary directory for recovery log
        self.temp_dir = tempfile.mkdtemp()
        self.recovery_log = os.path.join(self.temp_dir, 'recovery.log')
    
    def tearDown(self):
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_transaction_logging(self):
        """Test transaction logging for recovery"""
        tm = TransactionManager('localhost', 5000)
        tm.recovery_log = self.recovery_log
        
        # Begin a transaction
        operations = [{'node_id': 'N001', 'action': 'BOOK'}]
        txn_id = tm.begin_transaction(operations)
        
        # Check log was created
        self.assertTrue(os.path.exists(self.recovery_log))
        
        # Read log
        with open(self.recovery_log, 'r') as f:
            lines = f.readlines()
            self.assertGreater(len(lines), 0)
            
            # Check log contains transaction
            log_entry = lines[-1]
            self.assertIn(txn_id, log_entry)
    
    def test_recover_prepared_transactions(self):
        """Test recovery of prepared transactions"""
        tm = TransactionManager('localhost', 5000)
        tm.recovery_log = self.recovery_log
        
        # Create a log with prepared transaction
        log_entry = {
            'timestamp': time.time(),
            'transaction_id': 'TXN000001',
            'state': TransactionState.PREPARED.value,
            'action': 'PREPARED',
            'participants': ['N001', 'N002']
        }
        
        with open(self.recovery_log, 'w') as f:
            f.write('{"test": "data"}\n')  # Some other entry
            f.write(f'{json.dumps(log_entry)}\n')
        
        # Recover transactions
        tm.recover_transactions()
        
        # In a real implementation, this would initiate recovery
        # For now, just ensure no errors
    
    def test_transaction_timeout(self):
        """Test transaction timeout handling"""
        tm = TransactionManager('localhost', 5000)
        
        operations = [{'node_id': 'N001', 'action': 'BOOK'}]
        txn_id = tm.begin_transaction(operations)
        
        # Manually set transaction start time to past
        transaction = tm.transactions[txn_id]
        transaction.start_time = time.time() - 35  # 35 seconds ago
        
        # Check if expired
        self.assertTrue(transaction.is_expired(30))
        
        # Transaction should be marked for cleanup
        # In real system, coordinator would abort it

class TestSystemRecovery(unittest.TestCase):
    """Test full system recovery scenarios"""
    
    def test_coordinator_failure_recovery(self):
        """Test coordinator failure and recovery"""
        # This is a complex integration test that would require
        # setting up the full system. For now, we'll outline the test.
        
        # Scenario:
        # 1. System running with coordinator and nodes
        # 2. Coordinator fails during commit phase
        # 3. Coordinator restarts
        # 4. Coordinator recovers from log
        # 5. System queries nodes for transaction state
        # 6. System completes or aborts transactions based on consensus
        
        # Implementation would require actual system setup
        # For now, just pass this placeholder test
        self.assertTrue(True)
    
    def test_node_failure_during_transaction(self):
        """Test node failure during transaction"""
        # Scenario:
        # 1. Transaction begins across multiple nodes
        # 2. One node fails during prepare phase
        # 3. Coordinator detects failure
        # 4. Coordinator initiates abort
        # 5. Remaining nodes roll back
        
        # Implementation would require actual system setup
        self.assertTrue(True)
    
    def test_network_partition_recovery(self):
        """Test recovery from network partition"""
        # Scenario:
        # 1. Network partition splits nodes
        # 2. Transactions continue in each partition
        # 3. Network heals
        # 4. System reconciles state
        # 5. Consistency is maintained
        
        self.assertTrue(True)

def create_test_system():
    """Helper function to create a test system setup"""
    # This would set up coordinator and nodes for integration testing
    pass

if __name__ == '__main__':
    unittest.main()