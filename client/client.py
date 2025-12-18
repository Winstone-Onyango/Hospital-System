"""
Client for interacting with the distributed hospital system
"""
import socket
import pickle
import json
import time
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HospitalClient:
    """Client for interacting with the hospital distributed system"""
    
    def __init__(self, coordinator_host: str = 'localhost', coordinator_port: int = 5000):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        
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
    
    def book_appointment(self, patient_id: str, patient_name: str, 
                        department_id: str, slot_time: str, slot_date: str) -> Dict:
        """Book an appointment using distributed transaction"""
        
        # Create transaction operations
        operations = [{
            'node_id': department_id,
            'action': 'BOOK',
            'slot_id': f"SLOT{department_id}{int(time.time()) % 1000:03d}",
            'patient_id': patient_id,
            'patient_name': patient_name,
            'slot_time': slot_time,
            'slot_date': slot_date,
            'requires_exclusive': True
        }]
        
        # Begin transaction
        logger.info("Beginning distributed transaction...")
        begin_response = self.send_to_coordinator({
            'type': 'BEGIN_TXN',
            'operations': operations
        })
        
        if not begin_response.get('success'):
            return begin_response
        
        transaction_id = begin_response.get('transaction_id')
        logger.info(f"Transaction {transaction_id} begun")
        
        # Get participating nodes
        # In a real system, we would get this from the coordinator
        participating_nodes = [{
            'id': department_id,
            'host': 'localhost',
            'port': 5000 + int(department_id[-1])  # Simulated port
        }]
        
        # Prepare phase
        logger.info("Starting prepare phase...")
        prepare_response = self.send_to_coordinator({
            'type': 'PREPARE_PHASE',
            'transaction_id': transaction_id,
            'nodes': participating_nodes
        })
        
        if not prepare_response.get('success'):
            # Abort transaction
            self.send_to_coordinator({
                'type': 'ABORT_TXN',
                'transaction_id': transaction_id,
                'nodes': participating_nodes
            })
            return prepare_response
        
        # Commit phase
        logger.info("Starting commit phase...")
        commit_response = self.send_to_coordinator({
            'type': 'COMMIT_PHASE',
            'transaction_id': transaction_id,
            'nodes': participating_nodes
        })
        
        if commit_response.get('success'):
            logger.info(f"Transaction {transaction_id} committed successfully")
            return {
                'success': True,
                'transaction_id': transaction_id,
                'message': 'Appointment booked successfully'
            }
        else:
            logger.error(f"Transaction {transaction_id} failed during commit")
            return commit_response
    
    def query_available_slots(self, department_id: Optional[str] = None) -> Dict:
        """Query available slots from nodes"""
        # In a real implementation, this would query the coordinator
        # or specific nodes for slot information
        
        # For now, return simulated data
        return {
            'success': True,
            'slots': [
                {
                    'slot_id': 'SLOT001',
                    'department': 'Cardiology',
                    'time': '09:00',
                    'date': '2024-03-20',
                    'available': True
                },
                {
                    'slot_id': 'SLOT002',
                    'department': 'Cardiology',
                    'time': '10:00',
                    'date': '2024-03-20',
                    'available': True
                }
            ]
        }
    
    def get_system_status(self) -> Dict:
        """Get system status from coordinator"""
        return self.send_to_coordinator({
            'type': 'QUERY_STATUS'
        })
    
    def test_concurrent_bookings(self, num_concurrent: int = 5) -> Dict:
        """Test concurrent bookings"""
        import threading
        
        results = []
        lock = threading.Lock()
        
        def book_concurrent(thread_id: int):
            try:
                result = self.book_appointment(
                    patient_id=f'PAT{thread_id:03d}',
                    patient_name=f'Patient {thread_id}',
                    department_id='N001',  # Cardiology
                    slot_time='09:00',
                    slot_date='2024-03-20'
                )
                
                with lock:
                    results.append({
                        'thread_id': thread_id,
                        'success': result.get('success', False),
                        'transaction_id': result.get('transaction_id'),
                        'error': result.get('error')
                    })
                    
            except Exception as e:
                with lock:
                    results.append({
                        'thread_id': thread_id,
                        'success': False,
                        'error': str(e)
                    })
        
        # Start concurrent threads
        threads = []
        for i in range(num_concurrent):
            thread = threading.Thread(target=book_concurrent, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=10)
        
        # Return summary
        successful = sum(1 for r in results if r.get('success'))
        
        return {
            'success': True,
            'results': results,
            'summary': {
                'total': len(results),
                'successful': successful,
                'failed': len(results) - successful
            }
        }

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Hospital Distributed System Client')
    parser.add_argument('--host', default='localhost', help='Coordinator host')
    parser.add_argument('--port', type=int, default=5000, help='Coordinator port')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Book command
    book_parser = subparsers.add_parser('book', help='Book an appointment')
    book_parser.add_argument('--patient-id', required=True, help='Patient ID')
    book_parser.add_argument('--patient-name', required=True, help='Patient name')
    book_parser.add_argument('--department', required=True, help='Department ID')
    book_parser.add_argument('--time', required=True, help='Appointment time')
    book_parser.add_argument('--date', required=True, help='Appointment date')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Get system status')
    
    # Slots command
    slots_parser = subparsers.add_parser('slots', help='Query available slots')
    slots_parser.add_argument('--department', help='Department ID')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test concurrent bookings')
    test_parser.add_argument('--count', type=int, default=5, help='Number of concurrent bookings')
    
    args = parser.parse_args()
    
    client = HospitalClient(args.host, args.port)
    
    if args.command == 'book':
        result = client.book_appointment(
            args.patient_id,
            args.patient_name,
            args.department,
            args.time,
            args.date
        )
        print(json.dumps(result, indent=2))
        
    elif args.command == 'status':
        result = client.get_system_status()
        print(json.dumps(result, indent=2))
        
    elif args.command == 'slots':
        result = client.query_available_slots(args.department)
        print(json.dumps(result, indent=2))
        
    elif args.command == 'test':
        result = client.test_concurrent_bookings(args.count)
        print(json.dumps(result, indent=2))
        
    else:
        parser.print_help()