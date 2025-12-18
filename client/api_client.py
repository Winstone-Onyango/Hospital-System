"""
API Client for web interface
"""
import requests
import json
from typing import Dict, List, Optional

class APIClient:
    """API Client for web interface"""
    
    def __init__(self, base_url: str = 'http://localhost:8080'):
        self.base_url = base_url
        
    def get_system_status(self) -> Dict:
        """Get system status"""
        try:
            response = requests.get(f'{self.base_url}/api/status', timeout=5)
            return response.json()
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def book_appointment(self, data: Dict) -> Dict:
        """Book an appointment"""
        try:
            response = requests.post(f'{self.base_url}/booking', 
                                   data=data, 
                                   timeout=10)
            return response.json()
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def simulate_failure(self, scenario: str) -> Dict:
        """Simulate a failure scenario"""
        try:
            response = requests.post(f'{self.base_url}/api/simulate-failure',
                                   json={'scenario': scenario},
                                   timeout=5)
            return response.json()
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def test_concurrent(self, count: int = 5) -> Dict:
        """Test concurrent transactions"""
        try:
            response = requests.post(f'{self.base_url}/api/concurrent-test',
                                   json={'count': count},
                                   timeout=30)
            return response.json()
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_node_slots(self, node_id: str) -> Dict:
        """Get slots for a specific node"""
        try:
            response = requests.get(f'{self.base_url}/api/nodes/{node_id}/slots',
                                  timeout=5)
            return response.json()
        except Exception as e:
            return {'success': False, 'error': str(e)}