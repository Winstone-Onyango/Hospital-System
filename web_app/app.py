"""
Web Application for Hospital Booking System
"""
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import socket
import pickle
import json
import threading
import time
from datetime import datetime, date

app = Flask(__name__)
app.secret_key = 'matibabu-hospital-secret-key-2024'

# Configuration
COORDINATOR_HOST = 'localhost'
COORDINATOR_PORT = 5000
NODES = [
    {'id': 'N001', 'name': 'Cardiology Department', 'host': 'localhost', 'port': 5001, 'color': '#FF6B6B'},
    {'id': 'N002', 'name': 'Radiology Department', 'host': 'localhost', 'port': 5002, 'color': '#4ECDC4'},
    {'id': 'N003', 'name': 'Orthopedics Department', 'host': 'localhost', 'port': 5003, 'color': '#95E1D3'},
    {'id': 'N004', 'name': 'Emergency Department', 'host': 'localhost', 'port': 5004, 'color': '#FFD166'}
]

def send_to_coordinator(message):
    """Send message to coordinator"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((COORDINATOR_HOST, COORDINATOR_PORT))
            
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
        return {'success': False, 'error': str(e)}

def send_to_node(node_host, node_port, message):
    """Send message to specific node"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((node_host, node_port))
            
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
        return {'success': False, 'error': str(e)}

@app.route('/')
def index():
    """Home page"""
    return render_template('index.html', 
                         hospital_name="GROUP 4 MATIBABU REFERRAL HOSPITAL",
                         nodes=NODES)

@app.route('/dashboard')
def dashboard():
    """Dashboard showing system status"""
    # Get coordinator status
    status_response = send_to_coordinator({'type': 'QUERY_STATUS'})
    
    # Get status from each node
    nodes_status = []
    for node in NODES:
        try:
            node_response = send_to_node(node['host'], node['port'], {'type': 'NODE_STATUS'})
            if node_response.get('success'):
                nodes_status.append({
                    'id': node['id'],
                    'name': node['name'],
                    'color': node['color'],
                    'status': node_response.get('status', {}),
                    'online': True
                })
            else:
                nodes_status.append({
                    'id': node['id'],
                    'name': node['name'],
                    'color': node['color'],
                    'status': {},
                    'online': False,
                    'error': node_response.get('error')
                })
        except:
            nodes_status.append({
                'id': node['id'],
                'name': node['name'],
                'color': node['color'],
                'status': {},
                'online': False,
                'error': 'Connection failed'
            })
    
    coordinator_status = status_response.get('status', {}) if status_response.get('success') else {}
    
    return render_template('dashboard.html',
                         hospital_name="GROUP 4 MATIBABU REFERRAL HOSPITAL",
                         nodes=nodes_status,
                         coordinator=coordinator_status)

@app.route('/booking', methods=['GET', 'POST'])
def booking():
    """Appointment booking page"""
    if request.method == 'POST':
        # Get form data
        patient_name = request.form.get('patient_name')
        patient_id = request.form.get('patient_id')
        department_id = request.form.get('department')
        slot_time = request.form.get('slot_time')
        slot_date = request.form.get('slot_date')
        
        if not all([patient_name, patient_id, department_id, slot_time, slot_date]):
            return jsonify({'success': False, 'error': 'All fields are required'})
        
        # Find the selected department node
        selected_node = None
        for node in NODES:
            if node['id'] == department_id:
                selected_node = node
                break
        
        if not selected_node:
            return jsonify({'success': False, 'error': 'Invalid department selected'})
        
        # Generate a slot ID (in real system, this would come from the node)
        slot_id = f"SLOT{department_id}{int(time.time()) % 1000:03d}"
        
        # Create transaction operations
        operations = [{
            'node_id': department_id,
            'action': 'BOOK',
            'slot_id': slot_id,
            'patient_id': patient_id,
            'patient_name': patient_name,
            'slot_time': slot_time,
            'slot_date': slot_date,
            'requires_exclusive': True
        }]
        
        # Begin distributed transaction
        begin_response = send_to_coordinator({
            'type': 'BEGIN_TXN',
            'operations': operations
        })
        
        if not begin_response.get('success'):
            return jsonify({'success': False, 'error': begin_response.get('error', 'Transaction failed')})
        
        transaction_id = begin_response.get('transaction_id')
        
        # Get all participating nodes
        participating_nodes = []
        for op in operations:
            for node in NODES:
                if node['id'] == op['node_id']:
                    participating_nodes.append(node)
                    break
        
        # Prepare phase
        prepare_response = send_to_coordinator({
            'type': 'PREPARE_PHASE',
            'transaction_id': transaction_id,
            'nodes': participating_nodes
        })
        
        if not prepare_response.get('success'):
            # Abort transaction
            send_to_coordinator({
                'type': 'ABORT_TXN',
                'transaction_id': transaction_id,
                'nodes': participating_nodes
            })
            return jsonify({'success': False, 'error': 'Prepare phase failed'})
        
        # Commit phase
        commit_response = send_to_coordinator({
            'type': 'COMMIT_PHASE',
            'transaction_id': transaction_id,
            'nodes': participating_nodes
        })
        
        if commit_response.get('success'):
            return jsonify({
                'success': True,
                'message': f'Appointment booked successfully! Transaction ID: {transaction_id}',
                'transaction_id': transaction_id,
                'slot_id': slot_id
            })
        else:
            return jsonify({'success': False, 'error': 'Commit phase failed'})
    
    # GET request - show booking form
    # Get available slots from each department
    available_slots = []
    for node in NODES:
        try:
            response = send_to_node(node['host'], node['port'], {'type': 'QUERY_SLOTS'})
            if response.get('success'):
                slots = response.get('slots', [])
                for slot in slots:
                    slot['department_id'] = node['id']
                    slot['department_name'] = node['name']
                    slot['color'] = node['color']
                    available_slots.append(slot)
        except:
            continue
    
    return render_template('booking1.html',
                         hospital_name="GROUP 4 MATIBABU REFERRAL HOSPITAL",
                         slots=available_slots,
                         nodes=NODES,
                         now_date=date.today().isoformat())

@app.route('/admin')
def admin():
    """Admin panel for system management"""
    # Get system status
    status_response = send_to_coordinator({'type': 'QUERY_STATUS'})
    coordinator_status = status_response.get('status', {}) if status_response.get('success') else {}
    
    # Simulate failure injection options
    failure_scenarios = [
        {'id': 'node_crash', 'name': 'Simulate Node Crash', 'description': 'Crashes a random node'},
        {'id': 'network_partition', 'name': 'Simulate Network Partition', 'description': 'Creates network delay'},
        {'id': 'coordinator_fail', 'name': 'Simulate Coordinator Failure', 'description': 'Stops coordinator'},
        {'id': 'transaction_timeout', 'name': 'Simulate Transaction Timeout', 'description': 'Causes transaction timeout'}
    ]
    
    return render_template('admin.html',
                         hospital_name="GROUP 4 MATIBABU REFERRAL HOSPITAL",
                         coordinator=coordinator_status,
                         failure_scenarios=failure_scenarios)

@app.route('/api/status')
def api_status():
    """API endpoint for system status"""
    status_response = send_to_coordinator({'type': 'QUERY_STATUS'})
    return jsonify(status_response)

@app.route('/api/nodes/<node_id>/slots')
def api_node_slots(node_id):
    """API endpoint for node slots"""
    node = next((n for n in NODES if n['id'] == node_id), None)
    if not node:
        return jsonify({'success': False, 'error': 'Node not found'})
    
    response = send_to_node(node['host'], node['port'], {'type': 'QUERY_SLOTS'})
    return jsonify(response)

@app.route('/api/simulate-failure', methods=['POST'])
def simulate_failure():
    """API endpoint to simulate failures"""
    scenario = request.json.get('scenario')
    
    if scenario == 'node_crash':
        # Simulate node crash by stopping a random node's heartbeat
        return jsonify({
            'success': True,
            'message': 'Simulated node crash - Node will appear as failed in next heartbeat check'
        })
    
    elif scenario == 'transaction_timeout':
        # Create a transaction that will timeout
        operations = [{
            'node_id': 'N001',
            'action': 'BOOK',
            'slot_id': 'TEST_TIMEOUT',
            'patient_id': 'test',
            'patient_name': 'Test Timeout',
            'requires_exclusive': True
        }]
        
        response = send_to_coordinator({
            'type': 'BEGIN_TXN',
            'operations': operations,
            'simulate_timeout': True
        })
        
        return jsonify(response)
    
    else:
        return jsonify({'success': False, 'error': 'Unknown failure scenario'})

@app.route('/api/concurrent-test', methods=['POST'])
def concurrent_test():
    """Test concurrent bookings"""
    num_concurrent = min(int(request.json.get('count', 5)), 10)
    
    results = []
    threads = []
    
    def book_concurrent(thread_id):
        try:
            operations = [{
                'node_id': 'N001',
                'action': 'BOOK',
                'slot_id': f'CONCURRENT_TEST_{thread_id}',
                'patient_id': f'PAT{thread_id:03d}',
                'patient_name': f'Patient {thread_id}',
                'requires_exclusive': True
            }]
            
            response = send_to_coordinator({
                'type': 'BEGIN_TXN',
                'operations': operations
            })
            
            results.append({
                'thread_id': thread_id,
                'success': response.get('success', False),
                'transaction_id': response.get('transaction_id'),
                'error': response.get('error')
            })
        except Exception as e:
            results.append({
                'thread_id': thread_id,
                'success': False,
                'error': str(e)
            })
    
    # Start concurrent threads
    for i in range(num_concurrent):
        thread = threading.Thread(target=book_concurrent, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join(timeout=10)
    
    return jsonify({
        'success': True,
        'results': results,
        'summary': {
            'total': len(results),
            'successful': sum(1 for r in results if r.get('success')),
            'failed': sum(1 for r in results if not r.get('success'))
        }
    })

if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)