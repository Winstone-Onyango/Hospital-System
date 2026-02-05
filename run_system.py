"""
Main script to run the entire distributed hospital system
"""
import subprocess 
import time
import sys
import os
from threading import Thread

# Store base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_coordinator():
    """Run the coordinator server"""
    print("Starting Coordinator Server...")
    coord_dir = os.path.join(BASE_DIR, 'coordinator')
    script_path = os.path.join(coord_dir, 'coordinator_server.py')
    subprocess.run([sys.executable, script_path], cwd=coord_dir)

def run_node(node_script):
    """Run a node server"""
    print(f"Starting {node_script}...")
    nodes_dir = os.path.join(BASE_DIR, 'nodes')
    script_path = os.path.join(nodes_dir, node_script)
    subprocess.run([sys.executable, script_path], cwd=nodes_dir)

def run_web_app():
    """Run the web application"""
    print("Starting Web Application...")
    web_dir = os.path.join(BASE_DIR, 'web_app')
    script_path = os.path.join(web_dir, 'app.py')
    subprocess.run([sys.executable, script_path], cwd=web_dir)

def main():
    """Main function to start all components"""
    print("=" * 60)
    print("GROUP 4 MATIBABU REFERRAL HOSPITAL")
    print("Distributed Hospital Management System")
    print("=" * 60)
    print("\nStarting all system components...\n")
    
    # Create threads for each component
    threads = []
    
    # Coordinator
    coord_thread = Thread(target=run_coordinator)
    threads.append(coord_thread)
    
    # Nodes
    nodes = [
        'node1_cardiology.py',
        'node2_radiology.py',
        'node3_orthopedics.py',
        'node4_emergency.py'
    ]
    
    node_threads = []
    for node in nodes:
        thread = Thread(target=run_node, args=(node,))
        node_threads.append(thread)
        threads.append(thread)
    
    # Web App (with delay to ensure nodes are up)
    def delayed_web_start():
        time.sleep(3)
        run_web_app()
    
    web_thread = Thread(target=delayed_web_start)
    threads.append(web_thread)
    
    # Start all threads
    for thread in threads:
        thread.start()
        time.sleep(1)  # Small delay between starts
    
    print("\n" + "=" * 60)
    print("System Components Started Successfully!")
    print("=" * 60)
    print("\nAccess Points:")
    print("1. Web Interface: http://localhost:8080")
    print("2. Coordinator API: localhost:5000")
    print("3. Node Status Pages:")
    print("   - Cardiology: http://localhost:5101")
    print("   - Radiology: http://localhost:5102")
    print("   - Orthopedics: http://localhost:5103")
    print("   - Emergency: http://localhost:5104")
    print("\nPress Ctrl+C to stop all components")
    print("=" * 60)
    
    # Keep main thread alive
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nShutting down system...")
        sys.exit(0)

def create_node_scripts():
    """Create individual node starter scripts"""
    
    # Cardiology Node
    cardiology_script = """#!/usr/bin/env python3
from node_server import NodeServer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    node = NodeServer(
        node_id='N001',
        node_name='Cardiology Department',
        host='localhost',
        port=5001,
        total_slots=15,
        coordinator_host='localhost',
        coordinator_port=5000
    )
    node.start()
"""
    
    # Radiology Node
    radiology_script = """#!/usr/bin/env python3
from node_server import NodeServer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    node = NodeServer(
        node_id='N002',
        node_name='Radiology Department',
        host='localhost',
        port=5002,
        total_slots=10,
        coordinator_host='localhost',
        coordinator_port=5000
    )
    node.start()
"""
    
    # Orthopedics Node
    orthopedics_script = """#!/usr/bin/env python3
from node_server import NodeServer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    node = NodeServer(
        node_id='N003',
        node_name='Orthopedics Department',
        host='localhost',
        port=5003,
        total_slots=12,
        coordinator_host='localhost',
        coordinator_port=5000
    )
    node.start()
"""
    
    # Emergency Node
    emergency_script = """#!/usr/bin/env python3
from node_server import NodeServer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    node = NodeServer(
        node_id='N004',
        node_name='Emergency Department',
        host='localhost',
        port=5004,
        total_slots=20,
        coordinator_host='localhost',
        coordinator_port=5000
    )
    node.start()
"""
    
    # Write scripts to files
    scripts = {
        'node1_cardiology.py': cardiology_script,
        'node2_radiology.py': radiology_script,
        'node3_orthopedics.py': orthopedics_script,
        'node4_emergency.py': emergency_script
    }
    
    os.makedirs('nodes', exist_ok=True)
    
    for filename, content in scripts.items():
        with open(f'nodes/{filename}', 'w') as f:
            f.write(content)
        print(f"Created {filename}")

if __name__ == "__main__":
    # Create individual node starter scripts
    create_node_scripts()
    main()
