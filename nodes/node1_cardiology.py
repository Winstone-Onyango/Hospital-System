#!/usr/bin/env python3
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
