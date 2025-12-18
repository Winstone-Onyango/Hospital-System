#!/usr/bin/env python3
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
