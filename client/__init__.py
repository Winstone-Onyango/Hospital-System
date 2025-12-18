"""
Client package for interacting with the distributed hospital system
"""

from .client import HospitalClient
from .api_client import APIClient

__all__ = ['HospitalClient', 'APIClient']