"""
Data store for hospital department nodes with transaction support
"""
import json
import threading
import time
import logging
from typing import Dict, List, Optional
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SlotStatus(Enum):
    """Status of appointment slots"""
    AVAILABLE = "AVAILABLE"
    BOOKED = "BOOKED"
    PENDING = "PENDING"  # In transaction
    BLOCKED = "BLOCKED"  # For maintenance

class Appointment:
    """Represents a hospital appointment"""
    
    def __init__(self, appointment_id: str, patient_id: str, patient_name: str,
                 department: str, slot_time: str, slot_date: str, status: SlotStatus):
        self.appointment_id = appointment_id
        self.patient_id = patient_id
        self.patient_name = patient_name
        self.department = department
        self.slot_time = slot_time
        self.slot_date = slot_date
        self.status = status
        self.created_at = time.time()
        self.transaction_id = None

class DataStore:
    """Data store for hospital department with transaction support"""
    
    def __init__(self, department_id: str, department_name: str, total_slots: int):
        self.department_id = department_id
        self.department_name = department_name
        self.total_slots = total_slots
        self.available_slots = total_slots
        self.appointments: Dict[str, Appointment] = {}
        self.transaction_log: Dict[str, Dict] = {}  # transaction_id -> operations
        self.lock = threading.RLock()
        
        # Initialize some sample slots
        self._initialize_sample_slots()
    
    def _initialize_sample_slots(self):
        """Initialize sample appointment slots"""
        sample_times = ["09:00", "10:00", "11:00", "14:00", "15:00", "16:00"]
        for i, slot_time in enumerate(sample_times[:min(6, self.total_slots)]):
            slot_id = f"SLOT{self.department_id}{i+1:03d}"
            appointment = Appointment(
                appointment_id=slot_id,
                patient_id="",
                patient_name="",
                department=self.department_name,
                slot_time=slot_time,
                slot_date="2024-03-20",
                status=SlotStatus.AVAILABLE
            )
            self.appointments[slot_id] = appointment
    
    def begin_transaction(self, transaction_id: str) -> bool:
        """Begin a transaction"""
        with self.lock:
            if transaction_id in self.transaction_log:
                return False
            
            self.transaction_log[transaction_id] = {
                'start_time': time.time(),
                'operations': [],
                'state': 'ACTIVE'
            }
            return True
    
    def book_appointment(self, transaction_id: str, slot_id: str, 
                        patient_id: str, patient_name: str) -> bool:
        """Book an appointment within a transaction"""
        with self.lock:
            if transaction_id not in self.transaction_log:
                return False
            
            if slot_id not in self.appointments:
                return False
            
            appointment = self.appointments[slot_id]
            
            if appointment.status != SlotStatus.AVAILABLE:
                return False
            
            # Store original state for rollback
            original_status = appointment.status
            original_patient_id = appointment.patient_id
            original_patient_name = appointment.patient_name
            
            # Update appointment
            appointment.status = SlotStatus.PENDING
            appointment.patient_id = patient_id
            appointment.patient_name = patient_name
            appointment.transaction_id = transaction_id
            
            # Log operation for rollback
            self.transaction_log[transaction_id]['operations'].append({
                'type': 'BOOK',
                'slot_id': slot_id,
                'original_status': original_status.value,
                'original_patient_id': original_patient_id,
                'original_patient_name': original_patient_name,
                'timestamp': time.time()
            })
            
            logger.info(f"Transaction {transaction_id}: Booked slot {slot_id} for {patient_name}")
            return True
    
    def prepare_transaction(self, transaction_id: str) -> bool:
        """Prepare transaction for commit (vote yes if ready)"""
        with self.lock:
            if transaction_id not in self.transaction_log:
                return False
            
            # Check if all operations can be committed
            for op in self.transaction_log[transaction_id]['operations']:
                if op['type'] == 'BOOK':
                    slot_id = op['slot_id']
                    if slot_id in self.appointments:
                        appointment = self.appointments[slot_id]
                        if appointment.status != SlotStatus.PENDING:
                            return False
            
            self.transaction_log[transaction_id]['state'] = 'PREPARED'
            return True
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """Commit a transaction"""
        with self.lock:
            if transaction_id not in self.transaction_log:
                return False
            
            # Apply all operations
            for op in self.transaction_log[transaction_id]['operations']:
                if op['type'] == 'BOOK':
                    slot_id = op['slot_id']
                    if slot_id in self.appointments:
                        appointment = self.appointments[slot_id]
                        if appointment.status == SlotStatus.PENDING:
                            appointment.status = SlotStatus.BOOKED
                            self.available_slots -= 1
            
            # Clear transaction log
            del self.transaction_log[transaction_id]
            
            logger.info(f"Transaction {transaction_id} committed")
            return True
    
    def abort_transaction(self, transaction_id: str) -> bool:
        """Abort a transaction and rollback changes"""
        with self.lock:
            if transaction_id not in self.transaction_log:
                return False
            
            # Rollback all operations in reverse order
            for op in reversed(self.transaction_log[transaction_id]['operations']):
                if op['type'] == 'BOOK':
                    slot_id = op['slot_id']
                    if slot_id in self.appointments:
                        appointment = self.appointments[slot_id]
                        appointment.status = SlotStatus(op['original_status'])
                        appointment.patient_id = op['original_patient_id']
                        appointment.patient_name = op['original_patient_name']
                        appointment.transaction_id = None
            
            # Clear transaction log
            del self.transaction_log[transaction_id]
            
            logger.info(f"Transaction {transaction_id} aborted and rolled back")
            return True
    
    def get_available_slots(self) -> List[Dict]:
        """Get all available slots"""
        with self.lock:
            available = []
            for appointment in self.appointments.values():
                if appointment.status == SlotStatus.AVAILABLE:
                    available.append({
                        'slot_id': appointment.appointment_id,
                        'time': appointment.slot_time,
                        'date': appointment.slot_date,
                        'department': appointment.department
                    })
            return available
    
    def get_appointment(self, slot_id: str) -> Optional[Dict]:
        """Get appointment details"""
        with self.lock:
            if slot_id in self.appointments:
                appointment = self.appointments[slot_id]
                return {
                    'slot_id': appointment.appointment_id,
                    'patient_id': appointment.patient_id,
                    'patient_name': appointment.patient_name,
                    'department': appointment.department,
                    'time': appointment.slot_time,
                    'date': appointment.slot_date,
                    'status': appointment.status.value,
                    'transaction_id': appointment.transaction_id
                }
            return None
    
    def get_department_status(self) -> Dict:
        """Get department status"""
        with self.lock:
            booked_count = sum(1 for a in self.appointments.values() 
                             if a.status == SlotStatus.BOOKED)
            pending_count = sum(1 for a in self.appointments.values() 
                              if a.status == SlotStatus.PENDING)
            
            return {
                'department_id': self.department_id,
                'department_name': self.department_name,
                'total_slots': self.total_slots,
                'available_slots': self.available_slots,
                'booked_slots': booked_count,
                'pending_slots': pending_count,
                'active_transactions': len(self.transaction_log)
            }
    
    def add_slot(self, slot_time: str, slot_date: str) -> str:
        """Add a new slot"""
        with self.lock:
            slot_id = f"SLOT{self.department_id}{len(self.appointments)+1:03d}"
            appointment = Appointment(
                appointment_id=slot_id,
                patient_id="",
                patient_name="",
                department=self.department_name,
                slot_time=slot_time,
                slot_date=slot_date,
                status=SlotStatus.AVAILABLE
            )
            self.appointments[slot_id] = appointment
            self.total_slots += 1
            self.available_slots += 1
            return slot_id