"""
Database module initialization and global instances
"""
import os
from dotenv import load_dotenv

# Load .env relative to this file so it works regardless of working directory
_here = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(_here, '..', '.env'))

from .database_manager import DatabaseManager

SignalDatabase = None

# Global database instance
db = DatabaseManager()

# Global signal database instance (initialized after db)
signal_db = None


def initialize_signal_db(db_manager: DatabaseManager):
    """
    Initialize the signal database handler

    Args:
        db_manager: DatabaseManager instance

    Returns:
        SignalDatabase instance
    """
    global signal_db, SignalDatabase

    # Lazy import to avoid circular dependencies
    if SignalDatabase is None:
        from .signal_operations import SignalDatabase as SignalDB
        SignalDatabase = SignalDB

    signal_db = SignalDatabase(db_manager)
    return signal_db


# Export main components for backward compatibility
__all__ = ['db', 'signal_db', 'initialize_signal_db', 'DatabaseManager', 'SignalDatabase']