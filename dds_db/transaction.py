import functools
import uuid


def transactional(fn):
    """Decorator that runs the function inside a transaction and releases locks in a finally block."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        from dds_db.DDSdb import db  # lazy to avoid circular import with dds_db.__init__
        txn_id = uuid.uuid4()
        db._set_current_txn(txn_id)
        try:
            return fn(*args, **kwargs)
        finally:
            db.release_txn_locks(txn_id)
            db._clear_current_txn()
    return wrapper

def async_transactional(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        from dds_db.DDSdb import db
        txn_id = uuid.uuid4()
        db._set_current_txn(txn_id)
        try:
            return await fn(*args, **kwargs)
        finally:
            db.release_txn_locks(txn_id)
            db._clear_current_txn()
    return wrapper