# worker.py
import time
import hashlib
import os
import datetime
from database import get_session_factory, Session as DbSession, SessionStatus

class HealingWorker:
    def __init__(self, session_id, item1_bytes, item2_bytes, session_description, end_time):
        self.session_id = session_id
        self.item1_bytes = item1_bytes
        self.item2_bytes = item2_bytes
        self.session_description = session_description
        self.end_time = end_time
        Session_Factory = get_session_factory()
        self.db_session = Session_Factory()

    def _query_data(self, data_bytes):
        return hashlib.sha256(data_bytes).hexdigest()

    def _perform_work_cycle(self):
        """A single cycle of hashing both data packages."""
        self._query_data(self.item1_bytes)
        self._query_data(self.item2_bytes)
        time.sleep(1)

    def run(self):
        print(f"[Worker PID: {os.getpid()}] Starting session {self.session_id}: {self.session_description}")
        try:
            if self.end_time is None:
                print(f"[Worker PID: {os.getpid()}] Session {self.session_id} is running indefinitely.")
                while True:
                    self._perform_work_cycle()
            else:
                print(f"[Worker PID: {os.getpid()}] Session {self.session_id} will run until {self.end_time}.")
                while datetime.datetime.utcnow() < self.end_time:
                    self._perform_work_cycle()
            
            self._update_status(SessionStatus.COMPLETED)
            print(f"[Worker PID: {os.getpid()}] Session {self.session_id} completed successfully.")

        except KeyboardInterrupt:
            self._update_status(SessionStatus.STOPPED)
        except Exception as e:
            print(f"[Worker PID: {os.getpid()}] Error in session {self.session_id}: {e}")
            self._update_status(SessionStatus.FAILED)
        finally:
            self.db_session.close()

    def _update_status(self, status: SessionStatus):
        try:
            session = self.db_session.query(DbSession).filter(DbSession.id == self.session_id).first()
            if session:
                session.status = status
                session.worker_pid = None
                self.db_session.commit()
        except Exception as e:
            print(f"Failed to update session status for {self.session_id}: {e}")
            self.db_session.rollback()

