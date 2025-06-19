# healer_daemon.py
import socket
import json
import multiprocessing
import time
import datetime
import base64
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy import or_
from sqlalchemy import text as sa_text
from database import (get_session_factory, setup_database, Avatar, InformationCopy,
                      Request, Session, SessionStatus, SessionType, ICGroup, 
                      ICGroupMember, AvatarGroup, AvatarGroupMember, RequestGroup, RequestGroupMember)
from worker import HealingWorker
from config import DAEMON_HOST, DAEMON_PORT, DATABASE_URL

class HealerDaemon:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        Session_Factory = get_session_factory()
        self.db_session = Session_Factory()
        self.ic_cache = {}
        self.avatar_cache = {}
        self.request_cache = {}
        self.active_workers = {}

    # --- Caching and Spawning ---
    def _load_avatar_to_cache(self, avatar_id):
        if avatar_id not in self.avatar_cache:
            avatar = self.db_session.query(Avatar).filter(Avatar.id == avatar_id).first()
            if not avatar: raise ValueError(f"Avatar ID {avatar_id} not found.")
            self.avatar_cache[avatar_id] = avatar.photo_data + avatar.info_data.encode('utf-8')
        return self.avatar_cache[avatar_id]

    def _load_ic_to_cache(self, ic_id):
        if ic_id not in self.ic_cache:
            ic = self.db_session.query(InformationCopy).filter(InformationCopy.id == ic_id).first()
            if not ic: raise ValueError(f"IC ID {ic_id} not found.")
            self.ic_cache[ic_id] = ic.wav_data
        return self.ic_cache[ic_id]
        
    def _load_request_to_cache(self, request_id):
        if request_id not in self.request_cache:
            request = self.db_session.query(Request).filter(Request.id == request_id).first()
            if not request: raise ValueError(f"Request ID {request_id} not found.")
            self.request_cache[request_id] = request.request_data.encode('utf-8')
        return self.request_cache[request_id]

    def _spawn_worker_for_session(self, session):
        if not session or not session.id:
            print("Error: Invalid session object passed to _spawn_worker_for_session.")
            return

        if session.id in self.active_workers:
             print(f"Warning: Worker for session {session.id} already exists. Skipping spawn.")
             return
        
        try:
            item1_bytes = b''
            item2_bytes = b''

            if session.session_type in [SessionType.IC_SESSION, SessionType.GROUP_IC_SESSION]:
                item1_bytes = self._load_avatar_to_cache(session.avatar_id)
                item2_bytes = self._load_ic_to_cache(session.ic_id)
            elif session.session_type == SessionType.REQUEST_SESSION:
                item1_bytes = self._load_avatar_to_cache(session.avatar_id)
                item2_bytes = self._load_request_to_cache(session.request_id)
            elif session.session_type == SessionType.AVATAR_LINK:
                item1_bytes = self._load_avatar_to_cache(session.avatar_id)
                item2_bytes = self._load_avatar_to_cache(session.destination_avatar_id)
            
            worker = HealingWorker(session.id, item1_bytes, item2_bytes, session.description, session.end_time)
            process = multiprocessing.Process(target=worker.run, daemon=True)
            process.start()

            session.status = SessionStatus.RUNNING
            session.worker_pid = process.pid
            self.db_session.commit()

            self.active_workers[session.id] = process
            print(f"Started worker for session {session.id} (PID: {process.pid})")
        except Exception as e:
            print(f"Failed to spawn worker for session {session.id}: {e}")
            session.status = SessionStatus.FAILED
            self.db_session.commit()
    
    def _stop_single_session(self, session_id):
        session = self.db_session.query(Session).get(session_id)
        if not session or session.status != SessionStatus.RUNNING:
            return False
            
        process = self.active_workers.pop(session.id, None)
        if process and process.is_alive():
            process.terminate()
            process.join()
        
        session.status = SessionStatus.STOPPED
        session.worker_pid = None
        self.db_session.commit()
        return True

    def _fail_single_session(self, session_id):
        session = self.db_session.query(Session).get(session_id)
        if not session or session.status != SessionStatus.RUNNING:
            return False
            
        process = self.active_workers.pop(session.id, None)
        if process and process.is_alive():
            process.terminate()
            process.join()
        
        session.status = SessionStatus.FAILED
        session.worker_pid = None
        self.db_session.commit()
        return True

    # --- Handler Implementations ---
    def _get_target_avatar_ids(self, avatar_id, avatar_group_name):
        """Helper to resolve a single avatar ID or a group of IDs."""
        if avatar_id:
            avatar = self.db_session.query(Avatar).filter(Avatar.id == avatar_id).first()
            if not avatar: raise ValueError(f"Avatar ID {avatar_id} not found.")
            return [avatar_id]
        if avatar_group_name:
            group = self.db_session.query(AvatarGroup).options(selectinload(AvatarGroup.members)).filter(AvatarGroup.name == avatar_group_name).first()
            if not group: raise ValueError(f"Avatar group '{avatar_group_name}' not found.")
            ids = [m.avatar_id for m in group.members]
            if not ids: raise ValueError(f"Avatar group '{avatar_group_name}' is empty.")
            return ids
        raise ValueError("No target avatar or avatar group specified.")

    def _get_target_request_ids(self, request_id, request_group_name):
        """Helper to resolve a single request ID or a group of IDs."""
        if request_id:
            request = self.db_session.query(Request).filter(Request.id == request_id).first()
            if not request: raise ValueError(f"Request ID {request_id} not found.")
            return [request_id]
        if request_group_name:
            group = self.db_session.query(RequestGroup).options(selectinload(RequestGroup.members)).filter(RequestGroup.name == request_group_name).first()
            if not group: raise ValueError(f"Request group '{request_group_name}' not found.")
            ids = [m.request_id for m in group.members]
            if not ids: raise ValueError(f"Request group '{request_group_name}' is empty.")
            return ids
        raise ValueError("No target request or request group specified.")

    def handle_start_ic(self, data):
        """Applies a single IC to a single avatar or an avatar group."""
        try:
            target_avatar_ids = self._get_target_avatar_ids(data.get('avatar_id'), data.get('avatar_group'))
            ic_id = data['ic_id']
            ic = self.db_session.query(InformationCopy).get(ic_id)
            if not ic: raise ValueError(f"IC {ic_id} not found.")
            
            parent_session = None
            if len(target_avatar_ids) > 1:
                avatar_group = self.db_session.query(AvatarGroup).filter_by(name=data['avatar_group']).first()
                desc = f"IC '{ic.name}' on Avatar Group '{avatar_group.name}'"
                parent_session = Session(
                    is_group_session=True, description=desc, avatar_group_id=avatar_group.id, ic_id=ic.id,
                    session_type=SessionType.IC_SESSION, start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.RUNNING
                )
                self.db_session.add(parent_session)
                self.db_session.commit()

            created_sessions = 0
            for avatar_id in target_avatar_ids:
                avatar = self.db_session.query(Avatar).get(avatar_id)
                child_desc = f"'{avatar.name}' <=> '{ic.name}'"
                if parent_session: child_desc += f" (from Group Op #{parent_session.id})"
                
                child_session = Session(
                    parent_session_id=parent_session.id if parent_session else None,
                    avatar_id=avatar_id, ic_id=ic_id, description=child_desc,
                    session_type=SessionType.IC_SESSION,
                    start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.SCHEDULED
                )
                self.db_session.add(child_session)
                self.db_session.commit()
                self._spawn_worker_for_session(child_session)
                created_sessions += 1

            return {"status": "success", "message": f"Started {created_sessions} session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_start_request(self, data):
        """Applies a single Request or a Request Group to a single avatar or an avatar group."""
        try:
            target_avatar_ids = self._get_target_avatar_ids(data.get('avatar_id'), data.get('avatar_group'))
            target_request_ids = self._get_target_request_ids(data.get('request_id'), data.get('request_group'))

            parent_session = None
            # Create a parent session if we are targeting groups for *both* avatars and requests.
            if len(target_avatar_ids) > 1 and len(target_request_ids) > 1:
                avatar_group = self.db_session.query(AvatarGroup).filter_by(name=data['avatar_group']).first()
                request_group = self.db_session.query(RequestGroup).filter_by(name=data['request_group']).first()
                desc = f"Request Group '{request_group.name}' on Avatar Group '{avatar_group.name}'"
                parent_session = Session(
                    is_group_session=True, description=desc,
                    avatar_group_id=avatar_group.id, request_group_id=request_group.id,
                    session_type=SessionType.REQUEST_SESSION, start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.RUNNING
                )
                self.db_session.add(parent_session)
                self.db_session.commit()
            # Also create a parent session if we are targeting a group for just one of them.
            elif len(target_avatar_ids) > 1:
                avatar_group = self.db_session.query(AvatarGroup).filter_by(name=data['avatar_group']).first()
                request = self.db_session.query(Request).get(target_request_ids[0])
                desc = f"Request '{request.name}' on Avatar Group '{avatar_group.name}'"
                parent_session = Session(
                    is_group_session=True, description=desc,
                    avatar_group_id=avatar_group.id, request_id=request.id,
                    session_type=SessionType.REQUEST_SESSION, start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.RUNNING
                )
                self.db_session.add(parent_session)
                self.db_session.commit()
            elif len(target_request_ids) > 1:
                request_group = self.db_session.query(RequestGroup).filter_by(name=data['request_group']).first()
                avatar = self.db_session.query(Avatar).get(target_avatar_ids[0])
                desc = f"Request Group '{request_group.name}' on Avatar '{avatar.name}'"
                parent_session = Session(
                    is_group_session=True, description=desc,
                    request_group_id=request_group.id, avatar_id=avatar.id,
                    session_type=SessionType.REQUEST_SESSION, start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.RUNNING
                )
                self.db_session.add(parent_session)
                self.db_session.commit()

            created_sessions = 0
            for avatar_id in target_avatar_ids:
                for request_id in target_request_ids:
                    avatar = self.db_session.query(Avatar).get(avatar_id)
                    request = self.db_session.query(Request).get(request_id)
                    child_desc = f"'{avatar.name}' <=> '{request.name}'"
                    if parent_session: child_desc += f" (from Group Op #{parent_session.id})"
                    
                    child_session = Session(
                        parent_session_id=parent_session.id if parent_session else None,
                        avatar_id=avatar_id, request_id=request_id, description=child_desc,
                        session_type=SessionType.REQUEST_SESSION,
                        start_time=datetime.datetime.utcnow(),
                        end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                        status=SessionStatus.SCHEDULED
                    )
                    self.db_session.add(child_session)
                    self.db_session.commit()
                    self._spawn_worker_for_session(child_session)
                    created_sessions += 1

            return {"status": "success", "message": f"Started {created_sessions} request session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_start_link(self, data):
        """Links a source avatar to a destination avatar or destination group."""
        try:
            source_id = data['source_id']
            dest_avatar_ids = self._get_target_avatar_ids(data.get('dest_id'), data.get('dest_group'))
            
            source_avatar = self.db_session.query(Avatar).get(source_id)
            if not source_avatar: raise ValueError(f"Source Avatar {source_id} not found.")

            parent_session = None
            if len(dest_avatar_ids) > 1:
                dest_group = self.db_session.query(AvatarGroup).filter_by(name=data['dest_group']).first()
                desc = f"Link from '{source_avatar.name}' to Avatar Group '{dest_group.name}'"
                parent_session = Session(
                    is_group_session=True, description=desc, avatar_id=source_id, avatar_group_id=dest_group.id,
                    session_type=SessionType.AVATAR_LINK, start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.RUNNING
                )
                self.db_session.add(parent_session)
                self.db_session.commit()

            created_sessions = 0
            for dest_id in dest_avatar_ids:
                if source_id == dest_id: continue
                dest_avatar = self.db_session.query(Avatar).get(dest_id)
                child_desc = f"Link: '{source_avatar.name}' -> '{dest_avatar.name}'"
                if parent_session: child_desc += f" (from Group Op #{parent_session.id})"
                
                child_session = Session(
                    parent_session_id=parent_session.id if parent_session else None,
                    avatar_id=source_id, destination_avatar_id=dest_id, description=child_desc,
                    session_type=SessionType.AVATAR_LINK,
                    start_time=datetime.datetime.utcnow(),
                    end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                    status=SessionStatus.SCHEDULED
                )
                self.db_session.add(child_session)
                self.db_session.commit()
                self._spawn_worker_for_session(child_session)
                created_sessions += 1

            return {"status": "success", "message": f"Started {created_sessions} link session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_start_group(self, data):
        try:
            avatar_group_name = data.get('avatar_group')
            avatar_group = self.db_session.query(AvatarGroup).options(selectinload(AvatarGroup.members).joinedload(AvatarGroupMember.avatar)).filter_by(name=avatar_group_name).first()
            if not avatar_group: raise ValueError(f"Avatar group '{avatar_group_name}' not found.")

            ic_group_name = data.get('ic_group')
            ic_group = self.db_session.query(ICGroup).options(selectinload(ICGroup.members).joinedload(ICGroupMember.ic)).filter_by(name=ic_group_name).first()
            if not ic_group: raise ValueError(f"IC group '{ic_group_name}' not found.")

            if not avatar_group.members or not ic_group.members:
                return {"status": "error", "message": "Both avatar and IC groups must be non-empty."}

            desc = f"IC Group '{ic_group.name}' on Avatar Group '{avatar_group.name}'"
            parent_session = Session(
                is_group_session=True, description=desc, avatar_group_id=avatar_group.id, ic_group_id=ic_group.id,
                session_type=SessionType.GROUP_IC_SESSION, start_time=datetime.datetime.utcnow(),
                end_time=(datetime.datetime.utcnow() + datetime.timedelta(minutes=data['duration'])) if data.get('duration') else None,
                status=SessionStatus.RUNNING 
            )
            self.db_session.add(parent_session)
            self.db_session.commit()

            for avatar_member in avatar_group.members:
                for ic_member in ic_group.members:
                    child_desc = f"'{avatar_member.avatar.name}' <=> '{ic_member.ic.name}' (from Group Session #{parent_session.id})"
                    child_session = Session(
                        parent_session_id=parent_session.id, avatar_id=avatar_member.avatar_id, ic_id=ic_member.ic_id,
                        description=child_desc, session_type=SessionType.IC_SESSION,
                        start_time=parent_session.start_time, end_time=parent_session.end_time,
                        status=SessionStatus.SCHEDULED
                    )
                    self.db_session.add(child_session)
                    self.db_session.commit()
                    self._spawn_worker_for_session(child_session)
            
            return {"status": "success", "message": f"Started group session {parent_session.id} with {len(avatar_group.members) * len(ic_group.members)} workers."}
        
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_stop_session(self, data):
        session_id = data.get('session_id')
        session = self.db_session.query(Session).options(joinedload(Session.child_sessions)).filter_by(id=session_id).first()
        if not session: return {"status": "error", "message": f"Session {session_id} not found."}
        
        sessions_to_stop = [session]
        if session.is_group_session or session.parent_session_id is None:
            sessions_to_stop.extend(session.child_sessions)

        stopped_count = 0
        for s in sessions_to_stop:
            if s.status == SessionStatus.RUNNING:
                if self._stop_single_session(s.id):
                    stopped_count += 1
        
        session.status = SessionStatus.STOPPED
        self.db_session.commit()
        return {"status": "success", "message": f"Stopped {stopped_count} session(s)."}

    def handle_update_entity(self, data):
        entity_type = data['entity_type']
        entity_id = data['id']
        
        try:
            if entity_type == 'avatar':
                avatar = self.db_session.query(Avatar).filter_by(id=entity_id).first()
                if not avatar: return {"status": "error", "message": f"Avatar {entity_id} not found."}
                if 'photo_data_b64' in data:
                    avatar.photo_data = bytes(json.loads(data['photo_data_b64']))
                if 'info_data' in data:
                    avatar.info_data = data['info_data']
                self.avatar_cache.pop(entity_id, None)
            else:
                 return {"status": "error", "message": f"Entity type '{entity_type}' not supported for updates."}

            self.db_session.commit()

            affected_sessions = self.db_session.query(Session).filter(
                Session.status == SessionStatus.RUNNING,
                or_(Session.avatar_id == entity_id, Session.destination_avatar_id == entity_id)
            ).all()

            restarted_count = 0
            for session in affected_sessions:
                self.db_session.refresh(session)
                if self._stop_single_session(session.id):
                    self._spawn_worker_for_session(session)
                    restarted_count += 1

            return {"status": "success", "message": f"Entity updated. Restarted {restarted_count} active session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_view_running_on(self, data):
        avatar_identifier = data['avatar_identifier']
        try:
            query = self.db_session.query(Avatar)
            if avatar_identifier.isdigit():
                avatar = query.get(int(avatar_identifier))
            else:
                avatar = query.filter(Avatar.name == avatar_identifier).first()
            if not avatar: return {"status": "error", "message": f"Avatar '{avatar_identifier}' not found."}

            avatar_groups = self.db_session.query(AvatarGroup.id).join(AvatarGroupMember).filter(AvatarGroupMember.avatar_id == avatar.id).all()
            avatar_group_ids = [g[0] for g in avatar_groups]

            sessions = self.db_session.query(Session).filter(
                Session.status == SessionStatus.RUNNING,
                Session.is_group_session == False,
                or_(
                    Session.avatar_id == avatar.id,
                    Session.destination_avatar_id == avatar.id,
                    Session.parent_session.has(Session.avatar_group_id.in_(avatar_group_ids))
                )
            ).options(
                joinedload(Session.ic), 
                joinedload(Session.request), 
                joinedload(Session.dest_avatar),
                joinedload(Session.parent_session).joinedload(Session.ic_group),
                joinedload(Session.parent_session).joinedload(Session.avatar_group)
            ).all()
            
            response_data = []
            for s in sessions:
                target_str = s.description
                if s.parent_session and s.parent_session.is_group_session:
                    target_str = f"Part of Group Session #{s.parent_session.id}: {s.parent_session.description}"

                duration = int((s.end_time - s.start_time).total_seconds() / 60) if s.end_time else None
                response_data.append({
                    "session_id": s.id, "type": s.session_type.value, "target": target_str, "duration_minutes": duration
                })

            return {"status": "success", "avatar_name": avatar.name, "avatar_id": avatar.id, "data": response_data}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def handle_add_member_to_group(self, data):
        group_type = data['group_type']
        group_name = data['group_name']
        member_id = data['member_id']
        
        try:
            new_workers = 0
            if group_type == 'ic':
                group = self.db_session.query(ICGroup).filter_by(name=group_name).first()
                if not group: return {"status": "error", "message": f"IC group '{group_name}' not found."}
                if not self.db_session.query(InformationCopy).get(member_id): return {"status": "error", "message": f"IC {member_id} not found."}
                
                if self.db_session.query(ICGroupMember).filter_by(group_id=group.id, ic_id=member_id).first():
                    return {"status": "success", "message": f"IC {member_id} is already in group '{group_name}'."}
                
                self.db_session.add(ICGroupMember(group_id=group.id, ic_id=member_id))
                self.db_session.commit()
                
                active_group_sessions = self.db_session.query(Session).filter(
                    Session.is_group_session == True,
                    Session.status == SessionStatus.RUNNING,
                    Session.ic_group_id == group.id
                ).options(selectinload(Session.avatar_group).selectinload(AvatarGroup.members).joinedload(AvatarGroupMember.avatar)).all()

                for parent_session in active_group_sessions:
                    for avatar_member in parent_session.avatar_group.members:
                        new_workers +=1
                
                return {"status": "success", "message": f"Added IC {member_id} to group '{group_name}'. Started {new_workers} new live session(s)."}
            
            elif group_type == 'avatar':
                group = self.db_session.query(AvatarGroup).filter_by(name=group_name).first()
                if not group: return {"status": "error", "message": f"Avatar group '{group_name}' not found."}
                
                new_member_avatar = self.db_session.query(Avatar).get(member_id)
                if not new_member_avatar: return {"status": "error", "message": f"Avatar {member_id} not found."}

                if self.db_session.query(AvatarGroupMember).filter_by(group_id=group.id, avatar_id=member_id).first():
                    return {"status": "success", "message": f"Avatar {member_id} is already in group '{group_name}'."}

                self.db_session.add(AvatarGroupMember(group_id=group.id, avatar_id=member_id))
                self.db_session.commit()

                active_group_sessions = self.db_session.query(Session).filter(
                    Session.is_group_session == True,
                    Session.status == SessionStatus.RUNNING,
                    Session.avatar_group_id == group.id
                ).options(
                    selectinload(Session.ic_group).selectinload(ICGroup.members).joinedload(ICGroupMember.ic),
                    selectinload(Session.ic),
                    selectinload(Session.request),
                    selectinload(Session.source_avatar)
                ).all()
                
                for parent_session in active_group_sessions:
                    # Case 1: Group-to-Group session
                    if parent_session.session_type == SessionType.GROUP_IC_SESSION:
                        if not parent_session.ic_group:
                            continue
                        for ic_member in parent_session.ic_group.members:
                            child_desc = f"'{new_member_avatar.name}' <=> '{ic_member.ic.name}' (from Group Session #{parent_session.id})"
                            child_session = Session(
                                parent_session_id=parent_session.id, avatar_id=new_member_avatar.id, ic_id=ic_member.ic_id,
                                description=child_desc, session_type=SessionType.IC_SESSION,
                                start_time=parent_session.start_time, end_time=parent_session.end_time,
                                status=SessionStatus.SCHEDULED
                            )
                            self.db_session.add(child_session)
                            self.db_session.commit()
                            self._spawn_worker_for_session(child_session)
                            new_workers += 1
                    
                    # Case 2: Single IC to Group
                    elif parent_session.session_type == SessionType.IC_SESSION and parent_session.ic:
                        child_desc = f"'{new_member_avatar.name}' <=> '{parent_session.ic.name}' (from Group Op #{parent_session.id})"
                        child_session = Session(
                            parent_session_id=parent_session.id, avatar_id=new_member_avatar.id, ic_id=parent_session.ic_id,
                            description=child_desc, session_type=SessionType.IC_SESSION,
                            start_time=parent_session.start_time, end_time=parent_session.end_time,
                            status=SessionStatus.SCHEDULED
                        )
                        self.db_session.add(child_session)
                        self.db_session.commit()
                        self._spawn_worker_for_session(child_session)
                        new_workers += 1
                    
                    # Case 3: Single Request to Group
                    elif parent_session.session_type == SessionType.REQUEST_SESSION and parent_session.request:
                        child_desc = f"'{new_member_avatar.name}' <=> '{parent_session.request.name}' (from Group Op #{parent_session.id})"
                        child_session = Session(
                            parent_session_id=parent_session.id, avatar_id=new_member_avatar.id, request_id=parent_session.request_id,
                            description=child_desc, session_type=SessionType.REQUEST_SESSION,
                            start_time=parent_session.start_time, end_time=parent_session.end_time,
                            status=SessionStatus.SCHEDULED
                        )
                        self.db_session.add(child_session)
                        self.db_session.commit()
                        self._spawn_worker_for_session(child_session)
                        new_workers += 1

                    # Case 4: Avatar Link to Group
                    elif parent_session.session_type == SessionType.AVATAR_LINK and parent_session.source_avatar:
                        child_desc = f"Link: '{parent_session.source_avatar.name}' -> '{new_member_avatar.name}' (from Group Op #{parent_session.id})"
                        child_session = Session(
                            parent_session_id=parent_session.id, avatar_id=parent_session.avatar_id, destination_avatar_id=new_member_avatar.id,
                            description=child_desc, session_type=SessionType.AVATAR_LINK,
                            start_time=parent_session.start_time, end_time=parent_session.end_time,
                            status=SessionStatus.SCHEDULED
                        )
                        self.db_session.add(child_session)
                        self.db_session.commit()
                        self._spawn_worker_for_session(child_session)
                        new_workers += 1
                    else:
                        print("  - SKIPPING: No condition matched or a related object was missing.")

                return {"status": "success", "message": f"Added Avatar {member_id} to group '{group_name}'. Started {new_workers} new live session(s)."}

            elif group_type == 'request':
                group = self.db_session.query(RequestGroup).filter_by(name=group_name).first()
                if not group: return {"status": "error", "message": f"Request group '{group_name}' not found."}
                if not self.db_session.query(Request).get(member_id): return {"status": "error", "message": f"Request {member_id} not found."}

                if self.db_session.query(RequestGroupMember).filter_by(group_id=group.id, request_id=member_id).first():
                    return {"status": "success", "message": f"Request {member_id} is already in group '{group_name}'."}

                self.db_session.add(RequestGroupMember(group_id=group.id, request_id=member_id))
                self.db_session.commit()
                
                # For now, we will just add the member and not start new sessions.
                return {"status": "success", "message": f"Added Request {member_id} to group '{group_name}'. No new sessions started."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_remove_member_from_group(self, data):
        group_type = data['group_type']
        group_name = data['group_name']
        member_id = data['member_id']
        
        try:
            stopped_count = 0
            if group_type == 'ic':
                group = self.db_session.query(ICGroup).filter_by(name=group_name).first()
                if not group: return {"status": "error", "message": f"IC group '{group_name}' not found."}
                
                member_record = self.db_session.query(ICGroupMember).filter_by(group_id=group.id, ic_id=member_id).first()
                if not member_record: return {"status": "success", "message": f"IC {member_id} was not in group '{group_name}'."}
                
                sessions_to_stop = self.db_session.query(Session).filter(
                    Session.parent_session.has(is_group_session=True, status=SessionStatus.RUNNING, ic_group_id=group.id),
                    Session.ic_id == member_id
                ).all()
                for session in sessions_to_stop:
                    if self._stop_single_session(session.id): stopped_count += 1
                
                self.db_session.delete(member_record)
                self.db_session.commit()
                return {"status": "success", "message": f"Removed IC {member_id} from group '{group_name}'. Stopped {stopped_count} live session(s)."}
            
            elif group_type == 'avatar':
                group = self.db_session.query(AvatarGroup).filter_by(name=group_name).first()
                if not group: return {"status": "error", "message": f"Avatar group '{group_name}' not found."}

                member_record = self.db_session.query(AvatarGroupMember).filter_by(group_id=group.id, avatar_id=member_id).first()
                if not member_record: return {"status": "success", "message": f"Avatar {member_id} was not in group '{group_name}'."}

                sessions_to_stop = self.db_session.query(Session).filter(
                    Session.parent_session.has(is_group_session=True, status=SessionStatus.RUNNING, avatar_group_id=group.id),
                    Session.avatar_id == member_id
                ).all()
                for session in sessions_to_stop:
                    if self._stop_single_session(session.id): stopped_count += 1
                
                self.db_session.delete(member_record)
                self.db_session.commit()
                return {"status": "success", "message": f"Removed Avatar {member_id} from group '{group_name}'. Stopped {stopped_count} live session(s)."}

            elif group_type == 'request':
                group = self.db_session.query(RequestGroup).filter_by(name=group_name).first()
                if not group: return {"status": "error", "message": f"Request group '{group_name}' not found."}

                member_record = self.db_session.query(RequestGroupMember).filter_by(group_id=group.id, request_id=member_id).first()
                if not member_record: return {"status": "success", "message": f"Request {member_id} was not in group '{group_name}'."}

                sessions_to_stop = self.db_session.query(Session).filter(
                    Session.parent_session.has(is_group_session=True, status=SessionStatus.RUNNING, request_group_id=group.id),
                    Session.request_id == member_id
                ).all()
                for session in sessions_to_stop:
                    if self._stop_single_session(session.id): stopped_count += 1
                
                self.db_session.delete(member_record)
                self.db_session.commit()
                return {"status": "success", "message": f"Removed Request {member_id} from group '{group_name}'. Stopped {stopped_count} live session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_remove_entity(self, data):
        entity_type = data['entity_type']
        entity_id = data['id']

        try:
            entity = None
            if entity_type == 'avatar':
                entity = self.db_session.query(Avatar).options(joinedload(Avatar.source_sessions), joinedload(Avatar.dest_sessions)).get(entity_id)
            elif entity_type == 'ic':
                entity = self.db_session.query(InformationCopy).get(entity_id)
            elif entity_type == 'request':
                entity = self.db_session.query(Request).get(entity_id)
            else:
                return {"status": "error", "message": "Removal for this entity type not implemented."}

            if not entity:
                return {"status": "success", "message": f"{entity_type} {entity_id} already deleted."}

            sessions_to_stop = entity.source_sessions + entity.dest_sessions
            stopped_count = 0
            for session in sessions_to_stop:
                if self._stop_single_session(session.id):
                    stopped_count += 1

            self.db_session.delete(entity)
            self.db_session.commit()
            self.avatar_cache.pop(entity_id, None)

            return {"status": "success", "message": f"Stopped {stopped_count} session(s) and removed {entity_type} {entity_id}."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_remove_group(self, data):
        group_type = data['group_type']
        group_name = data['group_name']
        try:
            if group_type == "avatar":
                group = self.db_session.query(AvatarGroup).filter_by(name=group_name).first()
            elif group_type == "ic":
                group = self.db_session.query(ICGroup).filter_by(name=group_name).first()
            elif group_type == "request":
                group = self.db_session.query(RequestGroup).filter_by(name=group_name).first()
            else:
                return {"status": "error", "message": f"Unknown group type '{group_type}'"}

            if not group:
                return {"status": "success", "message": f"Group '{group_name}' not found or already deleted."}
            
            # This logic can be expanded to stop sessions before deleting
            self.db_session.delete(group)
            self.db_session.commit()
            return {"status": "success", "message": f"Group '{group_name}' and all its memberships have been deleted."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_fail_sessions_on_target(self, data):
        """Fails all running sessions for a given avatar ID or avatar group name."""
        avatar_id = data.get('avatar_id')
        group_name = data.get('avatar_group')
        failed_count = 0
        
        try:
            sessions_to_fail = set()

            if group_name:
                group = self.db_session.query(AvatarGroup).filter(AvatarGroup.name == group_name).first()
                if not group: return {"status": "error", "message": f"Avatar group '{group_name}' not found."}

                # Find all parent sessions for this group, regardless of status.
                parent_sessions = self.db_session.query(Session).filter(
                    Session.avatar_group_id == group.id
                ).all()
                parent_session_ids = [ps.id for ps in parent_sessions]

                # Find all their running child sessions.
                child_sessions = self.db_session.query(Session).filter(
                    Session.parent_session_id.in_(parent_session_ids),
                    Session.status == SessionStatus.RUNNING
                ).all()
                
                sessions_to_fail.update(child_sessions)
                
                # Mark the parent group sessions themselves as FAILED.
                for parent in parent_sessions:
                    parent.status = SessionStatus.FAILED

            if avatar_id:
                # Find all sessions where the avatar is either the source or destination.
                direct_sessions = self.db_session.query(Session).filter(
                    Session.status == SessionStatus.RUNNING,
                    or_(
                        Session.avatar_id == avatar_id,
                        Session.destination_avatar_id == avatar_id
                    )
                ).all()
                sessions_to_fail.update(direct_sessions)

            if not sessions_to_fail:
                identifier = f"group '{group_name}'" if group_name else f"avatar ID {avatar_id}"
                return {"status": "success", "message": f"No running sessions found for {identifier}."}

            for session in sessions_to_fail:
                if self._fail_single_session(session.id):
                    failed_count += 1
            
            self.db_session.commit()
            
            identifier = f"group '{group_name}'" if group_name else f"avatar ID {avatar_id}"
            return {"status": "success", "message": f"Set {failed_count} running session(s) for {identifier} to FAILED."}
        
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_fail_all_running_sessions(self, data):
        """Stops all running sessions and marks them as FAILED."""
        try:
            running_sessions = self.db_session.query(Session).filter(Session.status == SessionStatus.RUNNING).all()
            if not running_sessions:
                return {"status": "success", "message": "No running sessions to fail."}

            failed_count = 0
            for session in running_sessions:
                if self._fail_single_session(session.id):
                    failed_count += 1
            
            return {"status": "success", "message": f"Successfully failed {failed_count} running session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    def handle_redo_failed_sessions(self, data):
        try:
            failed_sessions = self.db_session.query(Session).filter(Session.status == SessionStatus.FAILED).all()
            if not failed_sessions:
                return {"status": "success", "message": "No failed sessions found to restart."}

            restarted_count = 0
            for old_session in failed_sessions:
                # Skip group parent sessions, as their children will be restarted individually.
                if old_session.is_group_session:
                    old_session.status = SessionStatus.RESTARTED
                    self.db_session.commit()
                    continue

                new_session = Session(
                    parent_session_id=old_session.parent_session_id,
                    is_group_session=old_session.is_group_session,
                    description=f"[REDO] {old_session.description}",
                    avatar_id=old_session.avatar_id,
                    ic_id=old_session.ic_id,
                    request_id=old_session.request_id,
                    destination_avatar_id=old_session.destination_avatar_id,
                    avatar_group_id=old_session.avatar_group_id,
                    ic_group_id=old_session.ic_group_id,
                    session_type=old_session.session_type,
                    start_time=datetime.datetime.utcnow(),
                    end_time=old_session.end_time,
                    status=SessionStatus.SCHEDULED
                )
                
                self.db_session.add(new_session)
                self.db_session.commit()
                
                self._spawn_worker_for_session(new_session)
                
                old_session.status = SessionStatus.RESTARTED
                self.db_session.commit()
                restarted_count += 1
            
            return {"status": "success", "message": f"Successfully restarted {restarted_count} failed session(s)."}
        except Exception as e:
            self.db_session.rollback()
            return {"status": "error", "message": str(e)}

    # --- Main Loop ---
    def run(self):
        """Main loop to listen for commands and manage workers."""
        # --- Initial Check for Running Sessions ---
        # On startup, find any sessions that were RUNNING and should be restarted.
        # This is a simplified recovery mechanism.
        running_sessions = self.db_session.query(Session).filter(Session.status == SessionStatus.RUNNING).all()
        if running_sessions:
            print(f"Found {len(running_sessions)} sessions marked as RUNNING on startup. Setting them to FAILED for manual restart.")
            for session in running_sessions:
                session.status = SessionStatus.FAILED
            self.db_session.commit()
        
        ACTION_HANDLERS = {
            "ping": lambda data: {"status": "success", "message": "pong"},
            "start_ic": self.handle_start_ic,
            "start_request": self.handle_start_request,
            "start_link": self.handle_start_link,
            "start_group": self.handle_start_group,
            "stop_session": self.handle_stop_session,
            "view_running_on": self.handle_view_running_on,
            "add_member_to_group": self.handle_add_member_to_group,
            "remove_member_from_group": self.handle_remove_member_from_group,
            "remove_entity": self.handle_remove_entity,
            "remove_group": self.handle_remove_group,
            "redo_failed": self.handle_redo_failed_sessions,
            "update_entity": self.handle_update_entity,
            "fail_sessions_on_target": self.handle_fail_sessions_on_target,
            "fail_all_running": self.handle_fail_all_running_sessions
        }

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()
            print(f"Healer Daemon listening on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                with conn:
                    data = conn.recv(16384)
                    if not data: continue
                    try:
                        command = json.loads(data.decode('utf-8'))
                        action = command.get('action')
                        data_payload = command.get('data')
                        response = {}

                        handler = ACTION_HANDLERS.get(action)
                        if handler and callable(handler):
                            response = handler(data_payload)
                        else:
                            response = {"status": "error", "message": f"Unknown command: {action}"}

                        conn.sendall(json.dumps(response).encode('utf-8'))
                    except Exception as e:
                        print(f"Error processing command: {e}")
                        self.db_session.rollback()
                        conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))

if __name__ == "__main__":
    print("--- Initializing Quantum Healer Daemon ---")
    daemon = HealerDaemon(DAEMON_HOST, DAEMON_PORT)
    try:
        daemon.run()
    except KeyboardInterrupt:
        print("\nDaemon shutting down.")
    finally:
        daemon.db_session.close()
