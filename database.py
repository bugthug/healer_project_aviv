# database.py
import datetime
import enum
from sqlalchemy import (create_engine, Column, Integer, String, Text,
                        LargeBinary, DateTime, ForeignKey, Boolean)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.dialects.postgresql import ENUM as PGEnum

from config import DATABASE_URL

class SessionType(enum.Enum):
    IC_SESSION = "ic_session"
    REQUEST_SESSION = "request_session"
    AVATAR_LINK = "avatar_link"
    GROUP_IC_SESSION = "group_ic_session" # New type for parent group sessions

class SessionStatus(enum.Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"
    RESTARTED = "restarted"

Base = declarative_base()

# Removed SessionBatch Class

class AvatarGroup(Base):
    __tablename__ = 'avatar_groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    members = relationship("AvatarGroupMember", back_populates="group", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="avatar_group") # Added relationship

class AvatarGroupMember(Base):
    __tablename__ = 'avatar_group_members'
    group_id = Column(Integer, ForeignKey('avatar_groups.id'), primary_key=True)
    avatar_id = Column(Integer, ForeignKey('avatars.id'), primary_key=True)
    
    group = relationship("AvatarGroup", back_populates="members")
    avatar = relationship("Avatar", back_populates="groups")

class ICGroup(Base):
    __tablename__ = 'ic_groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    members = relationship("ICGroupMember", back_populates="group", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="ic_group") # Added relationship

class ICGroupMember(Base):
    __tablename__ = 'ic_group_members'
    group_id = Column(Integer, ForeignKey('ic_groups.id'), primary_key=True)
    ic_id = Column(Integer, ForeignKey('information_copies.id'), primary_key=True)
    group = relationship("ICGroup", back_populates="members")
    ic = relationship("InformationCopy", back_populates="groups")

class RequestGroup(Base):
    __tablename__ = 'request_groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    members = relationship("RequestGroupMember", back_populates="group", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="request_group")

class RequestGroupMember(Base):
    __tablename__ = 'request_group_members'
    group_id = Column(Integer, ForeignKey('request_groups.id'), primary_key=True)
    request_id = Column(Integer, ForeignKey('requests.id'), primary_key=True)
    group = relationship("RequestGroup", back_populates="members")
    request = relationship("Request", back_populates="groups")

class Avatar(Base):
    __tablename__ = 'avatars'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    photo_data = Column(LargeBinary, nullable=False)
    info_data = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    source_sessions = relationship("Session", foreign_keys="Session.avatar_id", back_populates="source_avatar", cascade="all, delete-orphan")
    dest_sessions = relationship("Session", foreign_keys="Session.destination_avatar_id", back_populates="dest_avatar", cascade="all, delete-orphan")
    groups = relationship("AvatarGroupMember", back_populates="avatar", cascade="all, delete-orphan")

class InformationCopy(Base):
    __tablename__ = 'information_copies'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    wav_data = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    sessions = relationship("Session", back_populates="ic", cascade="all, delete-orphan")
    groups = relationship("ICGroupMember", back_populates="ic", cascade="all, delete-orphan")

class Request(Base):
    __tablename__ = 'requests'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    request_data = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    sessions = relationship("Session", back_populates="request", cascade="all, delete-orphan")
    groups = relationship("RequestGroupMember", back_populates="request", cascade="all, delete-orphan")

class Session(Base):
    __tablename__ = 'sessions'
    id = Column(Integer, primary_key=True)
    
    # --- New Columns for Hierarchy and Grouping ---
    parent_session_id = Column(Integer, ForeignKey('sessions.id'), nullable=True)
    is_group_session = Column(Boolean, default=False, nullable=False)
    description = Column(Text, nullable=True) # Description is more important now

    # --- Foreign Keys for linking to all possible entities ---
    avatar_id = Column(Integer, ForeignKey('avatars.id'), nullable=True) # Nullable for parent group sessions
    ic_id = Column(Integer, ForeignKey('information_copies.id'), nullable=True)
    request_id = Column(Integer, ForeignKey('requests.id'), nullable=True)
    destination_avatar_id = Column(Integer, ForeignKey('avatars.id'), nullable=True)
    avatar_group_id = Column(Integer, ForeignKey('avatar_groups.id'), nullable=True)
    ic_group_id = Column(Integer, ForeignKey('ic_groups.id'), nullable=True)
    request_group_id = Column(Integer, ForeignKey('request_groups.id'), nullable=True)
    
    # --- Core Session Details ---
    session_type = Column(PGEnum(SessionType, name='sessiontype'), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True) 
    status = Column(PGEnum(SessionStatus, name='sessionstatus'), nullable=False, default=SessionStatus.SCHEDULED)
    worker_pid = Column(Integer)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    # --- Relationships ---
    source_avatar = relationship("Avatar", foreign_keys=[avatar_id], back_populates="source_sessions")
    dest_avatar = relationship("Avatar", foreign_keys=[destination_avatar_id], back_populates="dest_sessions")
    ic = relationship("InformationCopy", back_populates="sessions")
    request = relationship("Request", back_populates="sessions")
    
    # New relationships for groups
    avatar_group = relationship("AvatarGroup", back_populates="sessions")
    ic_group = relationship("ICGroup", back_populates="sessions")
    request_group = relationship("RequestGroup", back_populates="sessions")

    # Relationships for parent/child sessions
    parent_session = relationship("Session", remote_side=[id], back_populates="child_sessions")
    child_sessions = relationship("Session", back_populates="parent_session", cascade="all, delete-orphan")


def get_engine():
    """Creates a SQLAlchemy engine."""
    return create_engine(DATABASE_URL)

def setup_database():
    """
    Initializes the database. THIS IS A DESTRUCTIVE OPERATION for development.
    It drops all existing tables and types to ensure a clean schema.
    """
    engine = get_engine()
    url = make_url(engine.url)
    if not database_exists(url):
        print(f"Database {url.database} not found, creating it...")
        create_database(url)
    
    print("Resetting database schema...")
    # Drop all tables, which also drops associated FK constraints
    Base.metadata.drop_all(engine)
    # Drop the custom ENUM types separately
    with engine.connect() as conn:
        # Use a transaction block
        with conn.begin():
            PGEnum(SessionType, name='sessiontype').drop(conn, checkfirst=True)
            PGEnum(SessionStatus, name='sessionstatus').drop(conn, checkfirst=True)
    
    print("Creating all tables and types...")
    Base.metadata.create_all(engine)
    print("Database setup complete.")


def get_session_factory():
    """Returns the session factory."""
    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal
