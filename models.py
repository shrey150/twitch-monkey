from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Index, text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from datetime import datetime
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

Base = declarative_base()

class Channel(Base):
    """Channel information table"""
    __tablename__ = 'channels'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True, index=True)  # e.g., "xqc", "shroud"
    display_name = Column(String(100))  # Human-readable name
    channel_type = Column(String(20), default='channel')  # 'channel' or 'channelid'
    room_id = Column(String(50), index=True)  # Twitch room ID
    created_at = Column(DateTime, default=datetime.utcnow)
    last_activity = Column(DateTime)
    total_messages = Column(Integer, default=0)
    is_active = Column(String(10), default='true')  # 'true'/'false' for compatibility
    
    # Relationships
    messages = relationship("ChatMessage", back_populates="channel")
    sync_cursors = relationship("SyncCursor", back_populates="channel")
    
    def __repr__(self):
        return f"<Channel(name='{self.name}', display_name='{self.display_name}')>"

class SyncCursor(Base):
    """Track the last indexed timestamp for incremental syncing"""
    __tablename__ = 'sync_cursor'
    
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey('channels.id'), nullable=False, index=True)
    last_indexed_timestamp = Column(DateTime, nullable=False)
    last_sync = Column(DateTime, nullable=False, default=datetime.utcnow)
    total_messages_indexed = Column(Integer, default=0)
    
    # Relationships
    channel = relationship("Channel", back_populates="sync_cursors")
    
    # Ensure one cursor per channel
    __table_args__ = (
        Index('idx_channel_cursor', 'channel_id'),
    )
    
    def __repr__(self):
        return f"<SyncCursor(channel_id={self.channel_id}, last_indexed='{self.last_indexed_timestamp}')>"

class ChatMessage(Base):
    __tablename__ = 'chat_messages'
    
    # Primary fields
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, ForeignKey('channels.id'), nullable=False, index=True)
    message_id = Column(String(255), index=True)  # The 'id' from JSON (can be empty)
    text = Column(Text, nullable=False, index=True)
    display_name = Column(String(255), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # User info (extracted from tags for easy querying)
    user_id = Column(String(50), index=True)
    color = Column(String(10))
    subscriber = Column(String(10))
    badges = Column(String(500))
    room_id = Column(String(50), index=True)  # Keep for backward compatibility
    
    # Store all tags as JSONB for flexibility and performance
    tags = Column(JSONB)
    
    # Raw timestamp from Twitch
    tmi_sent_ts = Column(String(20), index=True)
    
    # Text search vector for PostgreSQL full-text search
    search_vector = Column(TSVECTOR)  # Proper PostgreSQL TSVector type
    
    # Relationships
    channel = relationship("Channel", back_populates="messages")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_channel_timestamp', 'channel_id', 'timestamp'),
        Index('idx_channel_user', 'channel_id', 'user_id'),
        Index('idx_channel_display_name', 'channel_id', 'display_name'),
        Index('idx_message_unique', 'channel_id', 'message_id', 'timestamp'),
        # Full-text search index using GIN on TSVector
        Index('idx_chat_search', 'search_vector', postgresql_using='gin'),
    )
    
    @classmethod
    def from_json_line(cls, json_line, channel_id):
        """Parse a JSON line from the logs and create a ChatMessage instance"""
        try:
            data = json.loads(json_line.strip())
            
            # Parse timestamp
            timestamp_str = data.get('timestamp', '')
            timestamp = None
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            # Extract key fields from tags
            tags = data.get('tags', {})
            
            return cls(
                channel_id=channel_id,
                message_id=data.get('id', ''),
                text=data.get('text', ''),
                display_name=data.get('displayName', ''),
                timestamp=timestamp,
                user_id=tags.get('user-id', ''),
                color=tags.get('color', ''),
                subscriber=tags.get('subscriber', ''),
                badges=tags.get('badges', ''),
                room_id=tags.get('room-id', ''),
                tags=tags,
                tmi_sent_ts=tags.get('tmi-sent-ts', '')
            )
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print(f"Error parsing JSON line: {e}")
            return None
    
    def __repr__(self):
        return f"<ChatMessage(display_name='{self.display_name}', text='{self.text[:50]}...', timestamp='{self.timestamp}')>"

# Channel and cursor management functions
def get_or_create_channel(session, channel_name, channel_type='channel', display_name=None):
    """Get existing channel or create new one"""
    channel = session.query(Channel).filter_by(name=channel_name).first()
    
    if not channel:
        channel = Channel(
            name=channel_name,
            display_name=display_name or channel_name.upper(),
            channel_type=channel_type,
            created_at=datetime.utcnow()
        )
        session.add(channel)
        session.commit()
        session.refresh(channel)
    
    return channel

def get_cursor(session, channel_id):
    """Get the current sync cursor for a channel"""
    cursor = session.query(SyncCursor).filter_by(channel_id=channel_id).first()
    return cursor

def update_cursor(session, channel_id, last_timestamp, messages_added=0):
    """Update or create the sync cursor"""
    cursor = get_cursor(session, channel_id)
    
    if cursor:
        # Only update if this timestamp is newer
        if last_timestamp > cursor.last_indexed_timestamp:
            cursor.last_indexed_timestamp = last_timestamp
            cursor.last_sync = datetime.utcnow()
            cursor.total_messages_indexed += messages_added
    else:
        cursor = SyncCursor(
            channel_id=channel_id,
            last_indexed_timestamp=last_timestamp,
            last_sync=datetime.utcnow(),
            total_messages_indexed=messages_added
        )
        session.add(cursor)
    
    # Also update channel activity
    channel = session.query(Channel).get(channel_id)
    if channel:
        channel.last_activity = datetime.utcnow()
        channel.total_messages += messages_added
    
    session.commit()
    return cursor

def get_sync_start_date(session, channel_id, earliest_possible_date):
    """Determine the start date for syncing based on cursor"""
    cursor = get_cursor(session, channel_id)
    
    if cursor:
        # Start from 1 hour before last indexed to handle any potential gaps
        from datetime import timedelta
        start_from = cursor.last_indexed_timestamp - timedelta(hours=1)
        return max(start_from, datetime.fromisoformat(earliest_possible_date.replace('Z', '+00:00')))
    else:
        # No cursor exists, start from the earliest possible date
        return datetime.fromisoformat(earliest_possible_date.replace('Z', '+00:00'))

def list_channels(session):
    """List all channels with their stats"""
    channels = session.query(Channel).order_by(Channel.name).all()
    return channels

# Database setup functions
def create_database(db_url=None):
    """Create database connection and tables"""
    if not db_url:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is required")
    
    # Create engine with connection pooling
    engine = create_engine(
        db_url,
        echo=False,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,  # Verify connections before use
        pool_recycle=300     # Recycle connections every 5 minutes
    )
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    # Set up PostgreSQL full-text search
    with engine.connect() as conn:
        # Create or update search vector trigger function
        conn.execute(text("""
            CREATE OR REPLACE FUNCTION update_search_vector()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.search_vector := setweight(to_tsvector('english', COALESCE(NEW.text, '')), 'A') ||
                                   setweight(to_tsvector('english', COALESCE(NEW.display_name, '')), 'B');
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """))
        
        # Create trigger to automatically update search vector
        conn.execute(text("""
            DROP TRIGGER IF EXISTS chat_messages_search_vector_update ON chat_messages;
            CREATE TRIGGER chat_messages_search_vector_update
                BEFORE INSERT OR UPDATE ON chat_messages
                FOR EACH ROW EXECUTE FUNCTION update_search_vector();
        """))
        
        # Update existing records (if any)
        conn.execute(text("""
            UPDATE chat_messages SET search_vector = 
                setweight(to_tsvector('english', COALESCE(text, '')), 'A') ||
                setweight(to_tsvector('english', COALESCE(display_name, '')), 'B')
            WHERE search_vector IS NULL;
        """))
        
        conn.commit()
    
    Session = sessionmaker(bind=engine)
    return engine, Session

def search_messages(session, query, channel_id=None, limit=100):
    """Search messages using PostgreSQL full-text search"""
    # Clean and prepare search query
    # Replace special characters and prepare for PostgreSQL tsquery
    clean_query = query.replace("'", "''").replace("&", "").replace("|", "").replace("!", "")
    
    if channel_id:
        result = session.execute(text("""
            SELECT cm.* FROM chat_messages cm
            WHERE cm.channel_id = :channel_id 
            AND cm.search_vector @@ plainto_tsquery('english', :query)
            ORDER BY ts_rank(cm.search_vector, plainto_tsquery('english', :query)) DESC, 
                     cm.timestamp DESC
            LIMIT :limit
        """), {"query": clean_query, "channel_id": channel_id, "limit": limit})
    else:
        result = session.execute(text("""
            SELECT cm.* FROM chat_messages cm
            WHERE cm.search_vector @@ plainto_tsquery('english', :query)
            ORDER BY ts_rank(cm.search_vector, plainto_tsquery('english', :query)) DESC,
                     cm.timestamp DESC
            LIMIT :limit
        """), {"query": clean_query, "limit": limit})
    
    return result.fetchall()

def insert_messages_batch(session, messages, channel_id, batch_size=1000):
    """Insert messages in batches for better performance"""
    if not messages:
        return 0, None
        
    # Sort messages by timestamp to maintain order
    messages.sort(key=lambda x: x.timestamp if x.timestamp else datetime.min)
    
    inserted_count = 0
    latest_timestamp = None
    
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        
        # Use PostgreSQL UPSERT (ON CONFLICT) for efficient duplicate handling
        for message in batch:
            # Check if message already exists (by channel, message_id and timestamp)
            existing = None
            if message.message_id:
                existing = session.query(ChatMessage).filter_by(
                    channel_id=channel_id,
                    message_id=message.message_id,
                    timestamp=message.timestamp
                ).first()
            else:
                # For messages without IDs, check by timestamp and text to avoid duplicates
                existing = session.query(ChatMessage).filter_by(
                    channel_id=channel_id,
                    timestamp=message.timestamp,
                    text=message.text,
                    display_name=message.display_name
                ).first()
            
            if not existing:
                message.channel_id = channel_id  # Ensure channel_id is set
                session.add(message)
                inserted_count += 1
                if message.timestamp:
                    latest_timestamp = max(latest_timestamp or message.timestamp, message.timestamp)
        
        # Commit batch
        session.commit()
    
    return inserted_count, latest_timestamp 