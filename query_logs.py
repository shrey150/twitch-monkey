#!/usr/bin/env python3
"""
Query script for XQC chat logs database with multi-channel support
"""

from models import ChatMessage, Channel, SyncCursor, create_database, search_messages, get_cursor, list_channels
from sqlalchemy import func, desc
from datetime import datetime, timedelta
import sys

def setup_db():
    """Setup database connection"""
    engine, Session = create_database()  # Now uses environment variables
    return Session()

def stats(session, channel_name=None):
    """Show database statistics, optionally filtered by channel"""
    if channel_name:
        channel = session.query(Channel).filter_by(name=channel_name).first()
        if not channel:
            print(f"❌ Channel '{channel_name}' not found")
            return
            
        # Channel-specific stats
        total_messages = session.query(ChatMessage).filter_by(channel_id=channel.id).count()
        unique_users = session.query(ChatMessage.user_id).filter_by(channel_id=channel.id).distinct().count()
        date_range = session.query(
            func.min(ChatMessage.timestamp), 
            func.max(ChatMessage.timestamp)
        ).filter_by(channel_id=channel.id).first()
        
        print(f"📊 Statistics for {channel.display_name} ({channel.name}):")
        print(f"   Total messages: {total_messages:,}")
        print(f"   Unique users: {unique_users:,}")
        print(f"   Created: {channel.created_at}")
        if channel.last_activity:
            print(f"   Last activity: {channel.last_activity}")
        if date_range[0] and date_range[1]:
            print(f"   Message date range: {date_range[0]} to {date_range[1]}")
    else:
        # Global stats
        total_messages = session.query(ChatMessage).count()
        total_channels = session.query(Channel).count()
        active_channels = session.query(Channel).filter(Channel.total_messages > 0).count()
        date_range = session.query(
            func.min(ChatMessage.timestamp), 
            func.max(ChatMessage.timestamp)
        ).first()
        
        print("📊 Global Database Statistics:")
        print(f"   Total messages: {total_messages:,}")
        print(f"   Total channels: {total_channels:,}")
        print(f"   Active channels: {active_channels:,}")
        if date_range[0] and date_range[1]:
            print(f"   Date range: {date_range[0]} to {date_range[1]}")
            
        # Show top channels
        top_channels = session.query(
            Channel.name, 
            Channel.display_name,
            func.count(ChatMessage.id).label('message_count')
        ).join(ChatMessage).group_by(Channel.id)\
         .order_by(desc('message_count')).limit(5).all()
        
        if top_channels:
            print("\n   Top channels:")
            for i, (name, display_name, count) in enumerate(top_channels, 1):
                print(f"     {i}. {display_name} ({name}): {count:,} messages")
    print()

def list_channels_cmd(session):
    """List all channels"""
    channels = list_channels(session)
    
    print("📺 Available Channels:")
    if channels:
        for channel in channels:
            status = "🟢" if channel.total_messages > 0 else "🔴"
            last_activity = channel.last_activity.strftime("%Y-%m-%d %H:%M") if channel.last_activity else "Never"
            print(f"   {status} {channel.display_name} ({channel.name})")
            print(f"      Messages: {channel.total_messages:,} | Last activity: {last_activity}")
    else:
        print("   No channels found")
    print()

def cursor_status(session, channel_name=None):
    """Show sync cursor status"""
    if channel_name:
        channel = session.query(Channel).filter_by(name=channel_name).first()
        if not channel:
            print(f"❌ Channel '{channel_name}' not found")
            return
        cursors = [get_cursor(session, channel.id)]
        cursors = [c for c in cursors if c]  # Remove None values
    else:
        cursors = session.query(SyncCursor).join(Channel).all()
    
    print("🔄 Sync Status:")
    if cursors:
        for cursor in cursors:
            channel = session.query(Channel).get(cursor.channel_id)
            print(f"   Channel: {channel.display_name} ({channel.name})")
            print(f"   Last indexed: {cursor.last_indexed_timestamp}")
            print(f"   Messages indexed: {cursor.total_messages_indexed:,}")
            print(f"   Last sync: {cursor.last_sync}")
            
            # Calculate time since last sync
            time_since = datetime.utcnow() - cursor.last_sync
            if time_since.days > 0:
                print(f"   Sync age: {time_since.days} days ago")
            elif time_since.seconds > 3600:
                hours = time_since.seconds // 3600
                print(f"   Sync age: {hours} hours ago")
            else:
                minutes = time_since.seconds // 60
                print(f"   Sync age: {minutes} minutes ago")
            print()
    else:
        print("   No sync cursors found - database is empty or was created before cursor support")
    print()

def search_text(session, query, channel_name=None, limit=20):
    """Search messages using full-text search"""
    channel_id = None
    if channel_name:
        channel = session.query(Channel).filter_by(name=channel_name).first()
        if not channel:
            print(f"❌ Channel '{channel_name}' not found")
            return
        channel_id = channel.id
        print(f"🔍 Searching '{query}' in {channel.display_name}:")
    else:
        print(f"🔍 Searching '{query}' across all channels:")
        
    try:
        results = search_messages(session, query, channel_id, limit)
        if results:
            for row in results:
                # Get channel info for display
                channel = session.query(Channel).get(row.channel_id)
                channel_display = f"[{channel.name}]" if not channel_name else ""
                timestamp = row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "Unknown"
                print(f"   {channel_display}[{timestamp}] {row.display_name}: {row.text}")
        else:
            print("   No results found")
    except Exception as e:
        print(f"   Error searching: {e}")
    print()

def top_chatters(session, channel_name=None, limit=10):
    """Show most active chatters"""
    query = session.query(
        ChatMessage.display_name,
        func.count(ChatMessage.id).label('message_count')
    )
    
    if channel_name:
        channel = session.query(Channel).filter_by(name=channel_name).first()
        if not channel:
            print(f"❌ Channel '{channel_name}' not found")
            return
        query = query.filter_by(channel_id=channel.id)
        print(f"💬 Top {limit} chatters in {channel.display_name}:")
    else:
        print(f"💬 Top {limit} chatters across all channels:")
    
    results = query.group_by(ChatMessage.display_name)\
        .order_by(desc('message_count'))\
        .limit(limit).all()
    
    for i, (username, count) in enumerate(results, 1):
        print(f"   {i:2d}. {username}: {count:,} messages")
    print()

def recent_messages(session, channel_name=None, hours=1, limit=20):
    """Show recent messages"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    
    query = session.query(ChatMessage).filter(ChatMessage.timestamp > cutoff)
    
    if channel_name:
        channel = session.query(Channel).filter_by(name=channel_name).first()
        if not channel:
            print(f"❌ Channel '{channel_name}' not found")
            return
        query = query.filter_by(channel_id=channel.id)
        print(f"⏰ Recent messages from {channel.display_name} (last {hours} hour(s)):")
    else:
        print(f"⏰ Recent messages from all channels (last {hours} hour(s)):")
    
    results = query.order_by(desc(ChatMessage.timestamp)).limit(limit).all()
    
    for msg in results:
        channel = session.query(Channel).get(msg.channel_id)
        channel_display = f"[{channel.name}]" if not channel_name else ""
        timestamp = msg.timestamp.strftime("%H:%M:%S") if msg.timestamp else "Unknown"
        print(f"   {channel_display}[{timestamp}] {msg.display_name}: {msg.text}")
    print()

def messages_by_user(session, username, channel_name=None, limit=10):
    """Show messages from a specific user"""
    query = session.query(ChatMessage).filter(ChatMessage.display_name.ilike(f"%{username}%"))
    
    if channel_name:
        channel = session.query(Channel).filter_by(name=channel_name).first()
        if not channel:
            print(f"❌ Channel '{channel_name}' not found")
            return
        query = query.filter_by(channel_id=channel.id)
        print(f"👤 Recent messages from {username} in {channel.display_name}:")
    else:
        print(f"👤 Recent messages from {username} across all channels:")
    
    results = query.order_by(desc(ChatMessage.timestamp)).limit(limit).all()
    
    for msg in results:
        channel = session.query(Channel).get(msg.channel_id)
        channel_display = f"[{channel.name}]" if not channel_name else ""
        timestamp = msg.timestamp.strftime("%Y-%m-%d %H:%M:%S") if msg.timestamp else "Unknown"
        print(f"   {channel_display}[{timestamp}] {msg.display_name}: {msg.text}")
    print()

def main():
    if len(sys.argv) < 2:
        print("Usage: poetry run python3 query_logs.py <command> [args...]")
        print("\nCommands:")
        print("  stats [channel]           - Show database statistics (optionally for specific channel)")
        print("  channels                  - List all available channels")
        print("  cursor [channel]          - Show sync cursor status (optionally for specific channel)")
        print("  search 'query' [channel]  - Full-text search messages (optionally in specific channel)")
        print("  top [N] [channel]         - Show top N chatters (optionally in specific channel)")
        print("  recent [hours] [limit] [channel] - Show recent messages")
        print("  user 'username' [channel] [limit] - Show messages from specific user")
        print("\nExamples:")
        print("  poetry run python3 query_logs.py stats")
        print("  poetry run python3 query_logs.py stats xqc")
        print("  poetry run python3 query_logs.py channels")
        print("  poetry run python3 query_logs.py search 'OMEGALUL' xqc")
        print("  poetry run python3 query_logs.py top 20")
        print("  poetry run python3 query_logs.py user 'xqc' xqc 5")
        return

    session = setup_db()
    command = sys.argv[1].lower()

    try:
        if command == "stats":
            channel_name = sys.argv[2] if len(sys.argv) > 2 else None
            stats(session, channel_name)
        
        elif command == "channels":
            list_channels_cmd(session)
            
        elif command == "cursor":
            channel_name = sys.argv[2] if len(sys.argv) > 2 else None
            cursor_status(session, channel_name)
        
        elif command == "search":
            if len(sys.argv) < 3:
                print("Please provide a search query")
                return
            query = sys.argv[2]
            channel_name = sys.argv[3] if len(sys.argv) > 3 else None
            search_text(session, query, channel_name)
        
        elif command == "top":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 10
            channel_name = sys.argv[3] if len(sys.argv) > 3 else (sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].isdigit() else None)
            top_chatters(session, channel_name, limit)
        
        elif command == "recent":
            hours = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1
            limit = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 20
            channel_name = None
            # Try to find channel name in args
            for arg in sys.argv[2:]:
                if not arg.isdigit():
                    channel_name = arg
                    break
            recent_messages(session, channel_name, hours, limit)
        
        elif command == "user":
            if len(sys.argv) < 3:
                print("Please provide a username")
                return
            username = sys.argv[2]
            channel_name = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].isdigit() else None
            limit = int(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4].isdigit() else (int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 10)
            messages_by_user(session, username, channel_name, limit)
        
        else:
            print(f"Unknown command: {command}")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        session.close()

if __name__ == "__main__":
    main() 