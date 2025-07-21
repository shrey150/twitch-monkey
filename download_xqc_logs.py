import requests
from datetime import datetime, timedelta
import concurrent.futures
from dateutil.relativedelta import relativedelta
import multiprocessing
import threading
import time
from loguru import logger
from tqdm import tqdm
import json
from models import (
    ChatMessage, Channel, create_database, insert_messages_batch, 
    get_cursor, update_cursor, get_sync_start_date, get_or_create_channel
)

# Set the parameters
channel_name = "xqc"  # Channel name (can be changed to support other streamers)
channel_type = "channel"  # Could be "channelid" if needed
EARLIEST_DATE = "2019-04-23T00:00:00Z"  # Updated to include time
start_date = None  # Will be determined by cursor
end_date = datetime.now().strftime("%Y-%m-%d")
ndjson_format = True  # Change to True if you want newline-delimited JSON format
verbose = True  # Set to True for real-time streaming feedback
timeout_seconds = 30  # Timeout for each request
force_full_sync = False  # Set to True to ignore cursor and do full sync

# Initialize database
logger.info("Setting up Supabase database connection...")
engine, Session = create_database()  # Now uses environment variables
logger.success("Database connected!")

# Get or create channel record
session = Session()
channel = get_or_create_channel(session, channel_name, channel_type, f"{channel_name.upper()} Twitch Channel")
channel_id = channel.id
logger.info(f"Using channel: {channel.name} (ID: {channel_id})")

# Check cursor and determine start date
cursor = get_cursor(session, channel_id)

if cursor and not force_full_sync:
    logger.info(f"Found existing cursor for channel '{channel_name}'")
    logger.info(f"Last indexed: {cursor.last_indexed_timestamp}")
    logger.info(f"Total messages indexed: {cursor.total_messages_indexed:,}")
    logger.info(f"Last sync: {cursor.last_sync}")
    
    # Determine start date from cursor (with 1 hour overlap for safety)
    start_datetime = get_sync_start_date(session, channel_id, EARLIEST_DATE)
    logger.info(f"Resuming sync from: {start_datetime}")
else:
    if force_full_sync:
        logger.warning("Force full sync enabled - ignoring existing cursor")
    else:
        logger.info(f"No cursor found for channel '{channel_name}' - starting full sync")
    
    # Start from earliest date
    start_datetime = datetime.fromisoformat(EARLIEST_DATE.replace('Z', '+00:00'))
    logger.info(f"Starting full sync from: {start_datetime}")

session.close()

# Convert end date to datetime
end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

# Progress tracking
completed_count = 0
total_months = 0
progress_lock = threading.Lock()

# Create the date range
start_month = start_datetime.replace(day=1)
end_month = end_datetime.replace(day=1)
months = [start_month + relativedelta(months=i) for i in range((end_month.year - start_month.year) * 12 + end_month.month - start_month.month + 1)]
total_months = len(months)

logger.info(f"Starting {'incremental' if cursor and not force_full_sync else 'full'} sync for channel '{channel_name}'")
logger.info(f"Date range: {start_datetime.strftime('%Y-%m-%d')} to {end_date}")
logger.info(f"Total months to process: {total_months}")
logger.info(f"Using {min(4, total_months)} concurrent workers")

# Define the URL and parameters for the request
url = f"https://logs.ivr.fi/{channel_type}/{channel_name}"

# Create progress bar
pbar = tqdm(total=total_months, desc="Syncing logs", 
            unit="month", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")

# Create a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    # Define a function to make a request for a specific month
    def fetch_logs(current_datetime):
        global completed_count
        
        # Format the 'from' and 'to' dates for the current month
        from_date = current_datetime.strftime("%Y-%m-%dT00:00:00Z")
        next_month = current_datetime + relativedelta(months=1)
        to_date = (next_month - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")
        
        month_display = current_datetime.strftime("%Y-%m")
        
        params = {
            'from': from_date,
            'to': to_date,
            'ndjson': ndjson_format
        }
        
        start_time = time.time()
        total_bytes = 0
        line_count = 0
        messages_batch = []
        
        try:
            if verbose:
                tqdm.write(f"🔄 Starting {month_display}...")
            
            # Make streaming request with timeout
            response = requests.get(url, params=params, stream=True, timeout=timeout_seconds)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Process response in chunks for memory efficiency
                buffer = ""
                
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=False):
                    if chunk:  # filter out keep-alive chunks
                        chunk_str = chunk.decode('utf-8', errors='ignore')
                        total_bytes += len(chunk)
                        buffer += chunk_str
                        
                        # Process complete lines
                        lines = buffer.split('\n')
                        buffer = lines[-1]  # Keep incomplete line in buffer
                        
                        for line in lines[:-1]:  # Process all complete lines
                            if line.strip():
                                message = ChatMessage.from_json_line(line, channel_id)
                                if message:
                                    messages_batch.append(message)
                                    line_count += 1
                        
                        if verbose and total_bytes > 0 and total_bytes % 100000 < 8192:  # Every ~100KB
                            tqdm.write(f"   📊 {month_display}: {total_bytes:,} bytes, {line_count:,} messages")
                
                # Process any remaining line in buffer
                if buffer.strip():
                    message = ChatMessage.from_json_line(buffer, channel_id)
                    if message:
                        messages_batch.append(message)
                        line_count += 1
                
                # Insert messages to database and get insert stats
                inserted_count = 0
                latest_timestamp = None
                
                if messages_batch:
                    session = Session()
                    try:
                        inserted_count, latest_timestamp = insert_messages_batch(session, messages_batch, channel_id)
                        
                        # Update cursor with latest timestamp from this batch
                        if latest_timestamp:
                            update_cursor(session, channel_id, latest_timestamp, inserted_count)
                        
                        session.close()
                    except Exception as db_error:
                        session.rollback()
                        session.close()
                        raise db_error
                
                request_time = time.time() - start_time
                
                # Update progress bar
                pbar.update(1)
                pbar.set_postfix_str(f"{month_display}: +{inserted_count:,} new messages")
                
                # Final completion message
                if inserted_count > 0:
                    tqdm.write(f"✅ {month_display} completed - {total_bytes:,} bytes, {inserted_count:,}/{len(messages_batch):,} new messages, {request_time:.1f}s")
                else:
                    tqdm.write(f"✅ {month_display} completed - {total_bytes:,} bytes, 0 new messages (all duplicates), {request_time:.1f}s")
                
                with progress_lock:
                    completed_count += 1
                    
            else:
                request_time = time.time() - start_time
                
                with progress_lock:
                    completed_count += 1
                    
                pbar.update(1)
                pbar.set_postfix_str(f"{month_display}: FAILED")
                
                tqdm.write(f"❌ {month_display} failed - Status: {response.status_code}, Time: {request_time:.1f}s")
                if response.text:
                    tqdm.write(f"   Error details: {response.text[:200]}")
                    
        except requests.exceptions.Timeout:
            request_time = time.time() - start_time
            with progress_lock:
                completed_count += 1
                
            pbar.update(1)
            pbar.set_postfix_str(f"{month_display}: TIMEOUT")
            tqdm.write(f"⏰ {month_display} timeout after {timeout_seconds}s")
            
        except Exception as e:
            request_time = time.time() - start_time
            with progress_lock:
                completed_count += 1
                
            pbar.update(1)
            pbar.set_postfix_str(f"{month_display}: ERROR")
            tqdm.write(f"💥 {month_display} exception - {str(e)}, Time: {request_time:.1f}s")
    
    # Use the executor to make requests for all months in parallel
    executor.map(fetch_logs, months)
    
    # Close the progress bar
    pbar.close()

logger.success("🎉 Download completed!")