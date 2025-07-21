# 🐒 Twitch Monkey - Multi-Channel Chat Log Analyzer

A production-ready system for downloading, storing, and analyzing Twitch chat logs with **Supabase PostgreSQL** backend and full-text search capabilities.

## 🚀 **Quick Start for Cursor Agents**

### **Environment Setup**
```bash
# Install dependencies
poetry install
```

### **Core Commands**
```bash
# Download XQC chat logs (incremental sync with cursor)
poetry run python3 download_xqc_logs.py

# Query database statistics
poetry run python3 query_logs.py stats

# Search messages with PostgreSQL full-text search
poetry run python3 query_logs.py search "OMEGALUL" xqc

# List available channels
poetry run python3 query_logs.py channels

# View sync status and cursors
poetry run python3 query_logs.py cursor

# Top chatters analysis
poetry run python3 query_logs.py top 20 xqc
```

## 🏗️ **Architecture**

### **Database Models**
- **`Channel`** - Multi-streamer support (xqc, shroud, etc.)
- **`ChatMessage`** - Messages with JSONB tags and TSVector search
- **`SyncCursor`** - Incremental sync tracking per channel

### **Key Features**
- ✅ **PostgreSQL + Supabase** - Cloud-hosted, scalable
- ✅ **Multi-Channel Support** - Track multiple streamers
- ✅ **Incremental Syncing** - Smart cursor system to avoid re-downloading
- ✅ **Full-Text Search** - PostgreSQL TSVector with ranking
- ✅ **JSONB Storage** - Flexible tag storage and querying
- ✅ **Connection Pooling** - Production-ready reliability
- ✅ **Environment Security** - Credentials in .env (gitignored)

## 📊 **Usage Examples**

### **Add New Channels**
Edit `download_xqc_logs.py`:
```python
channel_name = "shroud"  # Change to any Twitch channel
```

### **Search & Analytics**  
```bash
# Channel-specific search
poetry run python3 query_logs.py search "poggers" xqc

# Recent messages from specific user
poetry run python3 query_logs.py user "xqc" xqc 10

# Recent activity across all channels
poetry run python3 query_logs.py recent 24 50
```

### **Database Access**
- **Local**: Through Python scripts
- **Web**: Supabase dashboard at supabase.com
- **SQL**: Direct PostgreSQL queries via psql or any client

## 🔧 **Development Tasks**

### **Potential Improvements**
- [ ] Add real-time streaming with WebSocket
- [ ] Implement sentiment analysis pipeline
- [ ] Add Discord bot integration
- [ ] Create web dashboard with analytics
- [ ] Add emote frequency tracking
- [ ] Implement user clustering analysis
- [ ] Add export functionality (CSV, JSON)
- [ ] Set up automated daily sync jobs

### **Performance Optimizations**
- [ ] Add database partitioning by date
- [ ] Implement Redis caching layer
- [ ] Add bulk insert optimizations
- [ ] Create materialized views for analytics
- [ ] Add database connection retry logic

### **Monitoring & Ops**  
- [ ] Add logging with structured JSON
- [ ] Implement health checks
- [ ] Add Prometheus metrics
- [ ] Create alerting for failed syncs
- [ ] Add data validation rules

## 🛠️ **Tech Stack**
- **Backend**: Python 3.13, SQLAlchemy, psycopg2
- **Database**: Supabase (PostgreSQL) with FTS5, JSONB, GIN indexes
- **Concurrency**: ThreadPoolExecutor for parallel downloads
- **Progress**: tqdm for real-time progress bars
- **Logging**: loguru with structured output
- **Config**: python-dotenv for environment management

## 🔒 **Security Notes**
- Database credentials stored in `.env` (gitignored)
- Uses Supabase Session Pooler for IPv4 compatibility
- Connection pooling with automatic retry logic
- SQL injection protection via SQLAlchemy

## 📝 **File Structure**
```
├── models.py           # SQLAlchemy ORM models + database functions
├── download_xqc_logs.py # Main sync script with cursor system  
├── query_logs.py       # CLI for database queries and analytics
├── pyproject.toml      # Poetry dependencies
├── .env               # Supabase credentials (create manually)
└── archive/           # Legacy files from SQLite era
```

---
**Ready for Cursor Agents development!** 🎯 The codebase is production-ready with proper error handling, logging, and PostgreSQL best practices. 