import redis
import json
import os
from typing import Optional, List
from datetime import datetime

# Configuration
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
CACHE_TTL = int(os.environ.get('EMAILS_JSON_CACHE_TTL', 1800))  # 30 minutes default

# Create Redis client
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    redis_client.ping()
    print(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"⚠ Redis connection failed: {e}. Caching disabled.")
    redis_client = None


def _get_cache_key(hashed_email: str) -> str:
    """Get cache key for emails JSON"""
    return f"emails_json:{hashed_email}"


def _get_dirty_flag_key(hashed_email: str) -> str:
    """Get dirty flag key to track if cache needs S3 sync"""
    return f"emails_json_dirty:{hashed_email}"


def get_cached_emails_json(hashed_email: str) -> Optional[List[dict]]:
    """
    Get emails JSON from Redis cache.
    Returns None if cache miss or Redis unavailable.
    """
    if not redis_client:
        return None
    
    key = _get_cache_key(hashed_email)
    try:
        cached = redis_client.get(key)
        if cached:
            # Reset TTL on read (activity-based expiration)
            redis_client.expire(key, CACHE_TTL)
            # Also extend dirty flag TTL if it exists
            dirty_key = _get_dirty_flag_key(hashed_email)
            redis_client.expire(dirty_key, CACHE_TTL)
            
            return json.loads(cached)
    except Exception as e:
        print(f"Redis get error: {e}")
    
    return None


def set_cached_emails_json(hashed_email: str, documents: List[dict], mark_dirty: bool = False) -> bool:
    """
    Set emails JSON in Redis cache with TTL.
    
    :param hashed_email: Hashed email identifier
    :param documents: List of document dictionaries
    :param mark_dirty: If True, marks cache as dirty (needs S3 sync on expiry)
    :return: True if successful
    """
    if not redis_client:
        return False
    
    key = _get_cache_key(hashed_email)
    try:
        # Store the data
        redis_client.setex(key, CACHE_TTL, json.dumps(documents))
        
        # If this is a write (not just a load), mark as dirty
        if mark_dirty:
            dirty_key = _get_dirty_flag_key(hashed_email)
            # Store the dirty flag with same TTL
            redis_client.setex(dirty_key, CACHE_TTL, json.dumps(documents))
        
        return True
    except Exception as e:
        print(f"Redis set error: {e}")
        return False


def get_or_load_emails_json(hashed_email: str, s3_path: str) -> List[dict]:
    """
    Cache-aside pattern: Check Redis first, fallback to S3.
    Only loads from S3 on cache miss.
    """
    # Try cache first
    documents = get_cached_emails_json(hashed_email)
    if documents is not None:
        print(f"✓ Cache HIT for {hashed_email}")
        return documents
    
    # Cache miss - load from S3
    print(f"✗ Cache MISS for {hashed_email}, loading from S3...")
    from S3_utils import get_json_file
    documents = get_json_file(hashed_email, s3_path)
    
    # Store in cache (not dirty since it's fresh from S3)
    set_cached_emails_json(hashed_email, documents, mark_dirty=False)
    
    return documents


def save_emails_json_to_cache(hashed_email: str, documents: List[dict]) -> None:
    """
    Write-back pattern: Update cache only, defer S3 write.
    S3 will be updated when cache expires (via background job or next load).
    
    Use this for edits, uploads, and deletes.
    """
    # Update cache and mark as dirty
    set_cached_emails_json(hashed_email, documents, mark_dirty=True)
    print(f"✓ Updated cache for {hashed_email} (S3 sync deferred)")


def flush_to_s3_if_dirty(hashed_email: str, s3_path: str) -> bool:
    """
    Check if cache is dirty and flush to S3 if needed.
    Call this periodically or on cache expiry.
    
    :return: True if flushed, False if not dirty or failed
    """
    if not redis_client:
        return False
    
    dirty_key = _get_dirty_flag_key(hashed_email)
    
    try:
        # Check if dirty flag exists
        dirty_data = redis_client.get(dirty_key)
        if not dirty_data:
            # Not dirty, no need to flush
            return False
        
        # Dirty - need to flush to S3
        documents = json.loads(dirty_data)
        
        from S3_utils import save_json_file
        save_json_file(hashed_email, s3_path, documents)
        
        # Clear dirty flag after successful flush
        redis_client.delete(dirty_key)
        
        print(f"✓ Flushed dirty cache for {hashed_email} to S3")
        return True
        
    except Exception as e:
        print(f"Error flushing to S3: {e}")
        return False


def force_sync_to_s3(hashed_email: str, s3_path: str) -> bool:
    """
    Force immediate sync to S3, regardless of dirty flag.
    Use this for critical operations or manual sync.
    """
    if not redis_client:
        return False
    
    try:
        # Get current cache data
        documents = get_cached_emails_json(hashed_email)
        if documents is None:
            print(f"No cache data for {hashed_email}")
            return False
        
        # Write to S3
        from S3_utils import save_json_file
        save_json_file(hashed_email, s3_path, documents)
        
        # Clear dirty flag
        dirty_key = _get_dirty_flag_key(hashed_email)
        redis_client.delete(dirty_key)
        
        print(f"✓ Force synced {hashed_email} to S3")
        return True
        
    except Exception as e:
        print(f"Error force syncing to S3: {e}")
        return False


def invalidate_emails_json(hashed_email: str) -> bool:
    """
    Invalidate cache and clear dirty flag.
    Forces reload from S3 on next access.
    """
    if not redis_client:
        return False
    
    key = _get_cache_key(hashed_email)
    dirty_key = _get_dirty_flag_key(hashed_email)
    
    try:
        redis_client.delete(key)
        redis_client.delete(dirty_key)
        return True
    except Exception as e:
        print(f"Redis delete error: {e}")
        return False


# Background sync function for periodic cleanup
def sync_all_dirty_caches_to_s3(s3_path: str) -> int:
    """
    Scan all dirty flags and sync to S3.
    Call this from a background job every few minutes.
    
    :return: Number of caches synced
    """
    if not redis_client:
        return 0
    
    synced_count = 0
    
    try:
        # Scan for all dirty flags
        cursor = 0
        pattern = "emails_json_dirty:*"
        
        while True:
            cursor, keys = redis_client.scan(cursor, match=pattern, count=100)
            
            for dirty_key in keys:
                # Extract hashed_email from key
                hashed_email = dirty_key.replace("emails_json_dirty:", "")
                
                # Flush to S3
                if flush_to_s3_if_dirty(hashed_email, s3_path):
                    synced_count += 1
            
            if cursor == 0:
                break
        
        if synced_count > 0:
            print(f"✓ Background sync completed: {synced_count} caches flushed to S3")
        
        return synced_count
        
    except Exception as e:
        print(f"Error in background sync: {e}")
        return synced_count