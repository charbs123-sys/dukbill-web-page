import redis
import json
import os
from threading import Thread
import logging

logging.basicConfig(level=logging.INFO)

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
CACHE_TTL = int(os.environ.get('EMAILS_JSON_CACHE_TTL', 1800))
S3_PATH = "/broker_anonymized/emails_anonymized.json"

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
    
    # Enable keyspace notifications for expired events
    redis_client.config_set('notify-keyspace-events', 'Ex')
    
    logging.info(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logging.error(f"⚠ Redis connection failed: {e}. Caching disabled.")
    redis_client = None


def _get_cache_key(hashed_email: str) -> str:
    return f"emails_json:{hashed_email}"


def _get_dirty_flag_key(hashed_email: str) -> str:
    return f"emails_json_dirty:{hashed_email}"


def get_cached_emails_json(hashed_email: str) -> list | None:
    if not redis_client:
        return None
    
    key = _get_cache_key(hashed_email)
    try:
        cached = redis_client.get(key)
        if cached:
            redis_client.expire(key, CACHE_TTL)
            dirty_key = _get_dirty_flag_key(hashed_email)
            redis_client.expire(dirty_key, CACHE_TTL)
            return json.loads(cached)
    except Exception as e:
        logging.error(f"Redis get error: {e}")
    
    return None


def set_cached_emails_json(hashed_email: str, documents: list, mark_dirty: bool = False) -> bool:
    if not redis_client:
        return False
    
    key = _get_cache_key(hashed_email)
    try:
        redis_client.setex(key, CACHE_TTL, json.dumps(documents))
        
        if mark_dirty:
            dirty_key = _get_dirty_flag_key(hashed_email)
            redis_client.setex(dirty_key, CACHE_TTL, json.dumps(documents))
        
        return True
    except Exception as e:
        logging.error(f"Redis set error: {e}")
        return False


def get_or_load_emails_json(hashed_email: str, s3_path: str) -> list:
    documents = get_cached_emails_json(hashed_email)
    if documents is not None:
        logging.info(f"✓ Cache HIT for {hashed_email}")
        return documents
    
    logging.info(f"✗ Cache MISS for {hashed_email}, loading from S3...")
    from Backend.Database.S3_utils import get_json_file
    documents = get_json_file(hashed_email, s3_path)
    set_cached_emails_json(hashed_email, documents, mark_dirty=False)
    
    return documents


def save_emails_json_to_cache(hashed_email: str, documents: list) -> None:
    set_cached_emails_json(hashed_email, documents, mark_dirty=True)
    logging.info(f"✓ Updated cache for {hashed_email} (S3 sync on expiry)")


def _handle_key_expiration(message):
    """
    Called when a Redis key expires.
    Syncs dirty cache to S3 before it's gone.
    """
    try:
        expired_key = message['data']
        
        # Check if it's a dirty flag expiring
        if expired_key.startswith('emails_json_dirty:'):
            hashed_email = expired_key.replace('emails_json_dirty:', '')
            
            # The dirty flag still exists for a moment, grab it
            dirty_data = redis_client.get(expired_key)
            
            if dirty_data:
                documents = json.loads(dirty_data)
                
                # Save to S3
                from Backend.Database.S3_utils import save_json_file
                save_json_file(hashed_email, S3_PATH, documents)
                
                logging.info(f"✓ Synced expired cache {hashed_email} to S3 on expiry")
                
    except Exception as e:
        logging.error(f"Error handling key expiration: {e}")


def start_expiry_listener():
    """
    Start listening for Redis key expiration events.
    Syncs dirty caches to S3 when they expire.
    """
    if not redis_client:
        logging.warning("Redis not available, expiry listener not started")
        return None
    
    def listen():
        try:
            pubsub = redis_client.pubsub()
            
            # Subscribe to expired key events on database 0
            pubsub.psubscribe(**{'__keyevent@0__:expired': _handle_key_expiration})
            
            logging.info("✓ Redis expiry listener started - will sync to S3 on expiry")
            
            # Run listener in thread
            for message in pubsub.listen():
                if message['type'] == 'pmessage':
                    _handle_key_expiration(message)
                    
        except Exception as e:
            logging.error(f"Expiry listener error: {e}")
    
    # Start listener in background thread
    thread = Thread(target=listen, daemon=True)
    thread.start()
    
    return thread


def invalidate_emails_json(hashed_email: str) -> bool:
    if not redis_client:
        return False
    
    key = _get_cache_key(hashed_email)
    dirty_key = _get_dirty_flag_key(hashed_email)
    
    try:
        redis_client.delete(key)
        redis_client.delete(dirty_key)
        return True
    except Exception as e:
        logging.error(f"Redis delete error: {e}")
        return False


def force_sync_to_s3(hashed_email: str, s3_path: str = S3_PATH) -> bool:
    """Force immediate sync to S3"""
    if not redis_client:
        return False
    
    try:
        documents = get_cached_emails_json(hashed_email)
        if documents is None:
            logging.info(f"No cache data for {hashed_email}")
            return False
        
        from Backend.Database.S3_utils import save_json_file
        save_json_file(hashed_email, s3_path, documents)
        
        dirty_key = _get_dirty_flag_key(hashed_email)
        redis_client.delete(dirty_key)
        
        logging.info(f"✓ Force synced {hashed_email} to S3")
        return True
        
    except Exception as e:
        logging.error(f"Error force syncing to S3: {e}")
        return False