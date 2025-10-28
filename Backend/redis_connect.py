from redis import Redis
import logging

logging.basicConfig(level=logging.INFO)

redis_client = Redis(
    host='dukbill-simple-cache.j3oeqj.ng.0001.apse2.cache.amazonaws.com',
    port=6379,
    decode_responses=True,
    socket_connect_timeout=5
)

try:
    if redis_client.ping():
        logging.info("✓ Connected to Redis!")
        
        # Test set/get
        redis_client.set('test', 'hello')
        value = redis_client.get('test')
        logging.info(f"✓ Set/Get works: {value}")
        
except Exception as e:
    logging.error(f"✗ Connection failed: {e}")