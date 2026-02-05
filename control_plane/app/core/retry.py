"""
Centralized database retry logic, metrics, and timeout configuration.
All service modules should import DB_RETRY and DB_TIMEOUT from this module.
"""
import logging
from tenacity import retry, stop_after_attempt, wait_fixed
from prometheus_client import Counter

# Setup logging
logger = logging.getLogger(__name__)

# Setup Prometheus Metrics
DB_RETRY_COUNTER = Counter("db_retries_total", "Total number of database retries")

def after_retry(retry_state):
    """Callback function executed after each retry attempt."""
    DB_RETRY_COUNTER.inc()
    logger.warning(f"Database query failed. Retrying... (Attempt {retry_state.attempt_number})")

# Retry configuration: 2 attempts total, 0.1s wait between attempts
DB_RETRY = retry(
    stop=stop_after_attempt(2), 
    wait=wait_fixed(0.1), 
    reraise=True,
    after=after_retry
)

# Database query timeout (in seconds)
DB_TIMEOUT = 5.0
