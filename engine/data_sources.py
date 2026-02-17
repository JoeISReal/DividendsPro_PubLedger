
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Connects to the PostgreSQL database using DIVIDENDSPRO_DATABASE_URL environment variable.
    """
    db_url = os.environ.get("DIVIDENDSPRO_DATABASE_URL")
    if not db_url:
        logger.warning("DIVIDENDSPRO_DATABASE_URL not set. Returning None for connection.")
        return None
        
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

def fetch_new_verdicts(minutes_back: int = 60) -> List[Dict]:
    """
    Fetches confirmed verdicts from the last N minutes.
    
    Query Logic:
    - Select from token_state_60s
    - Where verdict is NOT NULL
    - And verdict type is in allowable list
    - And time window is recent
    """
    conn = get_db_connection()
    if not conn:
        return []

    # Safe allow-list of verdicts
    # Matching the node.js implementation: ['BREAKOUT_CONFIRMED', 'ACCUMULATION', 'BREAKOUT_EARLY', 'UNWIND']
    # Note: DB stores them as strings.
    
    query = """
        SELECT 
            token,
            verdict,
            credibility as score,
            buy_sol as volume_sol,
            window_ends_at as timestamp_utc
        FROM token_state_60s
        WHERE verdict IN ('BREAKOUT_CONFIRMED', 'ACCUMULATION', 'BREAKOUT_EARLY', 'UNWIND')
          AND window_ends_at >= NOW() - INTERVAL '%s minutes'
        ORDER BY window_ends_at ASC
    """
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (minutes_back,))
            rows = cur.fetchall()
            
            # Transform to standard dictionary format if needed
            # For now, just return the raw rows as dicts
            results = []
            for row in rows:
                # Convert datetime to string ISO format
                if row.get('timestamp_utc'):
                     row['timestamp_utc'] = row['timestamp_utc'].isoformat()
                     
                results.append(row)
                
            return results
            
    except Exception as e:
        logger.error(f"Error fetching verdicts: {e}")
        return []
    finally:
        if conn:
            conn.close()
