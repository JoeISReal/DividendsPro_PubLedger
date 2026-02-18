
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
                
            # Enrich with real-time price/supply
            return enrich_verdicts(results)
            
    except Exception as e:
        logger.error(f"Error fetching verdicts: {e}")
        return []
    finally:
        if conn:
            conn.close()

import requests

def fetch_token_price(mint: str) -> float:
    """
    Fetches current token price from Jupiter API v2.
    Returns 0.0 if failed.
    """
    try:
        url = f"https://api.jup.ag/price/v2?ids={mint}"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        price_str = data.get('data', {}).get(mint, {}).get('price')
        return float(price_str) if price_str else 0.0
    except Exception as e:
        logger.error(f"Error fetching price for {mint}: {e}")
        return 0.0

def fetch_token_supply(mint: str) -> float:
    """
    Fetches token supply from Solana RPC (Helius data source).
    Returns 0.0 if failed.
    """
    rpc_url = os.environ.get("SOLANA_RPC_URL", os.environ.get("HELIUS_API_KEY"))
    # If HELIUS_API_KEY is just the key, construct URL
    if rpc_url and "http" not in rpc_url:
         rpc_url = f"https://mainnet.helius-rpc.com/?api-key={rpc_url}"
         
    if not rpc_url:
        logger.warning("No RPC URL configured for supply check.")
        return 0.0

    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [mint]
        }
        resp = requests.post(rpc_url, json=payload, timeout=5)
        data = resp.json()
        ui_amount = data.get('result', {}).get('value', {}).get('uiAmount')
        return float(ui_amount) if ui_amount is not None else 0.0
    except Exception as e:
        logger.error(f"Error fetching supply for {mint}: {e}")
        return 0.0

def enrich_verdicts(results: List[Dict]) -> List[Dict]:
    """
    Enriches verdict schemas with real-time price and mcap.
    """
    enriched = []
    for row in results:
        token = row.get('token')
        if not token:
            enriched.append(row)
            continue
            
        # Fetch Data
        price = fetch_token_price(token)
        supply = fetch_token_supply(token)
        mcap = price * supply
        
        # Add to row
        row['price_usd'] = price
        row['market_cap'] = mcap
        row['supply_total'] = supply
        
        enriched.append(row)
    return enriched

# Patch fetch_new_verdicts to call enrich
# Note: In a cleaner impl, we'd rename functions, but to keep diff small:
# We will wrap the return of fetch_new_verdicts in the caller (signal_engine.py)
# OR we can modify fetch_new_verdicts here. Let's modify here.

# ... (We will modify fetch_new_verdicts in a separate block/tool call if needed, 
# but better to just ADD the helper functions first, then modify fetch_new_verdicts logic)

