
import os
import json
import logging
import datetime
from typing import Dict, List
import sys

# Add parent directory to path to allow imports if running as script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.hashing import compute_entry_hash
from engine.signing import Signer
from engine.data_sources import fetch_new_verdicts

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SIGNALS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'signals')

def ensure_signals_dir():
    if not os.path.exists(SIGNALS_DIR):
        os.makedirs(SIGNALS_DIR)

def get_today_file_path() -> str:
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    return os.path.join(SIGNALS_DIR, f"{today}.jsonl")

def load_last_entry(file_path: str) -> Dict:
    """
    Reads the last line of the JSONL file to get the previous hash.
    If file doesn't exist or is empty, returns None.
    """
    if not os.path.exists(file_path):
        return None
        
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if not lines:
                return None
            last_line = lines[-1].strip()
            if not last_line:
                return None
            return json.loads(last_line)
    except Exception as e:
        logger.error(f"Error reading last entry from {file_path}: {e}")
        return None

def process_signals():
    ensure_signals_dir()
    
    # 1. Setup Signer
    try:
        signer = Signer()
        logger.info(f"Signer initialized with public key: {signer.get_public_key()}")
    except Exception as e:
        logger.error(f"Failed to initialize signer: {e}")
        return

    # 2. Fetch New Verdicts
    logger.info("Fetching new verdicts from database...")
    candidates = fetch_new_verdicts(minutes_back=60)
    
    if not candidates:
        logger.info("No new verdicts found.")
        return

    logger.info(f"Found {len(candidates)} candidate verdicts.")

    # 3. Process Each Candidate
    file_path = get_today_file_path()
    
    # Load last entry to link hash chain
    last_entry = load_last_entry(file_path)
    previous_hash = last_entry['entry_hash'] if last_entry else "GENESIS"
    
    # Avoid duplicates: Load all existing entries for today to check
    existing_entries = []
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    existing_entries.append(json.loads(line))
    
    # Create a set of unique identifiers for existing entries to prevent dups
    # Unique ID = asset + timestamp
    existing_ids = set()
    for e in existing_entries:
        uid = f"{e.get('asset')}_{e.get('timestamp_utc')}"
        existing_ids.add(uid)

    new_entries_count = 0
    
    with open(file_path, 'a') as f:
        for candidate in candidates:
            # unique identifier check
            asset = candidate['token']
            timestamp = candidate['timestamp_utc']
            uid = f"{asset}_{timestamp}"
            
            if uid in existing_ids:
                logger.debug(f"Skipping duplicate: {uid}")
                continue
                
            # Construct Entry
            entry = {
                "asset": asset,
                "score": float(candidate['score']) if candidate['score'] else 0.0,
                "state": candidate['verdict'], # e.g. BREAKOUT_CONFIRMED
                "ruleset_version": "1.1.0",
                "timestamp_utc": timestamp,
                "previous_hash": previous_hash,
                # entry_hash calculated below
                # signature calculated below
            }
            
            # Compute Hash
            entry_hash = compute_entry_hash(entry, previous_hash)
            entry['entry_hash'] = entry_hash
            
            # Sign Hash
            signature = signer.sign_entry(entry_hash)
            entry['signature'] = signature
            
            # Append to file
            f.write(json.dumps(entry) + '\n')
            
            # Update previous_hash for next entry in the chain
            previous_hash = entry_hash
            new_entries_count += 1
            existing_ids.add(uid) # Add to set to prevent dups within same batch
            
    logger.info(f" appended {new_entries_count} new entries to {file_path}")

if __name__ == "__main__":
    process_signals()
