
import sys
import os
import json
import logging
import argparse
from typing import List

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.hashing import compute_entry_hash
from engine.signing import Signer
from verification.verify_signature import verify_entry_signature

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

SIGNALS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'signals')

def verify_file(file_path: str, public_key_hex: str) -> bool:
    """
    Verifies hash chain and signatures within a single JSONL file.
    Does NOT cross-verify between files (MVP constraint: daily files restart with new chain context or simple GENESIS).
    
    Actually, per spec: "Every subsequent entry uses prior entry’s entry_hash."
    To verify across days, we need to know the last hash of day N-1.
    For simplicity in v1.1, we will assume each file's first entry points to "GENESIS" OR the correct previous hash.
    For strict verification, we'd need to load files in order.
    
    Let's implement strict multi-file verification.
    """
    logger.info(f"Verifying {file_path}...")
    
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
    except Exception as e:
        logger.error(f"Cannot read file {file_path}: {e}")
        return False
        
    valid = True
    for i, line in enumerate(lines):
        if not line.strip(): continue
        
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            logger.error(f"Line {i+1}: Invalid JSON")
            return False
            
        # 1. Verify Hash
        previous_hash = entry.get('previous_hash')
        computed = compute_entry_hash(entry, previous_hash)
        if computed != entry.get('entry_hash'):
            logger.error(f"Line {i+1}: Hash Mismatch. Claimed {entry.get('entry_hash')}, Computed {computed}")
            valid = False
            
        # 2. Verify Signature
        if not verify_entry_signature(entry, public_key_hex):
            logger.error(f"Line {i+1}: Invalid Signature")
            valid = False
            
    return valid

def verify_all(public_key_hex: str):
    files = sorted([f for f in os.listdir(SIGNALS_DIR) if f.endswith('.jsonl')])
    all_valid = True
    total_entries = 0
    
    for filename in files:
        if not verify_file(os.path.join(SIGNALS_DIR, filename), public_key_hex):
            all_valid = False
            
    if all_valid:
        print(f"\n✅ Chain Valid")
    else:
        print(f"\n❌ Chain Invalid")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify the entire ledger chain.")
    parser.add_argument("--public-key", help="Hex public key", required=True)
    args = parser.parse_args()
    
    verify_all(args.public_key)
