import hashlib
import json
import logging

logger = logging.getLogger(__name__)

def compute_entry_hash(entry_dict: dict, previous_hash: str) -> str:
    """
    Computes the SHA-256 hash of a ledger entry chained to a previous hash.
    
    Hash = SHA256( Serialize(Clean(entry)) + previous_hash )
    
    Args:
        entry_dict: The dictionary representation of the entry.
        previous_hash: The hex string of the previous entry's hash (or "GENESIS").
        
    Returns:
        Hex string of the SHA-256 hash.
    """
    if not isinstance(previous_hash, str):
        raise ValueError("Previous hash must be a string")
        
    # 1. Clean entry: Remove fields that shouldn't be part of the hash (signature, entry_hash)
    clean_entry = entry_dict.copy()
    keys_to_remove = ['entry_hash', 'signature']
    for k in keys_to_remove:
        clean_entry.pop(k, None)
        
    # 2. Serialize: Canonical JSON (sorted keys, no separators)
    # Using separators=(',', ':') removes whitespace for strict canonicalization
    serialized_json = json.dumps(clean_entry, sort_keys=True, separators=(',', ':'))
    
    # 3. Concatenate
    payload = serialized_json + previous_hash
    
    # 4. Hash
    entry_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    
    return entry_hash
