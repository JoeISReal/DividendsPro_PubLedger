
import sys
import os
import argparse
import base64
import json
import nacl.signing
import nacl.encoding
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.hashing import compute_entry_hash

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def verify_entry_signature(entry: dict, public_key_hex: str) -> bool:
    """
    Verifies the signature of a ledger entry.
    """
    try:
        signature_b64 = entry.get('signature')
        entry_hash = entry.get('entry_hash')
        
        if not signature_b64 or not entry_hash:
            logger.error("Entry missing signature or entry_hash")
            return False
            
        # Recompute hash to ensure integrity
        # Note: We need the previous hash to recompute. 
        # For standalone verification, we might just trust the hash matches the content 
        # and only verify the signature against the hash claim.
        # BUT, strict verification requires re-hashing. 
        # Here we assume the entry json includes 'previous_hash'
        
        previous_hash = entry.get('previous_hash')
        if not previous_hash:
             logger.error("Entry missing previous_hash")
             return False
             
        computed_hash = compute_entry_hash(entry, previous_hash)
        
        if computed_hash != entry_hash:
            logger.error(f"Hash Mismatch! Claimed: {entry_hash}, Computed: {computed_hash}")
            return False
            
        # Verify Signature
        verify_key = nacl.signing.VerifyKey(public_key_hex, encoder=nacl.encoding.HexEncoder)
        try:
            verify_key.verify(entry_hash.encode('utf-8'), base64.b64decode(signature_b64))
            return True
        except nacl.exceptions.BadSignatureError:
            logger.error("Invalid Signature")
            return False
            
    except Exception as e:
        logger.error(f"Verification Error: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify a single ledger entry signature.")
    parser.add_argument("--entry", help="JSON string of the entry", required=True)
    parser.add_argument("--public-key", help="Hex public key", required=True)
    
    args = parser.parse_args()
    
    try:
        entry = json.loads(args.entry)
        valid = verify_entry_signature(entry, args.public_key)
        if valid:
            print("✅ Signature Valid")
            sys.exit(0)
        else:
            print("❌ Signature Invalid")
            sys.exit(1)
    except json.JSONDecodeError:
        print("Error: Invalid JSON string")
        sys.exit(1)
